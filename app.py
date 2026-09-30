import os
import json
import time
import math
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, Response


# ============================================================
# TRADING-AI
# Stable WebSocket-first market engine
#
# Architecture:
# Twelve Data WebSocket
#        ↓
# Live tick
#        ↓
# Local 1m candle
#        ↓
# Local 5m candle
#        ↓
# Local 15m / 30m / 1H
#        ↓
# Multi-timeframe analysis
#        ↓
# False-signal filters
#        ↓
# BUY / SELL / WAIT
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"
HISTORY_URL = "https://api.twelvedata.com/time_series"

# Current Twelve Data plan does not provide reliable WTI access.
# Keep this disabled rather than generating fake crude data.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"


# ============================================================
# CANDLE STORAGE
# ============================================================

MAX_1M = 1800
MAX_5M = 800
MAX_15M = 500
MAX_30M = 300
MAX_1H = 250


candles_1m = deque(maxlen=MAX_1M)
candles_5m = deque(maxlen=MAX_5M)
candles_15m = deque(maxlen=MAX_15M)
candles_30m = deque(maxlen=MAX_30M)
candles_1h = deque(maxlen=MAX_1H)


# ============================================================
# THREAD / STATE CONTROL
# ============================================================

state_lock = threading.RLock()
stop_event = threading.Event()

threads_started = False


state = {
    "price": None,
    "last_price_ts": None,

    "ws_connected": False,
    "ws_last_message": None,
    "ws_reconnects": 0,

    "data_status": "STARTING",

    "bootstrap_status": "WAITING",
    "bootstrap_last_try": None,
    "bootstrap_error": None,

    "analysis": None,
    "history": deque(maxlen=20),

    "last_analysis_ts": None,
    "engine": "WAITING",
}


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ts():
    return int(time.time())


def iso_from_ts(ts):
    if not ts:
        return None

    try:
        return datetime.fromtimestamp(
            float(ts),
            tz=timezone.utc
        ).isoformat()
    except Exception:
        return None


def floor_bucket(ts, minutes):
    return int(ts // (minutes * 60)) * (minutes * 60)


def safe_float(value):
    try:
        x = float(value)

        if math.isfinite(x):
            return x

        return None

    except (TypeError, ValueError):
        return None


# ============================================================
# CANDLE INSERT / UPDATE
# ============================================================

def upsert_candle(store, candle):
    """
    Insert or update candle by timestamp.
    Keeps candles chronological.
    """

    if not candle:
        return

    if candle.get("ts") is None:
        return

    ts = int(candle["ts"])

    if not store:
        store.append(candle)
        return

    # Newest candle
    if ts > store[-1]["ts"]:
        store.append(candle)
        return

    # Update current candle
    if ts == store[-1]["ts"]:
        store[-1] = candle
        return

    # Older candle / replacement
    items = list(store)

    replaced = False

    for i, old in enumerate(items):

        if old["ts"] == ts:
            items[i] = candle
            replaced = True
            break

        if old["ts"] > ts:
            items.insert(i, candle)
            replaced = True
            break

    if not replaced:
        items.append(candle)

    store.clear()

    store.extend(items[-store.maxlen:])


def make_candle(ts, price, previous=None):
    p = safe_float(price)

    if p is None:
        return None

    if previous is None:

        return {
            "ts": int(ts),
            "open": p,
            "high": p,
            "low": p,
            "close": p,
        }

    return {
        "ts": int(ts),
        "open": previous["open"],
        "high": max(previous["high"], p),
        "low": min(previous["low"], p),
        "close": p,
    }


# ============================================================
# LIVE 1-MINUTE CANDLE
# ============================================================

def update_1m(price, ts):

    bucket = floor_bucket(ts, 1)

    p = safe_float(price)

    if p is None:
        return

    with state_lock:

        if candles_1m and candles_1m[-1]["ts"] == bucket:

            candle = candles_1m[-1]

            candle["high"] = max(
                candle["high"],
                p
            )

            candle["low"] = min(
                candle["low"],
                p
            )

            candle["close"] = p

        else:

            previous = candles_1m[-1] if candles_1m else None

            candle = make_candle(
                bucket,
                p,
                previous
            )

            upsert_candle(
                candles_1m,
                candle
            )

        state["price"] = p
        state["last_price_ts"] = int(ts)
        state["data_status"] = "LIVE"


# ============================================================
# TIMEFRAME AGGREGATION
# ============================================================

def aggregate(candles, minutes):

    if not candles:
        return []

    buckets = {}

    for candle in candles:

        ts = int(candle["ts"])

        bucket = floor_bucket(
            ts,
            minutes
        )

        item = buckets.get(bucket)

        if item is None:

            buckets[bucket] = {
                "ts": bucket,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
            }

        else:

            item["high"] = max(
                item["high"],
                candle["high"]
            )

            item["low"] = min(
                item["low"],
                candle["low"]
            )

            item["close"] = candle["close"]

    return [
        buckets[k]
        for k in sorted(buckets)
    ]


def rebuild_timeframes():

    with state_lock:

        # Build live 5m candles from live 1m ticks.
        if candles_1m:

            live_5m = aggregate(
                list(candles_1m),
                5
            )

            for candle in live_5m:

                upsert_candle(
                    candles_5m,
                    candle
                )

        # Everything above 5m is built locally.
        base_5m = list(candles_5m)

        if not base_5m:
            return

        local_15m = aggregate(
            base_5m,
            15
        )

        local_30m = aggregate(
            base_5m,
            30
        )

        local_1h = aggregate(
            base_5m,
            60
        )

        candles_15m.clear()
        candles_15m.extend(
            local_15m[-MAX_15M:]
        )

        candles_30m.clear()
        candles_30m.extend(
            local_30m[-MAX_30M:]
        )

        candles_1h.clear()
        candles_1h.extend(
            local_1h[-MAX_1H:]
        )


# ============================================================
# TWELVE DATA HISTORY PARSER
# ============================================================

def parse_history(payload):

    if not isinstance(payload, dict):
        return []

    values = payload.get("values") or []

    result = []

    for row in values:

        try:

            datetime_value = row.get("datetime")

            if isinstance(datetime_value, str):

                dt = datetime.strptime(
                    datetime_value,
                    "%Y-%m-%d %H:%M:%S"
                ).replace(
                    tzinfo=timezone.utc
                )

                ts = int(
                    dt.timestamp()
                )

            else:

                ts = int(
                    float(datetime_value)
                )

            o = safe_float(row.get("open"))
            h = safe_float(row.get("high"))
            l = safe_float(row.get("low"))
            c = safe_float(row.get("close"))

            if None in (o, h, l, c):
                continue

            result.append(
                {
                    "ts": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                }
            )

        except (
            TypeError,
            ValueError,
            OverflowError
        ):
            continue

    result.sort(
        key=lambda x: x["ts"]
    )

    return result


# ============================================================
# HISTORICAL BOOTSTRAP
#
# IMPORTANT:
# Only ONE HTTP history request.
# 15m / 30m / 1H are generated locally.
# ============================================================

def bootstrap_history():

    if not API_KEY:

        with state_lock:

            state["bootstrap_status"] = "NO API KEY"

            state["bootstrap_error"] = (
                "TWELVE_DATA_API_KEY is missing"
            )

        return

    with state_lock:

        state["bootstrap_status"] = "LOADING 5m"

        state["bootstrap_last_try"] = now_ts()

    try:

        params = {
            "symbol": GOLD_SYMBOL,
            "interval": "5min",

            # 700 x 5m gives enough data
            # to build more than 55 one-hour candles.
            "outputsize": 700,

            "timezone": "UTC",

            "apikey": API_KEY,
        }

        response = requests.get(
            HISTORY_URL,
            params=params,
            timeout=15
        )

        if response.status_code != 200:

            raise RuntimeError(
                f"HTTP {response.status_code}"
            )

        payload = response.json()

        if payload.get("status") == "error":

            raise RuntimeError(
                str(
                    payload.get("message")
                    or "Twelve Data error"
                )
            )

        history = parse_history(
            payload
        )

        if not history:

            raise RuntimeError(
                "No 5m candles returned"
            )

        with state_lock:

            candles_5m.clear()

            candles_5m.extend(
                history[-MAX_5M:]
            )

            state["bootstrap_status"] = (
                f"READY ({len(history)} x 5m)"
            )

            state["bootstrap_error"] = None

            rebuild_timeframes()

        print(
            f"BOOTSTRAP: loaded {len(history)} 5m candles",
            flush=True
        )

    except Exception as exc:

        with state_lock:

            state["bootstrap_status"] = "LIVE ONLY"

            state["bootstrap_error"] = str(exc)

        print(
            f"BOOTSTRAP WARNING: {exc}",
            flush=True
        )


# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):

    if len(values) < period:
        return None

    seed = (
        sum(values[:period])
        / period
    )

    multiplier = 2.0 / (
        period + 1.0
    )

    value = seed

    for x in values[period:]:

        value = (
            (x - value)
            * multiplier
            + value
        )

    return value


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        difference = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(difference, 0.0)
        )

        losses.append(
            max(-difference, 0.0)
        )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    relative_strength = (
        avg_gain
        / avg_loss
    )

    return (
        100.0
        - (
            100.0
            / (1.0 + relative_strength)
        )
    )


def atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    true_ranges = []

    for i in range(
        1,
        len(candles)
    ):

        current = candles[i]
        previous = candles[i - 1]

        true_range = max(
            current["high"]
            - current["low"],

            abs(
                current["high"]
                - previous["close"]
            ),

            abs(
                current["low"]
                - previous["close"]
            ),
        )

        true_ranges.append(
            true_range
        )

    if len(true_ranges) < period:
        return None

    value = (
        sum(true_ranges[:period])
        / period
    )

    for tr in true_ranges[period:]:

        value = (
            (
                value
                * (period - 1)
            )
            + tr
        ) / period

    return value


# ============================================================
# MARKET STRUCTURE
# ============================================================

def structure(
    candles,
    lookback=8
):

    if len(candles) < lookback + 2:
        return "WAITING"

    recent = candles[-lookback:]

    middle = max(
        2,
        lookback // 2
    )

    first = recent[:middle]
    second = recent[middle:]

    first_high = max(
        x["high"]
        for x in first
    )

    second_high = max(
        x["high"]
        for x in second
    )

    first_low = min(
        x["low"]
        for x in first
    )

    second_low = min(
        x["low"]
        for x in second
    )

    last = recent[-1]

    if (
        second_high > first_high
        and second_low > first_low
    ):
        return "HIGHER HIGH / HIGHER LOW"

    if (
        second_high < first_high
        and second_low < first_low
    ):
        return "LOWER HIGH / LOWER LOW"

    if (
        last["high"] > first_high
        and last["low"] > first_low
    ):
        return "BULLISH BREAK"

    if (
        last["high"] < first_high
        and last["low"] < first_low
    ):
        return "BEARISH BREAK"

    return "RANGE / MIXED"


# ============================================================
# LIQUIDITY
# ============================================================

def liquidity_levels(
    candles,
    lookback=30
):

    if len(candles) < 5:
        return None, None

    recent = candles[-lookback:]

    high = max(
        x["high"]
        for x in recent
    )

    low = min(
        x["low"]
        for x in recent
    )

    return high, low


def detect_sweep(
    candles,
    lookback=20
):

    if len(candles) < lookback + 2:
        return "NONE"

    previous = candles[
        -lookback - 1:-1
    ]

    last = candles[-1]

    previous_high = max(
        x["high"]
        for x in previous
    )

    previous_low = min(
        x["low"]
        for x in previous
    )

    # Price took previous high liquidity
    # and closed back below it.
    if (
        last["high"] > previous_high
        and last["close"] < previous_high
    ):
        return "HIGH SWEEP"

    # Price took previous low liquidity
    # and closed back above it.
    if (
        last["low"] < previous_low
        and last["close"] > previous_low
    ):
        return "LOW SWEEP"

    return "NONE"


# ============================================================
# SUPPORT / RESISTANCE
# ============================================================

def support_resistance(
    candles,
    lookback=40
):

    if len(candles) < 10:
        return None, None

    recent = candles[-lookback:]

    support = min(
        x["low"]
        for x in recent
    )

    resistance = max(
        x["high"]
        for x in recent
    )

    return support, resistance


# ============================================================
# SINGLE TIMEFRAME ANALYSIS
# ============================================================

def analyze_timeframe(candles):

    data = list(candles)

    if len(data) < 55:

        return {
            "ready": False,
            "count": len(data),

            "trend": "WAITING",
            "momentum": "WAITING",
            "structure": "WAITING",

            "rsi": None,
            "ema20": None,
            "ema50": None,
            "atr": None,

            "support": None,
            "resistance": None,

            "liquidity_high": None,
            "liquidity_low": None,

            "sweep": "NONE",
        }

    closes = [
        x["close"]
        for x in data
    ]

    ema9 = ema(
        closes,
        9
    )

    ema20 = ema(
        closes,
        20
    )

    ema50 = ema(
        closes,
        50
    )

    rsi_value = rsi(
        closes,
        14
    )

    atr_value = atr(
        data,
        14
    )

    last_price = closes[-1]

    # Trend
    if (
        ema20 is not None
        and ema50 is not None
        and last_price > ema20 > ema50
    ):

        trend = "BULLISH"

    elif (
        ema20 is not None
        and ema50 is not None
        and last_price < ema20 < ema50
    ):

        trend = "BEARISH"

    else:

        trend = "MIXED"

    # Momentum
    if (
        ema9 is not None
        and ema20 is not None
        and rsi_value is not None
    ):

        if (
            ema9 > ema20
            and rsi_value >= 55
        ):

            momentum = "BUYING"

        elif (
            ema9 < ema20
            and rsi_value <= 45
        ):

            momentum = "SELLING"

        else:

            momentum = "NEUTRAL"

    else:

        momentum = "WAITING"

    support, resistance = (
        support_resistance(data)
    )

    liquidity_high, liquidity_low = (
        liquidity_levels(data)
    )

    sweep = detect_sweep(
        data
    )

    return {
        "ready": True,
        "count": len(data),

        "trend": trend,
        "momentum": momentum,
        "structure": structure(data),

        "rsi": rsi_value,
        "ema20": ema20,
        "ema50": ema50,
        "atr": atr_value,

        "support": support,
        "resistance": resistance,

        "liquidity_high": liquidity_high,
        "liquidity_low": liquidity_low,

        "sweep": sweep,
    }


# ============================================================
# MULTI-TIMEFRAME SCORE
# ============================================================

def direction_score(
    tf5,
    tf15,
    tf1h
):

    score = 0

    evidence = []

    # --------------------------------------------------------
    # 5 MINUTE EXECUTION TIMEFRAME
    # --------------------------------------------------------

    if tf5["trend"] == "BULLISH":

        score += 2

        evidence.append(
            "5m trend bullish"
        )

    elif tf5["trend"] == "BEARISH":

        score -= 2

        evidence.append(
            "5m trend bearish"
        )


    if tf5["momentum"] == "BUYING":

        score += 2

        evidence.append(
            "5m momentum buying"
        )

    elif tf5["momentum"] == "SELLING":

        score -= 2

        evidence.append(
            "5m momentum selling"
        )


    # Structure
    if tf5["structure"] in (
        "HIGHER HIGH / HIGHER LOW",
        "BULLISH BREAK",
    ):

        score += 1

        evidence.append(
            "5m structure bullish"
        )

    elif tf5["structure"] in (
        "LOWER HIGH / LOWER LOW",
        "BEARISH BREAK",
    ):

        score -= 1

        evidence.append(
            "5m structure bearish"
        )


    # --------------------------------------------------------
    # 15 MINUTE CONFIRMATION
    # --------------------------------------------------------

    if tf15["trend"] == "BULLISH":

        score += 2

        evidence.append(
            "15m trend confirms bullish"
        )

    elif tf15["trend"] == "BEARISH":

        score -= 2

        evidence.append(
            "15m trend confirms bearish"
        )


    # --------------------------------------------------------
    # 1 HOUR CONTEXT
    # --------------------------------------------------------

    if tf1h["trend"] == "BULLISH":

        score += 1

        evidence.append(
            "1h context bullish"
        )

    elif tf1h["trend"] == "BEARISH":

        score -= 1

        evidence.append(
            "1h context bearish"
        )


    # --------------------------------------------------------
    # RSI
    #
    # RSI is confirmation only.
    # It is NOT allowed to generate signal alone.
    # --------------------------------------------------------

    if tf5["rsi"] is not None:

        if (
            55 <= tf5["rsi"] <= 68
        ):

            score += 1

            evidence.append(
                "5m RSI supports buyers"
            )

        elif (
            32 <= tf5["rsi"] <= 45
        ):

            score -= 1

            evidence.append(
                "5m RSI supports sellers"
            )


    # --------------------------------------------------------
    # LIQUIDITY SWEEP
    # --------------------------------------------------------

    if tf5["sweep"] == "LOW SWEEP":

        score += 2

        evidence.append(
            "5m low-liquidity sweep"
        )

    elif tf5["sweep"] == "HIGH SWEEP":

        score -= 2

        evidence.append(
            "5m high-liquidity sweep"
        )


    return score, evidence


# ============================================================
# TRADE SETUP
# ============================================================

def build_trade_setup(
    side,
    tf5
):

    price = state.get(
        "price"
    )

    atr_value = tf5.get(
        "atr"
    )

    if (
        price is None
        or atr_value is None
        or atr_value <= 0
    ):

        return {
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,

            "invalidation":
                "Insufficient volatility data",

            "setup": "WAIT",
        }


    entry = float(price)

    risk = max(
        atr_value * 1.2,
        entry * 0.0015
    )


    if side == "BUY":

        stop_loss = (
            entry - risk
        )

        target1 = (
            entry + risk
        )

        target2 = (
            entry + risk * 2
        )

        target3 = (
            entry + risk * 3
        )

        invalidation = (
            f"5m close below "
            f"{stop_loss:.3f}"
        )


    elif side == "SELL":

        stop_loss = (
            entry + risk
        )

        target1 = (
            entry - risk
        )

        target2 = (
            entry - risk * 2
        )

        target3 = (
            entry - risk * 3
        )

        invalidation = (
            f"5m close above "
            f"{stop_loss:.3f}"
        )


    else:

        return {
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,

            "invalidation":
                "No confirmed direction",

            "setup": "WAIT",
        }


    return {
        "entry": entry,
        "stop_loss": stop_loss,
        "target1": target1,
        "target2": target2,
        "target3": target3,

        "rr": "1:1 / 1:2 / 1:3",

        "invalidation": invalidation,

        "setup": side,
    }


# ============================================================
# FINAL AI DECISION
# ============================================================

def generate_analysis():

    with state_lock:

        tf5 = analyze_timeframe(
            candles_5m
        )

        tf15 = analyze_timeframe(
            candles_15m
        )

        tf1h = analyze_timeframe(
            candles_1h
        )

        price = state["price"]


        # ----------------------------------------------------
        # DATA QUALITY FILTER
        # ----------------------------------------------------

        if not (
            tf5["ready"]
            and tf15["ready"]
            and tf1h["ready"]
        ):

            result = {

                "decision": "WAIT",

                "confidence": 50,

                "score": 0,

                "engine": "DATA FILTER",

                "reason":
                    "Waiting for enough "
                    "multi-timeframe candle data.",

                "evidence": [],

                "setup":
                    build_trade_setup(
                        "WAIT",
                        tf5
                    ),

                "timeframe": "5m",

                "tfs": {
                    "5m": tf5,
                    "15m": tf15,
                    "1h": tf1h,
                },

                "price": price,

                "updated": now_ts(),
            }


        else:

            score, evidence = (
                direction_score(
                    tf5,
                    tf15,
                    tf1h
                )
            )


            # ------------------------------------------------
            # CONFLICT FILTER
            #
            # If 5m and 15m disagree,
            # don't force a trade.
            # ------------------------------------------------

            conflict = (

                tf5["trend"]
                in ("BULLISH", "BEARISH")

                and

                tf15["trend"]
                in ("BULLISH", "BEARISH")

                and

                tf5["trend"]
                !=
                tf15["trend"]
            )


            # ------------------------------------------------
            # EXTREME RSI FILTER
            #
            # Don't chase fresh entries at extreme RSI.
            # ------------------------------------------------

            extreme_rsi = (

                tf5["rsi"] is not None

                and (

                    tf5["rsi"] >= 75

                    or

                    tf5["rsi"] <= 25
                )
            )


            # ------------------------------------------------
            # DECISION
            # ------------------------------------------------

            if conflict:

                decision = "WAIT"

                engine = (
                    "CONFLICT FILTER"
                )

                reason = (
                    "5m and 15m direction "
                    "are not aligned."
                )


            elif extreme_rsi:

                decision = "WAIT"

                engine = (
                    "EXTREME FILTER"
                )

                reason = (
                    "Momentum is too extended "
                    "for a fresh entry."
                )


            elif score >= 7:

                decision = "BUY"

                engine = (
                    "MULTI-TF CONFIRMED"
                )

                reason = (
                    "Multiple independent "
                    "factors align on the buy side."
                )


            elif score <= -7:

                decision = "SELL"

                engine = (
                    "MULTI-TF CONFIRMED"
                )

                reason = (
                    "Multiple independent "
                    "factors align on the sell side."
                )


            else:

                decision = "WAIT"

                engine = (
                    "MULTI-FACTOR FILTER"
                )

                reason = (
                    "Evidence is mixed or below "
                    "the confirmation threshold."
                )


            confidence = min(
                95,
                50 + min(
                    45,
                    abs(score) * 5
                )
            )


            if decision == "WAIT":

                confidence = min(
                    confidence,
                    68
                )


            result = {

                "decision": decision,

                "confidence": confidence,

                "score": score,

                "engine": engine,

                "reason": reason,

                "evidence": evidence[-8:],

                "setup":
                    build_trade_setup(
                        decision,
                        tf5
                    ),

                "timeframe": "5m",

                "tfs": {
                    "5m": tf5,
                    "15m": tf15,
                    "1h": tf1h,
                },

                "price": price,

                "updated": now_ts(),
            }


        # ----------------------------------------------------
        # SIGNAL HISTORY
        # ----------------------------------------------------

        previous = state.get(
            "analysis"
        )

        if (
            previous
            and
            previous.get("decision")
            != result.get("decision")
        ):

            state["history"].appendleft(
                {
                    "time":
                        result["updated"],

                    "decision":
                        result["decision"],

                    "score":
                        result["score"],

                    "price":
                        result["price"],
                }
            )


        state["analysis"] = result

        state["last_analysis_ts"] = (
            result["updated"]
        )

        state["engine"] = (
            result["engine"]
        )


# ============================================================
# AI WORKER
# ============================================================

def ai_worker():

    while not stop_event.is_set():

        try:

            rebuild_timeframes()

            generate_analysis()

        except Exception as exc:

            print(
                f"AI WORKER WARNING: {exc}",
                flush=True
            )

        stop_event.wait(1.0)


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker():

    while not stop_event.is_set():

        if not API_KEY:

            time.sleep(5)

            continue


        ws = None

        try:

            with state_lock:

                state["ws_connected"] = False


            ws = websocket.create_connection(
                WS_URL,
                timeout=20,
                enableTrace=False
            )

            ws.settimeout(20)


            subscribe_message = {

                "action": "subscribe",

                "params": {
                    "symbols":
                        GOLD_SYMBOL
                },
            }


            ws.send(
                json.dumps(
                    subscribe_message
                )
            )


            with state_lock:

                state["ws_connected"] = True

                state["ws_reconnects"] += 1


            print(
                "WEBSOCKET: "
                "connected/subscribed to XAU/USD",
                flush=True
            )


            last_log_time = 0


            while not stop_event.is_set():

                raw = ws.recv()

                if raw is None:

                    raise RuntimeError(
                        "WebSocket closed"
                    )


                try:

                    message = json.loads(
                        raw
                    )

                except json.JSONDecodeError:

                    continue


                with state_lock:

                    state[
                        "ws_last_message"
                    ] = now_ts()


                event = message.get(
                    "event"
                )


                # --------------------------------------------
                # LIVE PRICE
                # --------------------------------------------

                if event == "price":

                    price = safe_float(
                        message.get("price")
                    )


                    timestamp = (
                        message.get(
                            "timestamp"
                        )
                    )


                    try:

                        timestamp = int(
                            float(timestamp)
                        )

                    except (
                        TypeError,
                        ValueError
                    ):

                        timestamp = now_ts()


                    if price is not None:

                        update_1m(
                            price,
                            timestamp
                        )


                        # Don't flood Render logs.
                        if (
                            time.time()
                            - last_log_time
                            >= 30
                        ):

                            print(
                                f"LIVE XAU/USD: "
                                f"{price:.5f}",
                                flush=True
                            )

                            last_log_time = (
                                time.time()
                            )


                # --------------------------------------------
                # HEARTBEAT
                # --------------------------------------------

                elif event == "heartbeat":

                    continue


                # --------------------------------------------
                # ERROR
                # --------------------------------------------

                elif (
                    message.get(
                        "status"
                    )
                    == "error"
                ):

                    raise RuntimeError(
                        str(
                            message.get(
                                "message"
                            )
                            or
                            "WebSocket error"
                        )
                    )


        except Exception as exc:

            with state_lock:

                state[
                    "ws_connected"
                ] = False


            print(
                "WEBSOCKET WARNING: "
                f"{exc}; reconnecting...",
                flush=True
            )


            stop_event.wait(3.0)


        finally:

            if ws is not None:

                try:

                    ws.close()

                except Exception:

                    pass


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    global threads_started

    with state_lock:

        if threads_started:

            return

        threads_started = True


    # One historical HTTP bootstrap.
    threading.Thread(
        target=bootstrap_history,
        name="bootstrap",
        daemon=True
    ).start()


    # Live WebSocket.
    threading.Thread(
        target=websocket_worker,
        name="websocket",
        daemon=True
    ).start()


    # Local analysis.
    threading.Thread(
        target=ai_worker,
        name="ai-worker",
        daemon=True
    ).start()


# ============================================================
# DASHBOARD
# ============================================================

@app.route("/")
def home():

    html = r"""
<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1"
>

<title>Trading-AI</title>


<style>

* {
    box-sizing: border-box;
}


body {

    margin: 0;

    background: #07111f;

    color: #e8eef7;

    font-family:
        Arial,
        Helvetica,
        sans-serif;
}


.wrap {

    max-width: 1180px;

    margin: auto;

    padding: 22px;
}


.card {

    background: #0c1929;

    border: 1px solid #1c314a;

    border-radius: 14px;

    padding: 18px;

    margin-bottom: 14px;

    box-shadow:
        0 8px 30px
        rgba(0,0,0,.18);
}


.top {

    display: flex;

    justify-content:
        space-between;

    gap: 20px;

    align-items: center;
}


.title {

    font-size: 25px;

    font-weight: 700;
}


.sub {

    color: #8da2b8;

    font-size: 13px;

    margin-top: 4px;
}


.price {

    font-size: 34px;

    font-weight: 700;
}


.live {

    font-size: 12px;

    color: #7ee2a8;
}


.grid {

    display: grid;

    grid-template-columns:
        repeat(4, 1fr);

    gap: 12px;
}


.metric {

    background: #091523;

    border: 1px solid #1a2c40;

    border-radius: 10px;

    padding: 13px;
}


.label {

    font-size: 11px;

    color: #7f95ab;

    text-transform:
        uppercase;
}


.value {

    font-size: 17px;

    font-weight: 700;

    margin-top: 6px;
}


.decision {

    font-size: 34px;

    font-weight: 800;

    margin:
        8px 0 14px;
}


.muted {

    color: #8da2b8;
}


.row {

    display: flex;

    justify-content:
        space-between;

    gap: 12px;

    padding: 8px 0;

    border-bottom:
        1px solid #17283a;
}


.row:last-child {

    border-bottom: 0;
}


.history {

    max-height: 220px;

    overflow: auto;
}


@media(max-width:850px) {

    .grid {

        grid-template-columns:
            repeat(2, 1fr);
    }

    .top {

        flex-direction: column;

        align-items: flex-start;
    }
}


@media(max-width:520px) {

    .grid {

        grid-template-columns:
            1fr;
    }

    .wrap {

        padding: 12px;
    }
}

</style>

</head>


<body>


<div class="wrap">


<div class="card top">

<div>

<div class="title">
🥇 Gold — XAU/USD
</div>

<div class="sub">
Trading-AI • WebSocket-first • Multi-timeframe confirmation
</div>

</div>


<div style="text-align:right">

<div
id="price"
class="price"
>
—
</div>

<div
id="live"
class="live"
>
● CONNECTING
</div>

</div>

</div>


<div class="card">

<div class="label">
Data Status
</div>

<div
id="data"
class="value"
>
STARTING
</div>

<div
id="bootstrap"
class="sub"
>
History: waiting
</div>

</div>


<div class="grid">


<div class="metric">

<div class="label">
Trend
</div>

<div
id="trend"
class="value"
>
WAITING
</div>

</div>


<div class="metric">

<div class="label">
Momentum
</div>

<div
id="momentum"
class="value"
>
WAITING
</div>

</div>


<div class="metric">

<div class="label">
Structure
</div>

<div
id="structure"
class="value"
>
WAITING
</div>

</div>


<div class="metric">

<div class="label">
RSI
</div>

<div
id="rsi"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
EMA 20
</div>

<div
id="ema20"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
EMA 50
</div>

<div
id="ema50"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Support
</div>

<div
id="support"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Resistance
</div>

<div
id="resistance"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Liquidity High
</div>

<div
id="lh"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Liquidity Low
</div>

<div
id="ll"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Sweep
</div>

<div
id="sweep"
class="value"
>
NONE
</div>

</div>


<div class="metric">

<div class="label">
5m Candles
</div>

<div
id="count5"
class="value"
>
0
</div>

</div>


</div>


<div class="card">


<div class="label">
TRADING-AI CONCLUSION
</div>


<div
id="decision"
class="decision"
>
WAIT
</div>


<div class="row">

<span>
Confidence
</span>

<b id="confidence">
50%
</b>

</div>


<div class="row">

<span>
AI Score
</span>

<b id="score">
0
</b>

</div>


<div class="row">

<span>
Engine
</span>

<b id="engine">
WAITING
</b>

</div>


<div class="row">

<span>
Why
</span>

<b id="reason">
Waiting for confirmation.
</b>

</div>


<div class="row">

<span>
Evidence
</span>

<b id="evidence">
—
</b>

</div>


</div>


<div class="card">


<div class="label">
AI TRADE SETUP
</div>


<div class="grid">


<div class="metric">

<div class="label">
Entry
</div>

<div
id="entry"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Stop Loss
</div>

<div
id="sl"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Target 1
</div>

<div
id="t1"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Target 2
</div>

<div
id="t2"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
Target 3
</div>

<div
id="t3"
class="value"
>
—
</div>

</div>


<div class="metric">

<div class="label">
R:R
</div>

<div
id="rr"
class="value"
>
—
</div>

</div>


</div>


<div class="row">

<span>
Invalidation
</span>

<b id="invalid">
—
</b>

</div>


</div>


<div class="card">


<div class="label">
TIMEFRAME DATA
</div>


<div id="tfdata">
</div>


</div>


<div class="card">


<div class="label">
AI SIGNAL HISTORY
</div>


<div
id="history"
class="history muted"
>
No signal changes recorded yet.
</div>


</div>


<div class="card">


<div class="label">
Crude Oil — WTI
</div>


<div class="muted">

PLAN LIMIT — WTI/USD is currently
disabled in this build because the
active Twelve Data plan/feed does
not provide it.

</div>


</div>


</div>


<script>


function fmt(value) {

    if (value === null ||
        value === undefined) {

        return "—";
    }

    return Number(value).toFixed(3);
}


function setText(id, value) {

    document.getElementById(id)
        .textContent = value;
}


function updateDashboard(data) {


    const analysis =
        data.analysis || {};


    const tfs =
        analysis.tfs || {};


    const tf5 =
        tfs["5m"] || {};


    setText(
        "price",
        data.price == null
            ? "—"
            : Number(data.price)
                .toFixed(3)
    );


    setText(
        "live",
        data.ws_connected
            ? "● LIVE"
            : "● RECONNECTING"
    );


    setText(
        "data",
        data.data_status ||
        "STARTING"
    );


    setText(
        "bootstrap",
        "History: " +
        (
            data.bootstrap_status ||
            "waiting"
        )
    );


    setText(
        "trend",
        tf5.trend ||
        "WAITING"
    );


    setText(
        "momentum",
        tf5.momentum ||
        "WAITING"
    );


    setText(
        "structure",
        tf5.structure ||
        "WAITING"
    );


    setText(
        "rsi",
        tf5.rsi == null
            ? "—"
            : Number(tf5.rsi)
                .toFixed(2)
    );


    setText(
        "ema20",
        fmt(tf5.ema20)
    );


    setText(
        "ema50",
        fmt(tf5.ema50)
    );


    setText(
        "support",
        fmt(tf5.support)
    );


    setText(
        "resistance",
        fmt(tf5.resistance)
    );


    setText(
        "lh",
        fmt(tf5.liquidity_high)
    );


    setText(
        "ll",
        fmt(tf5.liquidity_low)
    );


    setText(
        "sweep",
        tf5.sweep ||
        "NONE"
    );


    setText(
        "count5",
        tf5.count || 0
    );


    setText(
        "decision",
        analysis.decision ||
        "WAIT"
    );


    setText(
        "confidence",
        (
            analysis.confidence ??
            50
        ) + "%"
    );


    setText(
        "score",
        analysis.score ??
        0
    );


    setText(
        "engine",
        analysis.engine ||
        "WAITING"
    );


    setText(
        "reason",
        analysis.reason ||
        "Waiting for confirmation."
    );


    setText(
        "evidence",
        (
            analysis.evidence ||
            []
        ).join(" • ") ||
        "—"
    );


    const setup =
        analysis.setup || {};


    setText(
        "entry",
        fmt(setup.entry)
    );


    setText(
        "sl",
        fmt(setup.stop_loss)
    );


    setText(
        "t1",
        fmt(setup.target1)
    );


    setText(
        "t2",
        fmt(setup.target2)
    );


    setText(
        "t3",
        fmt(setup.target3)
    );


    setText(
        "rr",
        setup.rr ||
        "—"
    );


    setText(
        "invalid",
        setup.invalidation ||
        "—"
    );


    const rows =
        ["5m", "15m", "1h"]
        .map(function(key) {

            const x =
                tfs[key] || {};

            const rsiText =
                x.rsi == null
                    ? "—"
                    : Number(x.rsi)
                        .toFixed(1);


            return `
                <div class="row">
                    <span>${key}</span>
                    <b>
                        ${x.count || 0}
                        candles •
                        ${x.trend || "WAITING"} •
                        ${x.momentum || "WAITING"} •
                        RSI ${rsiText}
                    </b>
                </div>
            `;
        })
        .join("");


    document.getElementById(
        "tfdata"
    ).innerHTML = rows;


    const history =
        data.history || [];


    if (!history.length) {

        document.getElementById(
            "history"
        ).textContent =
            "No signal changes recorded yet.";

    } else {

        document.getElementById(
            "history"
        ).innerHTML = history
            .map(function(item) {

                const time =
                    new Date(
                        item.time * 1000
                    ).toLocaleTimeString();


                const price =
                    item.price == null
                        ? "—"
                        : Number(
                            item.price
                        ).toFixed(3);


                return `
                    <div class="row">
                        <span>
                            ${time}
                        </span>

                        <b>
                            ${item.decision}
                            • score
                            ${item.score}
                            • ${price}
                        </b>
                    </div>
                `;

            })
            .join("");
    }
}


async function refreshDashboard() {

    try {

        const response =
            await fetch(
                "/api/market",
                {
                    cache: "no-store"
                }
            );


        const data =
            await response.json();


        updateDashboard(
            data
        );


    } catch (error) {

        setText(
            "live",
            "● CONNECTION ERROR"
        );
    }
}


refreshDashboard();


setInterval(
    refreshDashboard,
    1000
);

</script>


</body>

</html>
"""

    return Response(
        html,
        mimetype="text/html"
    )


# ============================================================
# API
# ============================================================

@app.route("/api/market")
def api_market():

    with state_lock:

        payload = {

            "price":
                state["price"],

            "last_price":
                iso_from_ts(
                    state["last_price_ts"]
                ),

            "ws_connected":
                state["ws_connected"],

            "data_status":
                state["data_status"],

            "bootstrap_status":
                state["bootstrap_status"],

            "bootstrap_error":
                state["bootstrap_error"],

            "analysis":
                state["analysis"],

            "history":
                list(
                    state["history"]
                ),

            "engine":
                state["engine"],

            "candles": {

                "1m":
                    len(candles_1m),

                "5m":
                    len(candles_5m),

                "15m":
                    len(candles_15m),

                "30m":
                    len(candles_30m),

                "1h":
                    len(candles_1h),
            },
        }


    return jsonify(
        payload
    )


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    with state_lock:

        return jsonify({

            "ok": True,

            "price":
                state["price"],

            "ws_connected":
                state["ws_connected"],

            "data_status":
                state["data_status"],

            "bootstrap_status":
                state["bootstrap_status"],
        })


# ============================================================
# STREAM
#
# Kept because previous deployment/interface used /stream.
# ============================================================

@app.route("/stream")
def stream():

    with state_lock:

        return jsonify({

            "event": "status",

            "connected":
                state["ws_connected"],

            "price":
                state["price"],
        })


# ============================================================
# START
# ============================================================

start_workers()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                "5000"
            )
        ),
        debug=False
    )
