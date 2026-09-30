from flask import Flask, Response, jsonify, render_template_string
import os
import json
import time
import threading
import queue
import requests
import websocket
from datetime import datetime, timezone


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# Oil intentionally disabled until the account/feed supports it.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"
PRICE_URL = "https://api.twelvedata.com/price"
HISTORY_URL = "https://api.twelvedata.com/time_series"

PORT = int(os.environ.get("PORT", "10000"))


# ============================================================
# TIMEFRAMES
# ============================================================

# We fetch only 5m + 1day from Twelve Data.
# Higher intraday timeframes are built locally from 5m candles.
BASE_INTERVALS = [
    "5min",
    "1day"
]

DISPLAY_TIMEFRAMES = [
    "5min",
    "15min",
    "30min",
    "1h",
    "4h",
    "1day"
]


# ============================================================
# GLOBAL STATE
# ============================================================

lock = threading.RLock()
clients_lock = threading.Lock()

workers_started = False

clients = []

stop_event = threading.Event()

state = {
    "gold": {
        "symbol": GOLD_SYMBOL,
        "name": "Gold — XAU/USD",

        "price": None,

        "trend": "WAITING",
        "momentum": "WAITING",
        "structure": "WAITING",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "support": None,
        "resistance": None,

        "liquidity_high": None,
        "liquidity_low": None,

        "sweep": "NONE",

        "signal": "WAIT",
        "confidence": 50,
        "score": 0,

        "connection": "STARTING",
        "price_source": "NONE",

        "updated": None,

        "timeframes": {},

        "candles": {},

        "signal_history": [],

        "trade_plan": {
            "signal": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
            "target_3": None,
            "risk_reward": None,
            "invalidation": None,
            "reason": []
        },

        "error": None
    },

    "oil": {
        "symbol": OIL_SYMBOL,
        "name": "Crude Oil — WTI",

        "price": None,

        "trend": "UNAVAILABLE",
        "momentum": "UNAVAILABLE",
        "structure": "UNAVAILABLE",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "support": None,
        "resistance": None,

        "liquidity_high": None,
        "liquidity_low": None,

        "sweep": "NONE",

        "signal": "WAIT",
        "confidence": 0,
        "score": 0,

        "connection": "PLAN LIMIT",

        "updated": None,

        "timeframes": {},

        "error": (
            "WTI/USD is disabled until the current "
            "Twelve Data account/feed supports it."
        )
    }
}


# ============================================================
# BASIC HELPERS
# ============================================================

def now_text():
    return time.strftime("%H:%M:%S")


def number(value):
    try:
        if value is None:
            return None

        return float(value)

    except Exception:
        return None


def safe_round(value, digits=3):
    value = number(value)

    if value is None:
        return None

    return round(value, digits)


def parse_datetime(value):
    if not value:
        return None

    text = str(value).strip()

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ"
    ]

    for fmt in formats:

        try:

            dt = datetime.strptime(text, fmt)

            return dt.replace(
                tzinfo=timezone.utc
            ).timestamp()

        except Exception:
            pass

    try:

        dt = datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.timestamp()

    except Exception:

        return None


def candle_time(candle):
    return parse_datetime(
        candle.get("datetime")
        or candle.get("date")
    )


# ============================================================
# SSE BROADCAST
# ============================================================

def broadcast():

    with lock:

        payload = json.dumps(
            state,
            separators=(",", ":")
        )

    dead = []

    with clients_lock:

        for q in clients:

            try:

                q.put_nowait(payload)

            except Exception:

                dead.append(q)

        for q in dead:

            if q in clients:
                clients.remove(q)


# ============================================================
# PRICE UPDATE
# ============================================================

def update_gold_price(
    price,
    source="REST"
):

    price = number(price)

    if price is None:
        return

    with lock:

        state["gold"]["price"] = price

        state["gold"]["updated"] = now_text()

        state["gold"]["connection"] = "CONNECTED"

        state["gold"]["price_source"] = source

        state["gold"]["error"] = None

    print(
        f"STATE UPDATED: GOLD = {price} "
        f"SOURCE={source}"
    )

    broadcast()


# ============================================================
# TWELVE DATA REQUEST
# ============================================================

def twelve_request(
    endpoint,
    params,
    timeout=15
):

    if not API_KEY:

        raise RuntimeError(
            "TWELVE_DATA_API_KEY is missing"
        )

    request_params = dict(params)

    request_params["apikey"] = API_KEY

    response = requests.get(
        endpoint,
        params=request_params,
        timeout=timeout
    )

    content_type = (
        response.headers.get(
            "content-type",
            ""
        )
    )

    try:
        data = response.json()

    except Exception:

        body = response.text[:500]

        raise RuntimeError(
            f"Twelve Data returned non-JSON "
            f"HTTP {response.status_code}: {body}"
        )

    if response.status_code != 200:

        raise RuntimeError(
            f"Twelve Data HTTP "
            f"{response.status_code}: "
            f"{data}"
        )

    if isinstance(data, dict):

        if data.get("status") == "error":

            raise RuntimeError(
                str(
                    data.get(
                        "message",
                        data
                    )
                )
            )

    return data


# ============================================================
# GET LIVE PRICE VIA REST
# ============================================================

def get_rest_price():

    data = twelve_request(
        PRICE_URL,
        {
            "symbol": GOLD_SYMBOL
        },
        timeout=10
    )

    price = number(
        data.get("price")
    )

    if price is None:

        raise RuntimeError(
            f"Invalid price response: {data}"
        )

    return price


# ============================================================
# REST PRICE FALLBACK LOOP
# ============================================================

def rest_price_loop():

    print(
        "REST PRICE FALLBACK WORKER STARTED"
    )

    while not stop_event.is_set():

        try:

            price = get_rest_price()

            update_gold_price(
                price,
                source="REST"
            )

        except Exception as exc:

            error_text = (
                f"REST PRICE ERROR: {repr(exc)}"
            )

            print(error_text)

            with lock:

                if state["gold"]["price"] is None:

                    state["gold"]["connection"] = (
                        "WAITING FOR DATA"
                    )

                    state["gold"]["error"] = (
                        str(exc)
                    )

            broadcast()

        stop_event.wait(15)


# ============================================================
# HISTORY
# ============================================================

def get_candles(
    symbol,
    interval,
    outputsize=500
):

    try:

        data = twelve_request(
            HISTORY_URL,
            {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "timezone": "UTC"
            },
            timeout=20
        )

        values = data.get(
            "values",
            []
        )

        if not values:
            return []

        cleaned = []

        for item in values:

            try:

                o = number(item.get("open"))
                h = number(item.get("high"))
                l = number(item.get("low"))
                c = number(item.get("close"))

                if (
                    o is None
                    or h is None
                    or l is None
                    or c is None
                ):
                    continue

                if not item.get("datetime"):
                    continue

                cleaned.append({
                    "datetime": item["datetime"],
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c
                })

            except Exception:

                continue

        cleaned.sort(
            key=lambda x: (
                candle_time(x)
                or 0
            )
        )

        return cleaned

    except Exception as exc:

        print(
            f"HISTORY ERROR "
            f"{symbol} {interval}: "
            f"{repr(exc)}"
        )

        return []


# ============================================================
# CANDLE AGGREGATION
# ============================================================

def floor_bucket(
    timestamp,
    minutes
):

    seconds = int(minutes * 60)

    return (
        int(timestamp)
        // seconds
    ) * seconds


def timestamp_to_text(timestamp):

    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def aggregate_candles(
    candles,
    minutes
):

    if not candles:
        return []

    buckets = {}

    for candle in candles:

        ts = candle_time(candle)

        if ts is None:
            continue

        bucket = floor_bucket(
            ts,
            minutes
        )

        if bucket not in buckets:

            buckets[bucket] = {
                "datetime":
                    timestamp_to_text(bucket),

                "open":
                    number(candle["open"]),

                "high":
                    number(candle["high"]),

                "low":
                    number(candle["low"]),

                "close":
                    number(candle["close"])
            }

        else:

            item = buckets[bucket]

            item["high"] = max(
                item["high"],
                number(candle["high"])
            )

            item["low"] = min(
                item["low"],
                number(candle["low"])
            )

            item["close"] = number(
                candle["close"]
            )

    result = list(
        buckets.values()
    )

    result.sort(
        key=lambda x: (
            candle_time(x)
            or 0
        )
    )

    return result


def build_local_timeframes(
    candles_5m,
    candles_1d
):

    result = {}

    result["5min"] = candles_5m

    result["15min"] = aggregate_candles(
        candles_5m,
        15
    )

    result["30min"] = aggregate_candles(
        candles_5m,
        30
    )

    result["1h"] = aggregate_candles(
        candles_5m,
        60
    )

    result["4h"] = aggregate_candles(
        candles_5m,
        240
    )

    result["1day"] = candles_1d

    return result


# ============================================================
# EMA
# ============================================================

def ema(
    values,
    period
):

    if not values:
        return None

    if len(values) < period:
        return None

    multiplier = (
        2.0
        / (period + 1.0)
    )

    result = sum(
        values[:period]
    ) / period

    for value in values[period:]:

        result = (
            (value - result)
            * multiplier
        ) + result

    return result


# ============================================================
# RSI
# ============================================================

def rsi(
    values,
    period=14
):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(values)
    ):

        change = (
            values[i]
            - values[i - 1]
        )

        if change >= 0:

            gains.append(change)
            losses.append(0.0)

        else:

            gains.append(0.0)
            losses.append(abs(change))

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

    rs = (
        avg_gain
        / avg_loss
    )

    return (
        100.0
        - (
            100.0
            / (1.0 + rs)
        )
    )


# ============================================================
# ATR
# ============================================================

def atr(
    candles,
    period=14
):

    if len(candles) < period + 1:
        return None

    trs = []

    previous_close = None

    for candle in candles:

        high = number(
            candle.get("high")
        )

        low = number(
            candle.get("low")
        )

        close = number(
            candle.get("close")
        )

        if (
            high is None
            or low is None
            or close is None
        ):
            continue

        if previous_close is None:

            tr = high - low

        else:

            tr = max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close)
            )

        trs.append(tr)

        previous_close = close

    if len(trs) < period:
        return None

    value = (
        sum(trs[:period])
        / period
    )

    for tr in trs[period:]:

        value = (
            (
                value * (period - 1)
            )
            + tr
        ) / period

    return value


# ============================================================
# MARKET STRUCTURE
# ============================================================

def market_structure(
    candles,
    lookback=20
):

    if len(candles) < lookback:
        return "RANGE"

    recent = candles[-lookback:]

    highs = [
        number(x["high"])
        for x in recent
        if number(x["high"]) is not None
    ]

    lows = [
        number(x["low"])
        for x in recent
        if number(x["low"]) is not None
    ]

    if len(highs) < 10 or len(lows) < 10:
        return "RANGE"

    mid = len(recent) // 2

    first = recent[:mid]
    second = recent[mid:]

    first_high = max(
        number(x["high"])
        for x in first
    )

    second_high = max(
        number(x["high"])
        for x in second
    )

    first_low = min(
        number(x["low"])
        for x in first
    )

    second_low = min(
        number(x["low"])
        for x in second
    )

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

    if second_high > first_high:
        return "BULLISH BREAK"

    if second_low < first_low:
        return "BEARISH BREAK"

    return "RANGE / MIXED"


# ============================================================
# LIQUIDITY
# ============================================================

def liquidity_levels(
    candles,
    lookback=30
):

    if not candles:
        return None, None

    recent = candles[-lookback:]

    highs = [
        number(x["high"])
        for x in recent
        if number(x["high"]) is not None
    ]

    lows = [
        number(x["low"])
        for x in recent
        if number(x["low"]) is not None
    ]

    if not highs or not lows:
        return None, None

    return (
        max(highs),
        min(lows)
    )


# ============================================================
# LIQUIDITY SWEEP
# ============================================================

def detect_sweep(
    candles,
    lookback=20
):

    if len(candles) < 5:
        return "NONE"

    latest = candles[-1]

    previous = candles[
        -lookback - 1:-1
    ]

    if not previous:
        return "NONE"

    previous_high = max(
        number(x["high"])
        for x in previous
    )

    previous_low = min(
        number(x["low"])
        for x in previous
    )

    latest_high = number(
        latest["high"]
    )

    latest_low = number(
        latest["low"]
    )

    latest_close = number(
        latest["close"]
    )

    if (
        latest_high > previous_high
        and latest_close < previous_high
    ):
        return "HIGH SWEEP"

    if (
        latest_low < previous_low
        and latest_close > previous_low
    ):
        return "LOW SWEEP"

    return "NONE"


# ============================================================
# ANALYZE TIMEFRAME
# ============================================================

def analyze_timeframe(
    candles
):

    if len(candles) < 55:
        return {}

    closes = [
        number(x["close"])
        for x in candles
        if number(x["close"]) is not None
    ]

    if len(closes) < 55:
        return {}

    current = closes[-1]

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

    current_rsi = rsi(
        closes,
        14
    )

    current_atr = atr(
        candles,
        14
    )

    support, resistance = (
        liquidity_levels(
            candles,
            30
        )
    )

    liquidity_high, liquidity_low = (
        liquidity_levels(
            candles,
            30
        )
    )

    sweep = detect_sweep(
        candles,
        20
    )

    structure = market_structure(
        candles,
        20
    )

    # --------------------------------------------------------
    # TREND
    # --------------------------------------------------------

    if (
        ema20 is not None
        and ema50 is not None
    ):

        if current > ema20 > ema50:

            trend = "BULLISH"

        elif current < ema20 < ema50:

            trend = "BEARISH"

        else:

            trend = "NEUTRAL"

    else:

        trend = "NEUTRAL"

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    if (
        ema9 is not None
        and ema20 is not None
        and current_rsi is not None
    ):

        if (
            ema9 > ema20
            and current_rsi >= 55
        ):

            momentum = "BUYING"

        elif (
            ema9 < ema20
            and current_rsi <= 45
        ):

            momentum = "SELLING"

        else:

            momentum = "NEUTRAL"

    else:

        momentum = "NEUTRAL"

    return {

        "price": safe_round(
            current,
            3
        ),

        "trend": trend,

        "momentum": momentum,

        "structure": structure,

        "rsi": safe_round(
            current_rsi,
            2
        ),

        "ema9": safe_round(
            ema9,
            3
        ),

        "ema20": safe_round(
            ema20,
            3
        ),

        "ema50": safe_round(
            ema50,
            3
        ),

        "atr": safe_round(
            current_atr,
            3
        ),

        "support": safe_round(
            support,
            3
        ),

        "resistance": safe_round(
            resistance,
            3
        ),

        "liquidity_high": safe_round(
            liquidity_high,
            3
        ),

        "liquidity_low": safe_round(
            liquidity_low,
            3
        ),

        "sweep": sweep,

        "candles": candles[-250:]
    }


# ============================================================
# MULTI-TIMEFRAME AI ENGINE
# ============================================================

def build_ai_analysis(
    timeframes,
    live_price
):

    score = 0

    reasons = []

    five = timeframes.get(
        "5min",
        {}
    )

    fifteen = timeframes.get(
        "15min",
        {}
    )

    thirty = timeframes.get(
        "30min",
        {}
    )

    one_hour = timeframes.get(
        "1h",
        {}
    )

    four_hour = timeframes.get(
        "4h",
        {}
    )

    def add(
        condition,
        points,
        reason
    ):

        nonlocal score

        if condition:

            score += points

            reasons.append(
                reason
            )

    # --------------------------------------------------------
    # 5 MINUTE
    # --------------------------------------------------------

    add(
        five.get("trend") == "BULLISH",
        2,
        "5m trend bullish"
    )

    add(
        five.get("trend") == "BEARISH",
        -2,
        "5m trend bearish"
    )

    add(
        five.get("momentum") == "BUYING",
        1,
        "5m momentum buying"
    )

    add(
        five.get("momentum") == "SELLING",
        -1,
        "5m momentum selling"
    )

    # --------------------------------------------------------
    # 15 MINUTE
    # --------------------------------------------------------

    add(
        fifteen.get("trend") == "BULLISH",
        2,
        "15m trend bullish"
    )

    add(
        fifteen.get("trend") == "BEARISH",
        -2,
        "15m trend bearish"
    )

    add(
        fifteen.get("structure")
        in (
            "HIGHER HIGH / HIGHER LOW",
            "BULLISH BREAK"
        ),
        1,
        "15m structure supportive of buyers"
    )

    add(
        fifteen.get("structure")
        in (
            "LOWER HIGH / LOWER LOW",
            "BEARISH BREAK"
        ),
        -1,
        "15m structure supportive of sellers"
    )

    # --------------------------------------------------------
    # 30 MINUTE
    # --------------------------------------------------------

    add(
        thirty.get("trend") == "BULLISH",
        1,
        "30m trend bullish"
    )

    add(
        thirty.get("trend") == "BEARISH",
        -1,
        "30m trend bearish"
    )

    # --------------------------------------------------------
    # 1 HOUR
    # --------------------------------------------------------

    add(
        one_hour.get("trend") == "BULLISH",
        2,
        "1H trend bullish"
    )

    add(
        one_hour.get("trend") == "BEARISH",
        -2,
        "1H trend bearish"
    )

    # --------------------------------------------------------
    # 4 HOUR
    # --------------------------------------------------------

    add(
        four_hour.get("trend") == "BULLISH",
        1,
        "4H trend bullish"
    )

    add(
        four_hour.get("trend") == "BEARISH",
        -1,
        "4H trend bearish"
    )

    # --------------------------------------------------------
    # LIQUIDITY SWEEP
    # --------------------------------------------------------

    sweep = five.get(
        "sweep",
        "NONE"
    )

    add(
        sweep == "LOW SWEEP",
        1,
        "5m low liquidity sweep"
    )

    add(
        sweep == "HIGH SWEEP",
        -1,
        "5m high liquidity sweep"
    )

    # --------------------------------------------------------
    # RSI FILTER
    # --------------------------------------------------------

    current_rsi = five.get(
        "rsi"
    )

    if current_rsi is not None:

        if current_rsi >= 75:

            reasons.append(
                "5m RSI extreme high — chase risk"
            )

        elif current_rsi <= 25:

            reasons.append(
                "5m RSI extreme low — chase risk"
            )

    # --------------------------------------------------------
    # SCORE LIMIT
    # --------------------------------------------------------

    score = max(
        -10,
        min(
            10,
            score
        )
    )

    # --------------------------------------------------------
    # CONFLICT FILTER
    # --------------------------------------------------------

    conflict = False

    if (
        five.get("trend") in (
            "BULLISH",
            "BEARISH"
        )
        and fifteen.get("trend") in (
            "BULLISH",
            "BEARISH"
        )
        and five.get("trend")
        != fifteen.get("trend")
    ):

        conflict = True

        reasons.append(
            "5m / 15m trend conflict"
        )

    if (
        thirty.get("trend") in (
            "BULLISH",
            "BEARISH"
        )
        and one_hour.get("trend") in (
            "BULLISH",
            "BEARISH"
        )
        and thirty.get("trend")
        != one_hour.get("trend")
    ):

        conflict = True

        reasons.append(
            "30m / 1H trend conflict"
        )

    # --------------------------------------------------------
    # EXTREME RSI FILTER
    # --------------------------------------------------------

    extreme_rsi = (
        current_rsi is not None
        and (
            current_rsi >= 75
            or current_rsi <= 25
        )
    )

    # --------------------------------------------------------
    # DECISION
    # --------------------------------------------------------

    if conflict:

        decision = "WAIT"

    elif extreme_rsi:

        decision = "WAIT"

    elif score >= 6:

        decision = "BUY"

    elif score <= -6:

        decision = "SELL"

    else:

        decision = "WAIT"

    # --------------------------------------------------------
    # CONFIRMATION STRENGTH
    # --------------------------------------------------------

    if decision == "WAIT":

        confidence = min(
            68,
            50 + abs(score) * 3
        )

    else:

        confidence = min(
            95,
            50 + abs(score) * 5
        )

    return {

        "signal": decision,

        "confidence": confidence,

        "score": score,

        "reasons": reasons[:12]
    }


# ============================================================
# TRADE PLAN
# ============================================================

def build_trade_plan(
    timeframes,
    live_price,
    ai
):

    if (
        live_price is None
        or ai["signal"] == "WAIT"
    ):

        return {

            "signal": "WAIT",

            "entry": None,

            "stop_loss": None,

            "target_1": None,

            "target_2": None,

            "target_3": None,

            "risk_reward": None,

            "invalidation": None,

            "reason": ai.get(
                "reasons",
                []
            )
        }

    five = timeframes.get(
        "5min",
        {}
    )

    fifteen = timeframes.get(
        "15min",
        {}
    )

    entry = float(
        live_price
    )

    atr_value = (
        five.get("atr")
        or fifteen.get("atr")
    )

    support = (
        five.get("support")
        or fifteen.get("support")
    )

    resistance = (
        five.get("resistance")
        or fifteen.get("resistance")
    )

    if atr_value is None:

        atr_value = (
            entry * 0.002
        )

    atr_value = max(
        float(atr_value),
        entry * 0.001
    )

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    if ai["signal"] == "BUY":

        if (
            support is not None
            and support < entry
        ):

            stop = min(
                support,
                entry - atr_value
            )

        else:

            stop = (
                entry
                - 1.2 * atr_value
            )

        risk = max(
            entry - stop,
            entry * 0.001
        )

        target_1 = entry + risk
        target_2 = entry + (2 * risk)
        target_3 = entry + (3 * risk)

    # --------------------------------------------------------
    # SELL
    # --------------------------------------------------------

    else:

        if (
            resistance is not None
            and resistance > entry
        ):

            stop = max(
                resistance,
                entry + atr_value
            )

        else:

            stop = (
                entry
                + 1.2 * atr_value
            )

        risk = max(
            stop - entry,
            entry * 0.001
        )

        target_1 = entry - risk
        target_2 = entry - (2 * risk)
        target_3 = entry - (3 * risk)

    return {

        "signal": ai["signal"],

        "entry": safe_round(
            entry
        ),

        "stop_loss": safe_round(
            stop
        ),

        "target_1": safe_round(
            target_1
        ),

        "target_2": safe_round(
            target_2
        ),

        "target_3": safe_round(
            target_3
        ),

        "risk_reward":
            "1:1 / 1:2 / 1:3",

        "invalidation": safe_round(
            stop
        ),

        "reason": ai.get(
            "reasons",
            []
        )
    }


# ============================================================
# SIGNAL HISTORY
# ============================================================

def record_signal_history(
    ai,
    live_price
):

    with lock:

        history = state[
            "gold"
        ].setdefault(
            "signal_history",
            []
        )

        previous = (
            history[-1]
            if history
            else None
        )

        if (
            previous
            and previous.get("score")
            == ai["score"]
            and previous.get("signal")
            == ai["signal"]
        ):

            return

        history.append({

            "time":
                now_text(),

            "price":
                safe_round(
                    live_price
                ),

            "score":
                ai["score"],

            "signal":
                ai["signal"],

            "confidence":
                ai["confidence"],

            "reasons":
                ai.get(
                    "reasons",
                    []
                )[:8]
        })

        state[
            "gold"
        ][
            "signal_history"
        ] = history[-50:]


# ============================================================
# ANALYSIS WORKER
# ============================================================

def analysis_loop():

    print(
        "ANALYSIS WORKER STARTED"
    )

    first_run = True

    while not stop_event.is_set():

        try:

            # ------------------------------------------------
            # Fetch base data
            # ------------------------------------------------

            print(
                "ANALYSIS: fetching 5m history..."
            )

            candles_5m = get_candles(
                GOLD_SYMBOL,
                "5min",
                500
            )

            print(
                f"ANALYSIS: 5m candles = "
                f"{len(candles_5m)}"
            )

            print(
                "ANALYSIS: fetching daily history..."
            )

            candles_1d = get_candles(
                GOLD_SYMBOL,
                "1day",
                250
            )

            print(
                f"ANALYSIS: 1D candles = "
                f"{len(candles_1d)}"
            )

            if not candles_5m:

                with lock:

                    state[
                        "gold"
                    ][
                        "error"
                    ] = (
                        "No 5m history received "
                        "from Twelve Data."
                    )

                broadcast()

                stop_event.wait(
                    60
                )

                continue

            # ------------------------------------------------
            # Build local timeframes
            # ------------------------------------------------

            timeframe_candles = (
                build_local_timeframes(
                    candles_5m,
                    candles_1d
                )
            )

            timeframe_data = {}

            for interval in DISPLAY_TIMEFRAMES:

                candles = (
                    timeframe_candles.get(
                        interval,
                        []
                    )
                )

                result = (
                    analyze_timeframe(
                        candles
                    )
                )

                if result:

                    timeframe_data[
                        interval
                    ] = result

            # ------------------------------------------------
            # Live price
            # ------------------------------------------------

            with lock:

                live_price = (
                    state[
                        "gold"
                    ][
                        "price"
                    ]
                )

            if live_price is None:

                live_price = (
                    timeframe_data
                    .get(
                        "5min",
                        {}
                    )
                    .get(
                        "price"
                    )
                )

            # ------------------------------------------------
            # AI
            # ------------------------------------------------

            if live_price is not None:

                ai = build_ai_analysis(
                    timeframe_data,
                    live_price
                )

                trade_plan = (
                    build_trade_plan(
                        timeframe_data,
                        live_price,
                        ai
                    )
                )

                five = (
                    timeframe_data.get(
                        "5min",
                        {}
                    )
                )

                record_signal_history(
                    ai,
                    live_price
                )

                with lock:

                    state[
                        "gold"
                    ][
                        "timeframes"
                    ] = timeframe_data

                    state[
                        "gold"
                    ][
                        "candles"
                    ] = {
                        k: v.get(
                            "candles",
                            []
                        )
                        for k, v
                        in timeframe_data.items()
                    }

                    state[
                        "gold"
                    ][
                        "trade_plan"
                    ] = trade_plan

                    state[
                        "gold"
                    ].update({

                        "trend":
                            five.get(
                                "trend",
                                "NEUTRAL"
                            ),

                        "momentum":
                            five.get(
                                "momentum",
                                "NEUTRAL"
                            ),

                        "structure":
                            five.get(
                                "structure",
                                "RANGE"
                            ),

                        "rsi":
                            five.get(
                                "rsi"
                            ),

                        "ema20":
                            five.get(
                                "ema20"
                            ),

                        "ema50":
                            five.get(
                                "ema50"
                            ),

                        "support":
                            five.get(
                                "support"
                            ),

                        "resistance":
                            five.get(
                                "resistance"
                            ),

                        "liquidity_high":
                            five.get(
                                "liquidity_high"
                            ),

                        "liquidity_low":
                            five.get(
                                "liquidity_low"
                            ),

                        "sweep":
                            five.get(
                                "sweep",
                                "NONE"
                            ),

                        "signal":
                            ai[
                                "signal"
                            ],

                        "confidence":
                            ai[
                                "confidence"
                            ],

                        "score":
                            ai[
                                "score"
                            ],

                        "updated":
                            now_text(),

                        "error":
                            None
                    })

                print(
                    "AI UPDATE:",
                    f"price={live_price}",
                    f"score={ai['score']}",
                    f"decision={ai['signal']}"
                )

                broadcast()

            first_run = False

        except Exception as exc:

            print(
                "AI LOOP ERROR:",
                repr(exc)
            )

            with lock:

                state[
                    "gold"
                ][
                    "error"
                ] = str(exc)

            broadcast()

        # ----------------------------------------------------
        # Wait before next analysis
        # ----------------------------------------------------

        if first_run:

            stop_event.wait(
                5
            )

        else:

            stop_event.wait(
                60
            )


# ============================================================
# WEBSOCKET MESSAGE HANDLER
# ============================================================

def handle_ws_message(
    message
):

    if not isinstance(
        message,
        dict
    ):
        return

    print(
        "GOLD WS MESSAGE:",
        message
    )

    event = message.get(
        "event"
    )

    # --------------------------------------------------------
    # PRICE
    # --------------------------------------------------------

    if event == "price":

        symbol = message.get(
            "symbol"
        )

        price = number(
            message.get(
                "price"
            )
        )

        if (
            symbol == GOLD_SYMBOL
            and price is not None
        ):

            update_gold_price(
                price,
                source="WEBSOCKET"
            )

        return

    # --------------------------------------------------------
    # ERROR
    # --------------------------------------------------------

    if event == "error":

        print(
            "GOLD WS ERROR EVENT:",
            message
        )

        with lock:

            state[
                "gold"
            ][
                "error"
            ] = str(
                message
            )

        broadcast()

        return

    # --------------------------------------------------------
    # SUBSCRIBE STATUS
    # --------------------------------------------------------

    if event == "subscribe-status":

        print(
            "GOLD SUBSCRIBE STATUS:",
            message
        )

        return

    # --------------------------------------------------------
    # HEARTBEAT
    # --------------------------------------------------------

    if event == "heartbeat":

        return


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    reconnect_delay = 5

    print(
        "GOLD WEBSOCKET WORKER STARTED"
    )

    while not stop_event.is_set():

        ws = None

        try:

            if not API_KEY:

                raise RuntimeError(
                    "TWELVE_DATA_API_KEY is missing"
                )

            print(
                "------------------------------------------------"
            )

            print(
                "Connecting Twelve Data Gold WebSocket..."
            )

            print(
                "WS URL:",
                WS_URL
            )

            # ------------------------------------------------
            # Connect
            # ------------------------------------------------

            ws = websocket.create_connection(

                WS_URL,

                timeout=20,

                enableTrace=False,

                origin=None
            )

            print(
                "GOLD WEBSOCKET HANDSHAKE SUCCESS"
            )

            # ------------------------------------------------
            # Subscribe
            # ------------------------------------------------

            subscribe = {

                "action":
                    "subscribe",

                "params": {

                    "symbols":
                        GOLD_SYMBOL
                }
            }

            ws.send(
                json.dumps(
                    subscribe
                )
            )

            print(
                "TWELVE DATA GOLD "
                "SUBSCRIBE SENT:",
                subscribe
            )

            with lock:

                state[
                    "gold"
                ][
                    "connection"
                ] = "CONNECTED"

                state[
                    "gold"
                ][
                    "price_source"
                ] = "WEBSOCKET"

                state[
                    "gold"
                ][
                    "error"
                ] = None

            broadcast()

            reconnect_delay = 5

            last_heartbeat = (
                time.time()
            )

            # ------------------------------------------------
            # Receive loop
            # ------------------------------------------------

            while not stop_event.is_set():

                # websocket-client timeout is used so that
                # heartbeat can be maintained.
                try:

                    raw = ws.recv()

                except websocket.WebSocketTimeoutException:

                    raw = None

                if raw:

                    try:

                        message = json.loads(
                            raw
                        )

                    except Exception:

                        print(
                            "GOLD WS NON-JSON:",
                            str(raw)[:500]
                        )

                        continue

                    handle_ws_message(
                        message
                    )

                # ------------------------------------------------
                # Heartbeat every ~10 seconds
                # ------------------------------------------------

                if (
                    time.time()
                    - last_heartbeat
                    >= 10
                ):

                    try:

                        heartbeat = {
                            "action":
                                "heartbeat"
                        }

                        ws.send(
                            json.dumps(
                                heartbeat
                            )
                        )

                        last_heartbeat = (
                            time.time()
                        )

                    except Exception as exc:

                        raise RuntimeError(
                            "Heartbeat failed: "
                            + repr(exc)
                        )

        except Exception as exc:

            error_text = (
                f"WEBSOCKET ERROR: "
                f"{repr(exc)}"
            )

            print(
                "================================================"
            )

            print(
                error_text
            )

            print(
                "If this says "
                "'Handshake status 200 OK', "
                "REST fallback will continue "
                "trying to keep Gold live."
            )

            print(
                "================================================"
            )

            with lock:

                state[
                    "gold"
                ][
                    "connection"
                ] = "REST FALLBACK"

                state[
                    "gold"
                ][
                    "error"
                ] = str(exc)

            broadcast()

        finally:

            if ws is not None:

                try:
                    ws.close()

                except Exception:
                    pass

        print(
            f"Gold WebSocket reconnecting "
            f"in {reconnect_delay}s..."
        )

        stop_event.wait(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            60
        )


# ============================================================
# WORKERS
# ============================================================

def start_workers():

    global workers_started

    with lock:

        if workers_started:

            return

        workers_started = True

    print(
        "=============================================="
    )

    print(
        "Starting Trading-AI workers..."
    )

    print(
        "Gold:",
        GOLD_SYMBOL
    )

    print(
        "API KEY:",
        "FOUND"
        if API_KEY
        else "MISSING"
    )

    print(
        "=============================================="
    )

    # WebSocket
    threading.Thread(
        target=websocket_loop,
        daemon=True,
        name="GoldWebSocket"
    ).start()

    # REST fallback price
    threading.Thread(
        target=rest_price_loop,
        daemon=True,
        name="GoldRestPrice"
    ).start()

    # Analysis
    threading.Thread(
        target=analysis_loop,
        daemon=True,
        name="GoldAnalysis"
    ).start()


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    start_workers()

    return render_template_string(
        HTML
    )


# ============================================================
# MARKET API
# ============================================================

@app.route("/api/market")
def api_market():

    start_workers()

    with lock:

        data = json.loads(
            json.dumps(
                state
            )
        )

    return jsonify(
        data
    )


# ============================================================
# SSE STREAM
# ============================================================

@app.route("/stream")
def stream():

    start_workers()

    client_queue = queue.Queue(
        maxsize=20
    )

    with clients_lock:

        clients.append(
            client_queue
        )

    def generate():

        try:

            with lock:

                initial = json.dumps(
                    state,
                    separators=(
                        ",",
                        ":"
                    )
                )

            yield (
                "event: market\n"
                f"data: {initial}\n\n"
            )

            while True:

                try:

                    payload = (
                        client_queue.get(
                            timeout=25
                        )
                    )

                    yield (
                        "event: market\n"
                        f"data: {payload}\n\n"
                    )

                except queue.Empty:

                    yield (
                        ": heartbeat\n\n"
                    )

        except GeneratorExit:

            pass

        finally:

            with clients_lock:

                if client_queue in clients:

                    clients.remove(
                        client_queue
                    )

    return Response(

        generate(),

        mimetype="text/event-stream",

        headers={

            "Cache-Control":
                "no-cache",

            "Connection":
                "keep-alive",

            "X-Accel-Buffering":
                "no"
        }
    )


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    with lock:

        return jsonify({

            "status":
                "ok",

            "service":
                "Trading-AI",

            "gold_connection":
                state[
                    "gold"
                ][
                    "connection"
                ],

            "gold_price":
                state[
                    "gold"
                ][
                    "price"
                ],

            "price_source":
                state[
                    "gold"
                ][
                    "price_source"
                ],

            "time":
                now_text()
        })


# ============================================================
# DASHBOARD
# ============================================================

HTML = r"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>Trading AI</title>

<script src="
https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js
"></script>

<style>

*{
    box-sizing:border-box
}

body{
    margin:0;
    min-height:100vh;
    font-family:Arial,sans-serif;
    color:white;
    background:
        radial-gradient(
            circle at top,
            #183d75,
            #08172f 45%,
            #020817
        );
}

.page{
    width:94%;
    max-width:1280px;
    margin:auto;
    padding:22px 0 45px;
}

.header{
    text-align:center;
    margin-bottom:20px;
}

.header h1{
    margin:0;
    font-size:30px;
}

.header p{
    margin:7px 0;
    color:#7fa9e8;
}

.card{
    padding:20px;
    margin-bottom:18px;
    border-radius:18px;
    border:1px solid rgba(110,160,230,.2);
    background:
        linear-gradient(
            145deg,
            rgba(20,43,82,.96),
            rgba(5,19,42,.97)
        );
    box-shadow:
        0 18px 45px rgba(0,0,0,.32);
}

.asset{
    font-size:20px;
    font-weight:700;
}

.price{
    margin-top:8px;
    font-size:38px;
    font-weight:700;
}

.live{
    margin-top:4px;
    color:#2be48e;
    font-size:13px;
    font-weight:700;
}

.connection{
    margin-top:4px;
    color:#719bd4;
    font-size:11px;
}

.metrics{
    display:grid;
    grid-template-columns:
        repeat(6,1fr);
    gap:8px;
    margin-top:18px;
}

.metric{
    padding:12px;
    min-height:68px;
    border-radius:11px;
    background:
        rgba(11,35,70,.72);
    border:
        1px solid rgba(100,150,220,.13);
}

.label{
    color:#78a5e5;
    font-size:10px;
    margin-bottom:6px;
}

.value{
    font-size:15px;
    font-weight:700;
}

.analysis{
    margin-top:14px;
    padding:17px;
    border-radius:13px;
    background:
        linear-gradient(
            100deg,
            #12529f,
            #0b3977
        );
}

.analysis-label{
    color:#82b0f5;
    font-size:10px;
    font-weight:700;
}

.signal{
    margin-top:4px;
    font-size:27px;
    font-weight:800;
}

.confidence,
.score{
    margin-top:5px;
    font-size:14px;
    font-weight:700;
}

.toolbar{
    display:flex;
    gap:7px;
    flex-wrap:wrap;
    margin:15px 0 10px;
}

.tf{
    border:
        1px solid #38679e;
    background:#092447;
    color:#bcd8ff;
    padding:8px 12px;
    border-radius:8px;
    cursor:pointer;
    font-weight:700;
}

.tf.active{
    background:#1a64ad;
    color:white;
}

.chart{
    height:430px;
    border-radius:12px;
    overflow:hidden;
    border:
        1px solid rgba(100,150,220,.16);
}

.grid2{
    display:grid;
    grid-template-columns:
        1.3fr .7fr;
    gap:14px;
    margin-top:14px;
}

.panel{
    padding:16px;
    border-radius:13px;
    background:
        rgba(4,17,37,.55);
    border:
        1px solid rgba(100,150,220,.13);
}

.panel h3{
    margin:0 0 12px;
    font-size:14px;
    color:#9fc5f5;
}

.tradegrid{
    display:grid;
    grid-template-columns:
        repeat(4,1fr);
    gap:8px;
}

.tradebox{
    padding:11px;
    border-radius:9px;
    background:#081d39;
}

.tradebox b{
    display:block;
    font-size:14px;
    margin-top:4px;
}

.history{
    max-height:250px;
    overflow:auto;
}

.row{
    display:grid;
    grid-template-columns:
        75px 80px 55px 70px 1fr;
    gap:7px;
    padding:8px 0;
    border-bottom:
        1px solid rgba(130,170,220,.1);
    font-size:11px;
}

.reason{
    color:#8eafd8;
}

.error{
    margin-top:12px;
    color:#ffb3b3;
    font-size:12px;
}

.footer{
    text-align:center;
    color:#6489ba;
    font-size:11px;
    margin-top:15px;
}

@media(max-width:900px){

    .metrics{
        grid-template-columns:
            repeat(3,1fr);
    }

    .grid2{
        grid-template-columns:1fr;
    }

    .tradegrid{
        grid-template-columns:
            repeat(2,1fr);
    }
}

@media(max-width:480px){

    .page{
        width:96%;
    }

    .price{
        font-size:31px;
    }

    .metrics{
        grid-template-columns:
            repeat(2,1fr);
    }

    .chart{
        height:330px;
    }
}

</style>

</head>

<body>

<div class="page">

<div class="header">

<h1>Trading AI</h1>

<p>
Real-Time Market Intelligence
•
Explainable Multi-Timeframe Analysis
</p>

</div>

<div id="gold"></div>

<div id="oil"></div>

<div class="footer">
LIVE MARKET DATA • TRADING-AI
</div>

</div>


<script>

let activeTF = "5min";

let chart = null;

let resizeObserver = null;

let latestData = null;


function fmt(
    value,
    digits=3
){

    if(
        value === null ||
        value === undefined
    ){
        return "—";
    }

    const n = Number(value);

    if(
        Number.isNaN(n)
    ){
        return "—";
    }

    return n.toFixed(
        digits
    );
}


function tfLabel(x){

    return {

        "1min":"1m",
        "5min":"5m",
        "15min":"15m",
        "30min":"30m",
        "1h":"1H",
        "4h":"4H",
        "1day":"1D"

    }[x] || x;
}


function metric(
    a,
    b
){

    return `
    <div class="metric">
        <div class="label">${a}</div>
        <div class="value">${b}</div>
    </div>
    `;
}


function box(
    a,
    b
){

    return `
    <div class="tradebox">
        <span class="label">${a}</span>
        <b>${b}</b>
    </div>
    `;
}


function draw(
    candles
){

    const el =
        document.getElementById(
            "price-chart"
        );

    if(!el){
        return;
    }

    if(chart){
        chart.remove();
        chart = null;
    }

    chart =
        LightweightCharts.createChart(
            el,
            {
                layout:{
                    background:{
                        type:"solid",
                        color:"#06162d"
                    },
                    textColor:"#9fc5f5"
                },

                grid:{
                    vertLines:{
                        color:
                            "rgba(100,150,220,.08)"
                    },

                    horzLines:{
                        color:
                            "rgba(100,150,220,.08)"
                    }
                },

                timeScale:{
                    timeVisible:true,
                    secondsVisible:false
                }
            }
        );

    const series =
        chart.addCandlestickSeries({

            upColor:"#20c997",

            downColor:"#ff5c73",

            borderVisible:false,

            wickUpColor:"#20c997",

            wickDownColor:"#ff5c73"

        });

    const data =
        (candles || [])
        .map(x => {

            const ts =
                Math.floor(
                    new Date(
                        x.datetime ||
                        x.date
                    ).getTime()
                    / 1000
                );

            return {

                time:ts,

                open:Number(x.open),

                high:Number(x.high),

                low:Number(x.low),

                close:Number(x.close)

            };

        })
        .filter(x =>

            Number.isFinite(
                x.time
            )

            &&

            [
                x.open,
                x.high,
                x.low,
                x.close
            ].every(
                Number.isFinite
            )

        );

    series.setData(
        data
    );

    chart.timeScale()
        .fitContent();

    if(
        resizeObserver
    ){

        resizeObserver.disconnect();

    }

    resizeObserver =
        new ResizeObserver(
            () => {

                chart.applyOptions({
                    width:
                        el.clientWidth
                });

            }
        );

    resizeObserver.observe(
        el
    );
}


function hist(h){

    if(
        !h ||
        !h.length
    ){

        return `
        <div class="reason">
            No signal changes recorded yet.
        </div>
        `;

    }

    return h
        .slice()
        .reverse()
        .map(
            x => `

            <div class="row">

                <span>
                    ${x.time}
                </span>

                <span>
                    ${fmt(x.price)}
                </span>

                <b>
                    ${
                        x.score > 0
                        ? "+"
                        : ""
                    }${x.score}
                </b>

                <span>
                    ${x.signal}
                </span>

                <span class="reason">
                    ${
                        (
                            x.reasons ||
                            []
                        )
                        .slice(0,3)
                        .join(" • ")
                    }
                </span>

            </div>

            `
        )
        .join("");

}


function render(
    data
){

    latestData =
        data;

    const g =
        data.gold;

    const p =
        g.trade_plan ||
        {};

    document.getElementById(
        "gold"
    ).innerHTML = `

    <div class="card">

        <div class="asset">
            🥇 Gold — XAU/USD
        </div>

        <div class="price">
            ${fmt(g.price)}
        </div>

        <div class="live">
            ● ${
                g.price_source ===
                "WEBSOCKET"
                ? "LIVE"
                : "LIVE / REST"
            }
        </div>

        <div class="connection">

            Connection:
            ${g.connection || "STARTING"}

            •

            Source:
            ${g.price_source || "NONE"}

            •

            Updated:
            ${g.updated || "—"}

        </div>

        <div class="metrics">

            ${metric(
                "TREND",
                g.trend
            )}

            ${metric(
                "MOMENTUM",
                g.momentum
            )}

            ${metric(
                "STRUCTURE",
                g.structure
            )}

            ${metric(
                "RSI",
                fmt(g.rsi,2)
            )}

            ${metric(
                "EMA 20",
                fmt(g.ema20)
            )}

            ${metric(
                "EMA 50",
                fmt(g.ema50)
            )}

            ${metric(
                "SUPPORT",
                fmt(g.support)
            )}

            ${metric(
                "RESISTANCE",
                fmt(g.resistance)
            )}

            ${metric(
                "LIQUIDITY HIGH",
                fmt(g.liquidity_high)
            )}

            ${metric(
                "LIQUIDITY LOW",
                fmt(g.liquidity_low)
            )}

            ${metric(
                "SWEEP",
                g.sweep || "NONE"
            )}

            ${metric(
                "TIMEFRAME",
                tfLabel(activeTF)
            )}

        </div>


        <div class="analysis">

            <div class="analysis-label">
                TRADING-AI CONCLUSION
            </div>

            <div class="signal">
                ${g.signal || "WAIT"}
            </div>

            <div class="confidence">
                Confirmation Strength:
                ${g.confidence || 0}%
            </div>

            <div class="score">
                AI SCORE:
                ${g.score ?? 0}
            </div>

        </div>


        <div class="toolbar">

            ${
                [
                    "5min",
                    "15min",
                    "30min",
                    "1h",
                    "4h",
                    "1day"
                ]
                .map(
                    x => `

                    <button
                        class="tf ${
                            activeTF === x
                            ? "active"
                            : ""
                        }"
                        onclick="
                            selectTF('${x}')
                        "
                    >
                        ${tfLabel(x)}
                    </button>

                    `
                )
                .join("")
            }

        </div>


        <div
            id="price-chart"
            class="chart"
        ></div>


        <div class="grid2">

            <div class="panel">

                <h3>
                    🎯 AI TRADE SETUP
                </h3>

                <div class="tradegrid">

                    ${box(
                        "ENTRY",
                        fmt(p.entry)
                    )}

                    ${box(
                        "STOP LOSS",
                        fmt(p.stop_loss)
                    )}

                    ${box(
                        "TARGET 1",
                        fmt(p.target_1)
                    )}

                    ${box(
                        "TARGET 2",
                        fmt(p.target_2)
                    )}

                    ${box(
                        "TARGET 3",
                        fmt(p.target_3)
                    )}

                    ${box(
                        "R:R",
                        p.risk_reward || "—"
                    )}

                    ${box(
                        "INVALIDATION",
                        fmt(p.invalidation)
                    )}

                    ${box(
                        "SETUP",
                        p.signal || "WAIT"
                    )}

                </div>

                <div
                    class="reason"
                    style="margin-top:12px"
                >

                    <b>Why:</b>

                    ${
                        (
                            p.reason ||
                            []
                        )
                        .join(" • ")
                        ||
                        "Waiting for confirmation."
                    }

                </div>

            </div>


            <div class="panel">

                <h3>
                    🕐 AI SIGNAL HISTORY
                </h3>

                <div class="history">

                    ${
                        hist(
                            g.signal_history
                        )
                    }

                </div>

            </div>

        </div>


        ${
            g.error
            ?
            `
            <div class="error">
                ${g.error}
            </div>
            `
            :
            ""
        }

    </div>

    `;


    document.getElementById(
        "oil"
    ).innerHTML = `

    <div class="card">

        <div class="asset">
            🛢️ Crude Oil — WTI
        </div>

        <div class="price">
            —
        </div>

        <div class="live">
            ● PLAN LIMIT
        </div>

        <div class="connection">
            Connection:
            ${
                data.oil.connection ||
                "UNAVAILABLE"
            }
        </div>

        <div class="error">
            ${
                data.oil.error ||
                ""
            }
        </div>

    </div>

    `;


    draw(
        (
            g.candles ||
            {}
        )[activeTF] ||
        []
    );
}


function selectTF(
    x
){

    activeTF =
        x;

    if(
        latestData
    ){

        render(
            latestData
        );

    }
}


// ------------------------------------------------------------
// INITIAL API LOAD
// ------------------------------------------------------------

fetch(
    "/api/market",
    {
        cache:"no-store"
    }
)
.then(
    response =>
        response.json()
)
.then(
    render
)
.catch(
    console.error
);


// ------------------------------------------------------------
// LIVE SERVER-SENT EVENTS
// ------------------------------------------------------------

const source =
    new EventSource(
        "/stream"
    );


source.addEventListener(
    "market",
    event => {

        try{

            render(
                JSON.parse(
                    event.data
                )
            );

        }catch(err){

            console.error(
                err
            );

        }

    }
);


source.onerror =
    function(){

        console.warn(
            "Market stream reconnecting..."
        );

    };


</script>

</body>

</html>
"""


# ============================================================
# START APP
# ============================================================

if __name__ == "__main__":

    start_workers()

    print(
        "Trading-AI Flask server starting..."
    )

    print(
        f"PORT={PORT}"
    )

    app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True
    )
