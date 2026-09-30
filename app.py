import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket

from flask import Flask, jsonify, Response

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"
OIL_SYMBOL = "WTI/USD"

OIL_ENABLED = False

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

# Keep enough history locally for indicators
MAX_CANDLES = 1000

# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.Lock()

gold_price = None
gold_timestamp = None
gold_ws_connected = False
gold_ws_last_message = None

data_status = "STARTING"
bootstrap_status = "WAITING"

last_error = ""

# Local candle stores
candles_1m = deque(maxlen=MAX_CANDLES)
candles_5m = deque(maxlen=MAX_CANDLES)
candles_15m = deque(maxlen=MAX_CANDLES)
candles_30m = deque(maxlen=MAX_CANDLES)
candles_1h = deque(maxlen=MAX_CANDLES)

# AI state
ai_state = {
    "conclusion": "WAIT",
    "confidence": 50,
    "score": 0,
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
    "engine": "LOCAL",
    "engine_status": "WAITING",
    "reasons": ["Waiting for market candle data."],
    "warnings": [],
    "setup": "WAIT",
    "entry": None,
    "stop_loss": None,
    "target1": None,
    "target2": None,
    "target3": None,
    "rr": None,
    "invalidation": None,
    "updated": None,
}

signal_history = deque(maxlen=20)

workers_started = False


# ============================================================
# BASIC HELPERS
# ============================================================

def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def now_utc():
    return datetime.now(timezone.utc)


def timestamp_to_iso(ts):
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()
    except Exception:
        return now_utc().isoformat()


def floor_timestamp(ts, seconds):
    return int(float(ts) // seconds) * seconds


def normalize_candle(item):
    """
    Convert a Twelve Data candle into:
    {
        time, open, high, low, close, volume
    }
    """

    if not isinstance(item, dict):
        return None

    ts = item.get("timestamp")

    if ts is None:
        dt_value = item.get("datetime")

        if dt_value:
            try:
                text = str(dt_value).replace("Z", "+00:00")
                dt = datetime.fromisoformat(text)

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

                ts = int(dt.timestamp())
            except Exception:
                return None

    try:
        ts = int(float(ts))
    except Exception:
        return None

    o = safe_float(item.get("open"))
    h = safe_float(item.get("high"))
    l = safe_float(item.get("low"))
    c = safe_float(item.get("close"))
    v = safe_float(item.get("volume")) or 0.0

    if None in (o, h, l, c):
        return None

    return {
        "time": ts,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
    }


# ============================================================
# LOCAL CANDLE ENGINE
# ============================================================

def update_1m_candle(price, timestamp):
    """
    Build 1-minute candles directly from WebSocket ticks.
    No HTTP request required.
    """

    minute_start = floor_timestamp(timestamp, 60)

    with state_lock:

        if not candles_1m:
            candles_1m.append({
                "time": minute_start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
            })
            return

        current = candles_1m[-1]

        if current["time"] == minute_start:
            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price
            return

        if minute_start > current["time"]:
            candles_1m.append({
                "time": minute_start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
            })


def aggregate_from_source(source, minutes):
    """
    Aggregate completed source candles into a higher timeframe.
    """

    if not source:
        return []

    bucket_seconds = minutes * 60

    result = []

    current_bucket = None
    current = None

    for candle in source:

        ts = int(candle["time"])
        bucket = floor_timestamp(ts, bucket_seconds)

        if current_bucket != bucket:

            if current is not None:
                result.append(current)

            current_bucket = bucket

            current = {
                "time": bucket,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle.get("volume", 0.0),
            }

        else:

            current["high"] = max(
                current["high"],
                candle["high"]
            )

            current["low"] = min(
                current["low"],
                candle["low"]
            )

            current["close"] = candle["close"]

            current["volume"] += candle.get("volume", 0.0)

    if current is not None:
        result.append(current)

    return result


def rebuild_local_timeframes():
    """
    Rebuild 5m/15m/30m/1h from local 1m candles.
    """

    with state_lock:

        source = list(candles_1m)

        if not source:
            return

        five = aggregate_from_source(source, 5)
        fifteen = aggregate_from_source(source, 15)
        thirty = aggregate_from_source(source, 30)
        sixty = aggregate_from_source(source, 60)

        candles_5m.clear()
        candles_5m.extend(five[-MAX_CANDLES:])

        candles_15m.clear()
        candles_15m.extend(fifteen[-MAX_CANDLES:])

        candles_30m.clear()
        candles_30m.extend(thirty[-MAX_CANDLES:])

        candles_1h.clear()
        candles_1h.extend(sixty[-MAX_CANDLES:])


# ============================================================
# HISTORICAL BOOTSTRAP
# ============================================================

def fetch_historical_candles(interval, outputsize=100):
    """
    Limited HTTP bootstrap.

    This is NOT used continuously.
    """

    global last_error

    if not API_KEY:
        last_error = "TWELVE_DATA_API_KEY is missing."
        return []

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": GOLD_SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "format": "JSON",
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=15,
        )

        if response.status_code == 429:

            last_error = (
                f"Twelve Data rate limit for {interval}."
            )

            print(
                f"CANDLE RATE LIMIT 429: {interval}"
            )

            return []

        if response.status_code != 200:

            last_error = (
                f"Twelve Data HTTP {response.status_code} "
                f"for {interval}."
            )

            print(
                f"CANDLE HTTP ERROR: "
                f"{interval} {response.status_code}"
            )

            return []

        data = response.json()

        if data.get("status") == "error":

            last_error = str(
                data.get("message", "Twelve Data error")
            )

            print(
                f"CANDLE API ERROR: {interval} "
                f"{last_error}"
            )

            return []

        values = data.get("values", [])

        result = []

        for item in reversed(values):

            candle = normalize_candle(item)

            if candle:
                result.append(candle)

        return result

    except Exception as exc:

        last_error = str(exc)

        print(
            f"CANDLE FETCH ERROR: {interval}: {exc}"
        )

        return []


def merge_historical_into_local(candles):
    """
    Add historical 5m candles into local storage.

    These candles become the seed for the AI.
    """

    if not candles:
        return

    with state_lock:

        existing = {
            int(c["time"]): c
            for c in candles_5m
        }

        for candle in candles:
            existing[int(candle["time"])] = candle

        ordered = sorted(
            existing.values(),
            key=lambda x: x["time"]
        )

        candles_5m.clear()
        candles_5m.extend(
            ordered[-MAX_CANDLES:]
        )


def merge_1h_historical(candles):
    if not candles:
        return

    with state_lock:

        existing = {
            int(c["time"]): c
            for c in candles_1h
        }

        for candle in candles:
            existing[int(candle["time"])] = candle

        ordered = sorted(
            existing.values(),
            key=lambda x: x["time"]
        )

        candles_1h.clear()
        candles_1h.extend(
            ordered[-MAX_CANDLES:]
        )


def bootstrap_worker():
    """
    Only a small number of historical requests.

    If Twelve Data is rate limited, the application
    continues running from WebSocket data.
    """

    global bootstrap_status
    global data_status

    print("Starting limited historical bootstrap...")

    bootstrap_status = "LOADING"

    # 5m seed
    print("BOOTSTRAP: requesting 5min history...")

    five = fetch_historical_candles(
        "5min",
        100,
    )

    if five:

        merge_historical_into_local(five)

        print(
            f"BOOTSTRAP: loaded {len(five)} 5m candles"
        )

    else:

        print(
            "BOOTSTRAP: 5m unavailable; "
            "continuing with WebSocket."
        )

    # Do not immediately hammer the API
    time.sleep(8)

    # 1h seed
    print("BOOTSTRAP: requesting 1h history...")

    one_hour = fetch_historical_candles(
        "1h",
        100,
    )

    if one_hour:

        merge_1h_historical(one_hour)

        print(
            f"BOOTSTRAP: loaded "
            f"{len(one_hour)} 1h candles"
        )

    else:

        print(
            "BOOTSTRAP: 1h unavailable; "
            "local engine will continue."
        )

    with state_lock:

        if candles_5m:
            data_status = "READY"
        elif candles_1m:
            data_status = "BUILDING"
        else:
            data_status = "STARTING"

    bootstrap_status = "DONE"

    print(
        "Historical bootstrap finished. "
        "Live WebSocket remains primary."
    )


# ============================================================
# WEBSOCKET
# ============================================================

def gold_ws_message(ws, message):

    global gold_price
    global gold_timestamp
    global gold_ws_last_message
    global data_status

    try:

        data = json.loads(message)

    except Exception:

        return

    gold_ws_last_message = data

    event = data.get("event")

    if event == "subscribe-status":

        print(
            "GOLD SUBSCRIBE STATUS:",
            data,
        )

        return

    if event != "price":

        return

    symbol = data.get("symbol")

    if symbol != GOLD_SYMBOL:

        return

    price = safe_float(data.get("price"))

    if price is None:

        return

    timestamp = safe_float(
        data.get("timestamp")
    )

    if timestamp is None:
        timestamp = time.time()

    gold_price = price
    gold_timestamp = timestamp

    print(
        f"STATE UPDATED: GOLD = {price}"
    )

    update_1m_candle(
        price,
        timestamp,
    )

    rebuild_local_timeframes()

    with state_lock:

        if len(candles_5m) >= 20:
            data_status = "READY"
        elif len(candles_1m) > 0:
            data_status = "BUILDING"
        else:
            data_status = "STARTING"


def gold_ws_open(ws):

    global gold_ws_connected

    gold_ws_connected = True

    print(
        "Connecting Twelve Data Gold WebSocket..."
    )

    payload = {
        "action": "subscribe",
        "params": {
            "symbols": GOLD_SYMBOL,
        },
    }

    ws.send(json.dumps(payload))

    print(
        "TWELVE DATA GOLD SUBSCRIBE SENT"
    )


def gold_ws_error(ws, error):

    global gold_ws_connected

    gold_ws_connected = False

    print(
        "GOLD WEBSOCKET ERROR:",
        error,
    )


def gold_ws_close(ws, close_status_code, close_msg):

    global gold_ws_connected

    gold_ws_connected = False

    print(
        "GOLD WEBSOCKET CLOSED:",
        close_status_code,
        close_msg,
    )


def gold_websocket_loop():

    global gold_ws_connected

    while True:

        if not API_KEY:

            print(
                "TWELVE_DATA_API_KEY missing."
            )

            time.sleep(15)

            continue

        try:

            print(
                "Connecting Twelve Data Gold WebSocket..."
            )

            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=gold_ws_open,
                on_message=gold_ws_message,
                on_error=gold_ws_error,
                on_close=gold_ws_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:

            gold_ws_connected = False

            print(
                "GOLD WS LOOP ERROR:",
                exc,
            )

        print(
            "Gold WebSocket reconnecting in 5 seconds..."
        )

        time.sleep(5)


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def closes(candles):
    return [
        float(c["close"])
        for c in candles
    ]


def ema(values, period):

    if not values:
        return None

    if len(values) < period:
        return None

    multiplier = 2.0 / (period + 1.0)

    value = sum(
        values[:period]
    ) / period

    for price in values[period:]:

        value = (
            (price - value) * multiplier
        ) + value

    return value


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = values[i] - values[i - 1]

        if change >= 0:

            gains.append(change)
            losses.append(0.0)

        else:

            gains.append(0.0)
            losses.append(abs(change))

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


def atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):

        current = candles[i]
        previous = candles[i - 1]

        high = current["high"]
        low = current["low"]
        prev_close = previous["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    value = sum(
        trs[:period]
    ) / period

    for tr in trs[period:]:

        value = (
            (value * (period - 1))
            + tr
        ) / period

    return value


# ============================================================
# MARKET STRUCTURE
# ============================================================

def calculate_structure(candle_data):

    if len(candle_data) < 6:

        return "WAITING"

    recent = candle_data[-5:]
    previous = candle_data[-10:-5]

    if len(previous) < 5:
        return "WAITING"

    recent_high = max(
        c["high"] for c in recent
    )

    recent_low = min(
        c["low"] for c in recent
    )

    previous_high = max(
        c["high"] for c in previous
    )

    previous_low = min(
        c["low"] for c in previous
    )

    if (
        recent_high > previous_high
        and recent_low > previous_low
    ):

        return "HIGHER HIGH / HIGHER LOW"

    if (
        recent_high < previous_high
        and recent_low < previous_low
    ):

        return "LOWER HIGH / LOWER LOW"

    return "RANGE"


def detect_sweep(candle_data):

    if len(candle_data) < 10:

        return "NONE"

    previous = candle_data[-2]
    current = candle_data[-1]

    recent = candle_data[-11:-2]

    liquidity_high = max(
        c["high"] for c in recent
    )

    liquidity_low = min(
        c["low"] for c in recent
    )

    # Buy-side liquidity sweep
    if (
        previous["high"] >= liquidity_high
        and current["close"] < liquidity_high
    ):

        return "BUY-SIDE SWEEP"

    # Sell-side liquidity sweep
    if (
        previous["low"] <= liquidity_low
        and current["close"] > liquidity_low
    ):

        return "SELL-SIDE SWEEP"

    return "NONE"


def support_resistance(candle_data):

    if len(candle_data) < 10:

        return None, None

    recent = candle_data[-30:]

    support = min(
        c["low"] for c in recent
    )

    resistance = max(
        c["high"] for c in recent
    )

    return support, resistance


def liquidity_levels(candle_data):

    if len(candle_data) < 10:

        return None, None

    sample = candle_data[-20:]

    high = max(
        c["high"] for c in sample
    )

    low = min(
        c["low"] for c in sample
    )

    return high, low


# ============================================================
# AI ANALYSIS
# ============================================================

def analyze_timeframe(candle_data):

    if len(candle_data) < 50:

        return {
            "ready": False,
            "reason": (
                f"Waiting for candles "
                f"({len(candle_data)}/50)."
            ),
        }

    values = closes(candle_data)

    price = values[-1]

    ema20 = ema(values, 20)
    ema50 = ema(values, 50)
    ema9 = ema(values, 9)

    rsi_value = rsi(values, 14)
    atr_value = atr(candle_data, 14)

    structure = calculate_structure(
        candle_data
    )

    sweep = detect_sweep(
        candle_data
    )

    support, resistance = (
        support_resistance(candle_data)
    )

    liquidity_high, liquidity_low = (
        liquidity_levels(candle_data)
    )

    score = 0
    reasons = []

    # ---------------------------
    # TREND
    # ---------------------------

    if (
        ema20 is not None
        and ema50 is not None
    ):

        if price > ema20 > ema50:

            trend = "BULLISH"
            score += 2

            reasons.append(
                "Price is above EMA20 and EMA50."
            )

        elif price < ema20 < ema50:

            trend = "BEARISH"
            score -= 2

            reasons.append(
                "Price is below EMA20 and EMA50."
            )

        else:

            trend = "MIXED"

            reasons.append(
                "EMA structure is mixed."
            )

    else:

        trend = "WAITING"

    # ---------------------------
    # MOMENTUM
    # ---------------------------

    if (
        ema9 is not None
        and ema20 is not None
        and rsi_value is not None
    ):

        if (
            ema9 > ema20
            and rsi_value >= 52
        ):

            momentum = "BUYING"
            score += 2

            reasons.append(
                "Short-term momentum is positive."
            )

        elif (
            ema9 < ema20
            and rsi_value <= 48
        ):

            momentum = "SELLING"
            score -= 2

            reasons.append(
                "Short-term momentum is negative."
            )

        else:

            momentum = "NEUTRAL"

    else:

        momentum = "WAITING"

    # ---------------------------
    # RSI EXTREMES
    # ---------------------------

    if rsi_value is not None:

        if rsi_value >= 70:

            reasons.append(
                "RSI is elevated; chasing longs has higher risk."
            )

        elif rsi_value <= 30:

            reasons.append(
                "RSI is depressed; chasing shorts has higher risk."
            )

    # ---------------------------
    # STRUCTURE
    # ---------------------------

    if structure == "HIGHER HIGH / HIGHER LOW":

        score += 2

        reasons.append(
            "Market structure is making higher highs and higher lows."
        )

    elif structure == "LOWER HIGH / LOWER LOW":

        score -= 2

        reasons.append(
            "Market structure is making lower highs and lower lows."
        )

    # ---------------------------
    # LIQUIDITY SWEEP
    # ---------------------------

    if sweep == "SELL-SIDE SWEEP":

        score += 1

        reasons.append(
            "Sell-side liquidity sweep detected."
        )

    elif sweep == "BUY-SIDE SWEEP":

        score -= 1

        reasons.append(
            "Buy-side liquidity sweep detected."
        )

    # ---------------------------
    # FINAL DECISION
    # ---------------------------

    if score >= 5:

        conclusion = "BUY"

    elif score <= -5:

        conclusion = "SELL"

    else:

        conclusion = "WAIT"

    # ---------------------------
    # CONFIDENCE
    # ---------------------------

    raw_confidence = 50 + (
        min(abs(score), 8) * 5
    )

    if conclusion == "WAIT":

        confidence = min(
            raw_confidence,
            65,
        )

    else:

        confidence = min(
            raw_confidence,
            90,
        )

    return {
        "ready": True,
        "price": price,
        "ema20": ema20,
        "ema50": ema50,
        "rsi": rsi_value,
        "atr": atr_value,
        "trend": trend,
        "momentum": momentum,
        "structure": structure,
        "sweep": sweep,
        "support": support,
        "resistance": resistance,
        "liquidity_high": liquidity_high,
        "liquidity_low": liquidity_low,
        "score": score,
        "confidence": confidence,
        "conclusion": conclusion,
        "reasons": reasons,
    }


# ============================================================
# MULTI-TIMEFRAME AI
# ============================================================

def build_ai_analysis():

    global ai_state

    with state_lock:

        five = list(candles_5m)
        fifteen = list(candles_15m)
        thirty = list(candles_30m)
        one_hour = list(candles_1h)

    # If historical 5m exists but local 1m
    # history is not enough, use 5m as source
    # to create higher timeframes.
    if len(five) >= 10:

        local_15 = aggregate_from_source(
            five,
            15,
        )

        local_30 = aggregate_from_source(
            five,
            30,
        )

        if len(local_15) > len(fifteen):

            fifteen = local_15

        if len(local_30) > len(thirty):

            thirty = local_30

    primary = analyze_timeframe(
        five
    )

    if not primary.get("ready"):

        ai_state.update({
            "conclusion": "WAIT",
            "confidence": 50,
            "score": 0,
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
            "engine": "LOCAL",
            "engine_status": "WAITING",
            "reasons": [
                primary.get(
                    "reason",
                    "Waiting for market candle data.",
                )
            ],
            "warnings": [
                "AI requires at least 50 completed 5m candles."
            ],
            "setup": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,
            "invalidation": None,
            "updated": now_utc().isoformat(),
        })

        return

    higher15 = analyze_timeframe(
        fifteen
    )

    higher30 = analyze_timeframe(
        thirty
    )

    higher1h = analyze_timeframe(
        one_hour
    )

    final_score = primary["score"]

    warnings = []

    # ---------------------------------------
    # 15m CONFIRMATION
    # ---------------------------------------

    if higher15.get("ready"):

        if (
            primary["conclusion"] == "BUY"
            and higher15["conclusion"] == "BUY"
        ):

            final_score += 2

        elif (
            primary["conclusion"] == "SELL"
            and higher15["conclusion"] == "SELL"
        ):

            final_score -= 2

        elif (
            primary["conclusion"] != "WAIT"
            and higher15["conclusion"] != "WAIT"
            and primary["conclusion"]
            != higher15["conclusion"]
        ):

            warnings.append(
                "5m and 15m direction disagree."
            )

    # ---------------------------------------
    # 1H CONFLICT FILTER
    # ---------------------------------------

    if higher1h.get("ready"):

        if (
            primary["conclusion"] == "BUY"
            and higher1h["conclusion"] == "SELL"
        ):

            warnings.append(
                "5m BUY conflicts with 1H SELL."
            )

            final_score -= 2

        elif (
            primary["conclusion"] == "SELL"
            and higher1h["conclusion"] == "BUY"
        ):

            warnings.append(
                "5m SELL conflicts with 1H BUY."
            )

            final_score += 2

    # ---------------------------------------
    # FINAL DECISION
    # ---------------------------------------

    if final_score >= 7:

        conclusion = "BUY"

    elif final_score <= -7:

        conclusion = "SELL"

    else:

        conclusion = "WAIT"

    confidence = 50 + min(
        abs(final_score) * 4,
        40,
    )

    if warnings:

        confidence = min(
            confidence,
            70,
        )

    reasons = list(
        primary.get("reasons", [])
    )

    if higher15.get("ready"):

        reasons.append(
            "15m confirmation is included."
        )

    else:

        warnings.append(
            "15m confirmation is still warming up."
        )

    if higher1h.get("ready"):

        reasons.append(
            "1H context is included."
        )

    else:

        warnings.append(
            "1H confirmation is still warming up."
        )

    ai_state.update({
        "conclusion": conclusion,
        "confidence": int(confidence),
        "score": int(final_score),
        "trend": primary["trend"],
        "momentum": primary["momentum"],
        "structure": primary["structure"],
        "rsi": primary["rsi"],
        "ema20": primary["ema20"],
        "ema50": primary["ema50"],
        "support": primary["support"],
        "resistance": primary["resistance"],
        "liquidity_high": primary["liquidity_high"],
        "liquidity_low": primary["liquidity_low"],
        "sweep": primary["sweep"],
        "engine": "LOCAL MULTI-TIMEFRAME",
        "engine_status": "ACTIVE",
        "reasons": reasons,
        "warnings": warnings,
        "updated": now_utc().isoformat(),
    })


# ============================================================
# TRADE SETUP
# ============================================================

def build_trade_setup():

    with state_lock:

        result = dict(ai_state)

        five = list(candles_5m)

    if not five:

        result.update({
            "setup": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,
            "invalidation": None,
        })

        return result

    price = safe_float(gold_price)

    if price is None:

        price = five[-1]["close"]

    atr_value = atr(
        five,
        14,
    )

    if atr_value is None:

        return result

    atr_value = max(
        atr_value,
        price * 0.0005,
    )

    conclusion = result["conclusion"]

    support = result.get("support")
    resistance = result.get("resistance")

    if conclusion == "BUY":

        entry = price

        stop = min(
            price - atr_value * 1.2,
            support if support else price - atr_value * 1.2,
        )

        risk = entry - stop

        if risk <= 0:
            risk = atr_value

        target1 = entry + risk * 1.5
        target2 = entry + risk * 2.2
        target3 = entry + risk * 3.0

        result.update({
            "setup": "BUY",
            "entry": entry,
            "stop_loss": stop,
            "target1": target1,
            "target2": target2,
            "target3": target3,
            "rr": 1.5,
            "invalidation": stop,
        })

    elif conclusion == "SELL":

        entry = price

        stop = max(
            price + atr_value * 1.2,
            resistance if resistance else price + atr_value * 1.2,
        )

        risk = stop - entry

        if risk <= 0:
            risk = atr_value

        target1 = entry - risk * 1.5
        target2 = entry - risk * 2.2
        target3 = entry - risk * 3.0

        result.update({
            "setup": "SELL",
            "entry": entry,
            "stop_loss": stop,
            "target1": target1,
            "target2": target2,
            "target3": target3,
            "rr": 1.5,
            "invalidation": stop,
        })

    else:

        result.update({
            "setup": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,
            "invalidation": None,
        })

    return result


# ============================================================
# SIGNAL HISTORY
# ============================================================

last_recorded_signal = None


def update_signal_history():

    global last_recorded_signal

    conclusion = ai_state.get(
        "conclusion",
        "WAIT",
    )

    score = ai_state.get(
        "score",
        0,
    )

    key = (
        conclusion,
        score,
    )

    if key == last_recorded_signal:
        return

    last_recorded_signal = key

    signal_history.appendleft({
        "time": now_utc().isoformat(),
        "signal": conclusion,
        "score": score,
        "confidence": ai_state.get(
            "confidence",
            50,
        ),
    })


# ============================================================
# AI WORKER
# ============================================================

def ai_worker():

    global data_status

    print(
        "Starting Trading-AI local analysis engine..."
    )

    while True:

        try:

            build_ai_analysis()

            setup = build_trade_setup()

            ai_state.update({
                "setup": setup.get(
                    "setup",
                    "WAIT",
                ),
                "entry": setup.get(
                    "entry"
                ),
                "stop_loss": setup.get(
                    "stop_loss"
                ),
                "target1": setup.get(
                    "target1"
                ),
                "target2": setup.get(
                    "target2"
                ),
                "target3": setup.get(
                    "target3"
                ),
                "rr": setup.get(
                    "rr"
                ),
                "invalidation": setup.get(
                    "invalidation"
                ),
            })

            update_signal_history()

            with state_lock:

                if len(candles_5m) >= 50:

                    data_status = "READY"

                elif len(candles_1m) > 0:

                    data_status = "BUILDING"

                else:

                    data_status = "STARTING"

        except Exception as exc:

            print(
                "AI WORKER ERROR:",
                exc,
            )

        time.sleep(3)


# ============================================================
# API PAYLOAD
# ============================================================

def public_number(value, decimals=3):

    if value is None:
        return None

    try:
        return round(
            float(value),
            decimals,
        )
    except Exception:
        return None


def build_market_payload():

    with state_lock:

        price = gold_price

        payload = {
            "symbol": GOLD_SYMBOL,
            "price": public_number(
                price,
                3,
            ),
            "timestamp": (
                timestamp_to_iso(
                    gold_timestamp
                )
                if gold_timestamp
                else None
            ),
            "live": gold_ws_connected,
            "connection": (
                "CONNECTED"
                if gold_ws_connected
                else "DISCONNECTED"
            ),
            "data_status": data_status,
            "bootstrap": bootstrap_status,
            "last_error": last_error,

            "ai": {
                "conclusion": ai_state[
                    "conclusion"
                ],
                "confidence": ai_state[
                    "confidence"
                ],
                "score": ai_state[
                    "score"
                ],
                "trend": ai_state[
                    "trend"
                ],
                "momentum": ai_state[
                    "momentum"
                ],
                "structure": ai_state[
                    "structure"
                ],
                "rsi": public_number(
                    ai_state["rsi"],
                    2,
                ),
                "ema20": public_number(
                    ai_state["ema20"],
                    3,
                ),
                "ema50": public_number(
                    ai_state["ema50"],
                    3,
                ),
                "support": public_number(
                    ai_state["support"],
                    3,
                ),
                "resistance": public_number(
                    ai_state["resistance"],
                    3,
                ),
                "liquidity_high": public_number(
                    ai_state[
                        "liquidity_high"
                    ],
                    3,
                ),
                "liquidity_low": public_number(
                    ai_state[
                        "liquidity_low"
                    ],
                    3,
                ),
                "sweep": ai_state[
                    "sweep"
                ],
                "engine": ai_state[
                    "engine"
                ],
                "engine_status": ai_state[
                    "engine_status"
                ],
                "reasons": ai_state[
                    "reasons"
                ],
                "warnings": ai_state[
                    "warnings"
                ],
                "setup": ai_state[
                    "setup"
                ],
                "entry": public_number(
                    ai_state["entry"],
                    3,
                ),
                "stop_loss": public_number(
                    ai_state["stop_loss"],
                    3,
                ),
                "target1": public_number(
                    ai_state["target1"],
                    3,
                ),
                "target2": public_number(
                    ai_state["target2"],
                    3,
                ),
                "target3": public_number(
                    ai_state["target3"],
                    3,
                ),
                "rr": public_number(
                    ai_state["rr"],
                    2,
                ),
                "invalidation": public_number(
                    ai_state[
                        "invalidation"
                    ],
                    3,
                ),
                "updated": ai_state[
                    "updated"
                ],
            },

            "candles": {
                "1m": len(candles_1m),
                "5m": len(candles_5m),
                "15m": len(candles_15m),
                "30m": len(candles_30m),
                "1h": len(candles_1h),
            },

            "history": list(
                signal_history
            ),

            "oil": {
                "enabled": OIL_ENABLED,
                "symbol": OIL_SYMBOL,
                "price": None,
                "status": (
                    "PLAN LIMIT"
                    if not OIL_ENABLED
                    else "LIVE"
                ),
            },
        }

    return payload


# ============================================================
# DASHBOARD HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>Trading-AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #07101d;
    color: #e8eef7;
    font-family: Arial, Helvetica, sans-serif;
}

.container {
    max-width: 1200px;
    margin: auto;
    padding: 20px;
}

.card {
    background: #0d1726;
    border: 1px solid #1b2a3d;
    border-radius: 14px;
    padding: 20px;
    margin-bottom: 18px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 20px;
    flex-wrap: wrap;
}

.title {
    font-size: 22px;
    font-weight: 700;
}

.price {
    font-size: 32px;
    font-weight: 700;
    margin-top: 8px;
}

.live {
    font-size: 13px;
    margin-top: 6px;
}

.dot {
    color: #39d98a;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(4, minmax(0, 1fr));
    gap: 12px;
}

.metric {
    background: #111e30;
    border-radius: 10px;
    padding: 14px;
    min-height: 75px;
}

.label {
    color: #8392a7;
    font-size: 11px;
    text-transform: uppercase;
    margin-bottom: 8px;
}

.value {
    font-size: 17px;
    font-weight: 700;
}

.big {
    font-size: 28px;
}

.wait {
    color: #c4ccd7;
}

.buy {
    color: #36d991;
}

.sell {
    color: #ff6878;
}

.tabs {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
}

.tab {
    background: #111e30;
    border: 1px solid #26374e;
    color: #dce5f0;
    padding: 8px 14px;
    border-radius: 8px;
    cursor: pointer;
}

.tab.active {
    border-color: #5c8dff;
}

.reason {
    padding: 8px 0;
    border-bottom: 1px solid #1b2a3d;
    color: #b8c4d3;
}

.warning {
    color: #ffbf69;
    padding: 7px 0;
}

.history {
    font-size: 13px;
}

.history-row {
    display: flex;
    justify-content: space-between;
    padding: 9px 0;
    border-bottom: 1px solid #1b2a3d;
}

.small {
    color: #7f8da0;
    font-size: 12px;
}

@media(max-width: 800px) {

    .grid {
        grid-template-columns:
            repeat(2, minmax(0, 1fr));
    }
}

@media(max-width: 500px) {

    .grid {
        grid-template-columns:
            1fr;
    }

    .price {
        font-size: 26px;
    }
}

</style>
</head>

<body>

<div class="container">

<div class="card">

<div class="header">

<div>

<div class="title">
🥇 Gold — XAU/USD
</div>

<div id="price"
     class="price">
—
</div>

<div id="live"
     class="live">
● LIVE
</div>

</div>

<div id="dataStatus"
     class="small">
Data: STARTING
</div>

</div>

</div>


<div class="card">

<div class="grid">

<div class="metric">
<div class="label">Trend</div>
<div id="trend"
     class="value">
WAITING
</div>
</div>

<div class="metric">
<div class="label">Momentum</div>
<div id="momentum"
     class="value">
WAITING
</div>
</div>

<div class="metric">
<div class="label">Structure</div>
<div id="structure"
     class="value">
WAITING
</div>
</div>

<div class="metric">
<div class="label">RSI</div>
<div id="rsi"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">EMA 20</div>
<div id="ema20"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">EMA 50</div>
<div id="ema50"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Support</div>
<div id="support"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Resistance</div>
<div id="resistance"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Liquidity High</div>
<div id="liqHigh"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Liquidity Low</div>
<div id="liqLow"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Sweep</div>
<div id="sweep"
     class="value">
NONE
</div>
</div>

</div>

</div>


<div class="card">

<div class="label">
TIMEFRAME
</div>

<div class="tabs">

<button class="tab active">1m</button>
<button class="tab">5m</button>
<button class="tab">15m</button>
<button class="tab">30m</button>
<button class="tab">1H</button>
<button class="tab">4H</button>
<button class="tab">1D</button>
<button class="tab">1W</button>
<button class="tab">1M</button>

</div>

</div>


<div class="card">

<div class="label">
TRADING-AI CONCLUSION
</div>

<div id="conclusion"
     class="big wait">
WAIT
</div>

<div style="margin-top:10px">
Confidence:
<strong id="confidence">
50%
</strong>
</div>

<div style="margin-top:8px">
AI SCORE:
<strong id="score">
0
</strong>
</div>

<div style="margin-top:8px"
     class="small">

Engine:
<span id="engine">
LOCAL • WAITING
</span>

</div>

</div>


<div class="card">

<div class="title">
🎯 AI TRADE SETUP
</div>

<br>

<div class="grid">

<div class="metric">
<div class="label">Entry</div>
<div id="entry"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Stop Loss</div>
<div id="stop"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Target 1</div>
<div id="target1"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Target 2</div>
<div id="target2"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Target 3</div>
<div id="target3"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">R:R</div>
<div id="rr"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Invalidation</div>
<div id="invalid"
     class="value">
—
</div>
</div>

<div class="metric">
<div class="label">Setup</div>
<div id="setup"
     class="value">
WAIT
</div>
</div>

</div>

<br>

<div class="label">
WHY
</div>

<div id="reasons">
Waiting for confirmation.
</div>

<br>

<div class="label">
MULTI-FACTOR EVIDENCE
</div>

<div id="warnings">
Waiting for enough evidence.
</div>

</div>


<div class="card">

<div class="title">
🕐 AI SIGNAL HISTORY
</div>

<br>

<div id="history"
     class="history">
No signal changes recorded yet.
</div>

</div>


<div class="card">

<div class="title">
🛢️ Crude Oil — WTI
</div>

<div style="margin-top:10px">

<strong>—</strong>

</div>

<div class="small"
     style="margin-top:8px">

● PLAN LIMIT

<br><br>

WTI/USD is not available on the
current Twelve Data plan.

</div>

</div>


<div class="small">
LIVE MARKET DATA • TRADING-AI
</div>

</div>


<script>

function setText(id, value) {

    const el =
        document.getElementById(id);

    if (!el) return;

    if (
        value === null ||
        value === undefined
    ) {

        el.textContent = "—";

    } else {

        el.textContent = value;
    }
}


function formatNumber(value) {

    if (
        value === null ||
        value === undefined
    ) {

        return "—";
    }

    return Number(value).toFixed(3);
}


function updateConclusion(value) {

    const el =
        document.getElementById(
            "conclusion"
        );

    el.textContent = value || "WAIT";

    el.className = "big";

    if (value === "BUY") {

        el.classList.add("buy");

    } else if (value === "SELL") {

        el.classList.add("sell");

    } else {

        el.classList.add("wait");
    }
}


function updateDashboard(data) {

    const ai = data.ai || {};

    setText(
        "price",
        formatNumber(data.price)
    );

    setText(
        "live",
        data.connection === "CONNECTED"
            ? "● LIVE • CONNECTED"
            : "● DISCONNECTED"
    );

    setText(
        "dataStatus",
        "Data: " +
        (
            data.data_status ||
            "STARTING"
        )
    );

    setText(
        "trend",
        ai.trend || "WAITING"
    );

    setText(
        "momentum",
        ai.momentum || "WAITING"
    );

    setText(
        "structure",
        ai.structure || "WAITING"
    );

    setText(
        "rsi",
        ai.rsi !== null &&
        ai.rsi !== undefined
            ? Number(ai.rsi).toFixed(2)
            : "—"
    );

    setText(
        "ema20",
        formatNumber(ai.ema20)
    );

    setText(
        "ema50",
        formatNumber(ai.ema50)
    );

    setText(
        "support",
        formatNumber(ai.support)
    );

    setText(
        "resistance",
        formatNumber(ai.resistance)
    );

    setText(
        "liqHigh",
        formatNumber(
            ai.liquidity_high
        )
    );

    setText(
        "liqLow",
        formatNumber(
            ai.liquidity_low
        )
    );

    setText(
        "sweep",
        ai.sweep || "NONE"
    );

    updateConclusion(
        ai.conclusion
    );

    setText(
        "confidence",
        (
            ai.confidence ??
            50
        ) + "%"
    );

    setText(
        "score",
        ai.score ?? 0
    );

    setText(
        "engine",
        (
            ai.engine ||
            "LOCAL"
        ) +
        " • " +
        (
            ai.engine_status ||
            "WAITING"
        )
    );

    setText(
        "entry",
        formatNumber(ai.entry)
    );

    setText(
        "stop",
        formatNumber(
            ai.stop_loss
        )
    );

    setText(
        "target1",
        formatNumber(ai.target1)
    );

    setText(
        "target2",
        formatNumber(ai.target2)
    );

    setText(
        "target3",
        formatNumber(ai.target3)
    );

    setText(
        "rr",
        ai.rr !== null &&
        ai.rr !== undefined
            ? "1:" +
              Number(ai.rr).toFixed(2)
            : "—"
    );

    setText(
        "invalid",
        formatNumber(
            ai.invalidation
        )
    );

    setText(
        "setup",
        ai.setup || "WAIT"
    );

    const reasons =
        document.getElementById(
            "reasons"
        );

    if (
        ai.reasons &&
        ai.reasons.length
    ) {

        reasons.innerHTML =
            ai.reasons
                .map(
                    x =>
                    '<div class="reason">' +
                    x +
                    '</div>'
                )
                .join("");

    } else {

        reasons.textContent =
            "Waiting for confirmation.";
    }


    const warnings =
        document.getElementById(
            "warnings"
        );

    if (
        ai.warnings &&
        ai.warnings.length
    ) {

        warnings.innerHTML =
            ai.warnings
                .map(
                    x =>
                    '<div class="warning">' +
                    x +
                    '</div>'
                )
                .join("");

    } else {

        warnings.textContent =
            "Multi-timeframe evidence is aligned.";
    }


    const history =
        document.getElementById(
            "history"
        );

    if (
        data.history &&
        data.history.length
    ) {

        history.innerHTML =
            data.history
                .map(item => {

                    const time =
                        new Date(
                            item.time
                        ).toLocaleTimeString();

                    return `
                    <div class="history-row">
                        <span>
                            ${item.signal}
                        </span>
                        <span>
                            Score ${item.score}
                            • ${item.confidence}%
                            • ${time}
                        </span>
                    </div>
                    `;

                })
                .join("");

    } else {

        history.textContent =
            "No signal changes recorded yet.";
    }
}


async function refresh() {

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

        updateDashboard(data);

    } catch (error) {

        console.log(
            "Dashboard refresh error:",
            error
        );
    }
}


document
    .querySelectorAll(".tab")
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                document
                    .querySelectorAll(
                        ".tab"
                    )
                    .forEach(
                        b =>
                        b.classList.remove(
                            "active"
                        )
                    );

                button.classList.add(
                    "active"
                );

            }
        );

    });


refresh();

setInterval(
    refresh,
    2000
);

</script>

</body>
</html>
"""


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    return Response(
        HTML,
        mimetype="text/html",
    )


@app.route("/api/market")
def market_api():

    return jsonify(
        build_market_payload()
    )


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "gold_ws": gold_ws_connected,
        "data_status": data_status,
        "candles": {
            "1m": len(candles_1m),
            "5m": len(candles_5m),
            "15m": len(candles_15m),
            "30m": len(candles_30m),
            "1h": len(candles_1h),
        },
    })


@app.route("/stream")
def stream():

    return jsonify({
        "status": "live",
        "gold": gold_price,
        "connection": (
            "CONNECTED"
            if gold_ws_connected
            else "DISCONNECTED"
        ),
    })


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    global workers_started

    if workers_started:
        return

    workers_started = True

    print(
        "Starting Trading-AI workers..."
    )

    websocket_thread = threading.Thread(
        target=gold_websocket_loop,
        daemon=True,
    )

    websocket_thread.start()

    bootstrap_thread = threading.Thread(
        target=bootstrap_worker,
        daemon=True,
    )

    bootstrap_thread.start()

    ai_thread = threading.Thread(
        target=ai_worker,
        daemon=True,
    )

    ai_thread.start()


start_workers()


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                5000,
            )
        ),
        debug=False,
        threaded=True,
    )
