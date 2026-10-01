import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string


# ============================================================
# TRADING AI - MULTI-FACTOR SIGNAL ENGINE 2.0
# Goal:
# Better information + fewer false signals
#
# Features:
# - Twelve Data REST history
# - Twelve Data WebSocket live price
# - EMA20 / EMA50
# - RSI14
# - ATR14
# - Market structure
# - Liquidity
# - Breakout detection
# - Retest detection
# - Liquidity sweep detection
# - Momentum
# - Volatility
# - Exhaustion / chase filter
# - Conflict detection
# - Confidence score
# - Entry zone
# - Invalidation
# - Target 1 / Target 2
# - BUY / SELL / WAIT
# ============================================================


# ============================================================
# CONFIG
# ============================================================

APP_NAME = "Trading AI"

SYMBOL = "XAU/USD"
INTERVAL = "5min"

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

REST_BASE = "https://api.twelvedata.com"
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"

PORT = int(os.getenv("PORT", "10000"))

HISTORY_SIZE = 300

# Prevent accidental repeated REST calls
HISTORY_REFRESH_SECONDS = 60 * 60

# Live price state
price = None
last_tick_time = None

# Engine state
candles = []

connected = False
subscribed = False
history_loaded = False

last_error = None
last_signal_change = None

last_history_load = 0

state_lock = threading.Lock()


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# LOGGING
# ============================================================

def log(message):
    print(f"[TRADING-AI] {message}", flush=True)


# ============================================================
# BASIC HELPERS
# ============================================================

def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def clamp(value, low, high):
    return max(low, min(high, value))


def utc_now():
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# TWELVE DATA REST
# ============================================================

def load_history(force=False):
    global candles
    global history_loaded
    global last_error
    global last_history_load

    now = time.time()

    if not force and history_loaded:
        if now - last_history_load < HISTORY_REFRESH_SECONDS:
            return True

    if not TWELVE_DATA_API_KEY:
        last_error = "TWELVE_DATA_API_KEY is missing"
        log(last_error)
        return False

    url = f"{REST_BASE}/time_series"

    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "outputsize": HISTORY_SIZE,
        "apikey": TWELVE_DATA_API_KEY,
        "format": "JSON",
    }

    try:
        log(f"Loading {HISTORY_SIZE} candles: {SYMBOL} {INTERVAL}")

        response = requests.get(
            url,
            params=params,
            timeout=20,
        )

        data = response.json()

        if response.status_code != 200:
            raise RuntimeError(
                f"Twelve Data HTTP {response.status_code}: {data}"
            )

        if isinstance(data, dict) and data.get("code"):
            raise RuntimeError(
                f"Twelve Data error: {data}"
            )

        values = data.get("values", [])

        if not values:
            raise RuntimeError(f"No candle data returned: {data}")

        parsed = []

        for row in values:
            try:
                parsed.append({
                    "datetime": row.get("datetime"),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": safe_float(row.get("volume"), 0),
                })
            except Exception:
                continue

        parsed.sort(key=lambda x: x["datetime"])

        if len(parsed) < 50:
            raise RuntimeError(
                f"Not enough candles. Received {len(parsed)}"
            )

        with state_lock:
            candles = parsed[-HISTORY_SIZE:]
            history_loaded = True
            last_history_load = now
            last_error = None

        log(
            f"History loaded successfully: {len(parsed[-HISTORY_SIZE:])} candles"
        )

        return True

    except Exception as exc:
        last_error = repr(exc)
        log(f"HISTORY ERROR {SYMBOL} {INTERVAL}: {repr(exc)}")
        return False


# ============================================================
# LIVE CANDLE UPDATE
# ============================================================

def update_live_candle(live_price, timestamp=None):
    global candles
    global price
    global last_tick_time

    if live_price is None:
        return

    live_price = safe_float(live_price)

    if live_price is None:
        return

    price = live_price
    last_tick_time = time.time()

    # Twelve Data websocket timestamp is usually Unix timestamp.
    # We convert it into the current 5-minute bucket.
    if timestamp is not None:
        try:
            ts = int(float(timestamp))
        except Exception:
            ts = int(time.time())
    else:
        ts = int(time.time())

    bucket = ts - (ts % 300)

    dt = datetime.fromtimestamp(
        bucket,
        tz=timezone.utc
    ).isoformat()

    with state_lock:

        if not candles:
            candles.append({
                "datetime": dt,
                "open": live_price,
                "high": live_price,
                "low": live_price,
                "close": live_price,
                "volume": 0,
            })

            return

        last = candles[-1]

        try:
            last_bucket = int(
                datetime.fromisoformat(
                    last["datetime"].replace("Z", "+00:00")
                ).timestamp()
            )
        except Exception:
            last_bucket = bucket

        last_bucket = last_bucket - (last_bucket % 300)

        # Same 5-minute candle
        if last_bucket == bucket:

            last["high"] = max(
                safe_float(last["high"], live_price),
                live_price
            )

            last["low"] = min(
                safe_float(last["low"], live_price),
                live_price
            )

            last["close"] = live_price

        # New 5-minute candle
        else:

            candles.append({
                "datetime": dt,
                "open": live_price,
                "high": live_price,
                "low": live_price,
                "close": live_price,
                "volume": 0,
            })

            if len(candles) > HISTORY_SIZE:
                candles = candles[-HISTORY_SIZE:]

    log(f"PRICE RECEIVED: {SYMBOL} = {live_price}")
    log(f"LIVE CANDLE UPDATED: {dt} close={live_price}")


# ============================================================
# WEBSOCKET
# ============================================================

def websocket_on_open(ws):
    global connected
    global subscribed

    connected = True

    log("WebSocket connected")

    subscribe_message = {
        "action": "subscribe",
        "params": {
            "symbols": SYMBOL
        }
    }

    try:
        ws.send(json.dumps(subscribe_message))
        log(f"Subscription request sent for {SYMBOL}")
    except Exception as exc:
        log(f"Subscription send error: {repr(exc)}")


def websocket_on_message(ws, message):
    global subscribed
    global last_error

    try:
        data = json.loads(message)

        # Debug useful response types
        event = data.get("event")

        if event == "subscribe-status":

            status = data.get("status")

            log(f"SUBSCRIBE STATUS: {status}")

            if status == "ok":
                subscribed = True
                log(f"Subscribed to {SYMBOL}")

            return

        # Some websocket responses contain status
        if data.get("status") == "error":
            last_error = str(data)
            log(f"WEBSOCKET API ERROR: {data}")
            return

        # Twelve Data price event
        if event == "price":

            live_price = safe_float(data.get("price"))

            timestamp = data.get("timestamp")

            if live_price is not None:
                update_live_candle(
                    live_price,
                    timestamp
                )

            return

        # Alternate format protection
        if "price" in data:

            live_price = safe_float(data.get("price"))

            timestamp = data.get("timestamp")

            if live_price is not None:
                update_live_candle(
                    live_price,
                    timestamp
                )

            return

    except Exception as exc:
        last_error = repr(exc)
        log(f"WEBSOCKET MESSAGE ERROR: {repr(exc)}")


def websocket_on_error(ws, error):
    global connected
    global subscribed
    global last_error

    connected = False
    subscribed = False

    last_error = repr(error)

    log(f"WEBSOCKET ERROR: {repr(error)}")


def websocket_on_close(ws, close_status_code, close_msg):
    global connected
    global subscribed

    connected = False
    subscribed = False

    log(
        f"WebSocket closed: "
        f"{close_status_code} {close_msg}"
    )


def websocket_worker():

    global connected
    global subscribed

    while True:

        try:

            if not TWELVE_DATA_API_KEY:
                log("WebSocket skipped: API key missing")
                time.sleep(10)
                continue

            log("Connecting to Twelve Data WebSocket...")

            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=websocket_on_open,
                on_message=websocket_on_message,
                on_error=websocket_on_error,
                on_close=websocket_on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:

            connected = False
            subscribed = False

            log(
                f"WebSocket worker exception: {repr(exc)}"
            )

        log("WebSocket reconnecting in 5 seconds...")

        time.sleep(5)


# ============================================================
# INDICATORS
# ============================================================

def closes():
    return [
        safe_float(c["close"])
        for c in candles
        if safe_float(c.get("close")) is not None
    ]


def highs():
    return [
        safe_float(c["high"])
        for c in candles
        if safe_float(c.get("high")) is not None
    ]


def lows():
    return [
        safe_float(c["low"])
        for c in candles
        if safe_float(c.get("low")) is not None
    ]


def ema(values, period):

    if not values:
        return None

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    value = sum(values[:period]) / period

    for current in values[period:]:
        value = (
            (current - value) * multiplier
        ) + value

    return value


def rsi(values, period=14):

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)

        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1)) +
            gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) +
            losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def atr(candle_data, period=14):

    if len(candle_data) <= period:
        return None

    trs = []

    for i in range(1, len(candle_data)):

        current = candle_data[i]
        previous = candle_data[i - 1]

        high = safe_float(current["high"])
        low = safe_float(current["low"])
        previous_close = safe_float(previous["close"])

        if high is None or low is None or previous_close is None:
            continue

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    value = sum(trs[:period]) / period

    for tr in trs[period:]:
        value = (
            ((value * (period - 1)) + tr)
            / period
        )

    return value


# ============================================================
# MARKET STRUCTURE
# ============================================================

def market_structure(data):

    if len(data) < 20:
        return "INSUFFICIENT"

    recent = data[-12:]

    recent_highs = [
        safe_float(x["high"])
        for x in recent
    ]

    recent_lows = [
        safe_float(x["low"])
        for x in recent
    ]

    mid = len(recent) // 2

    first_half = recent[:mid]
    second_half = recent[mid:]

    first_high = max(
        safe_float(x["high"])
        for x in first_half
    )

    second_high = max(
        safe_float(x["high"])
        for x in second_half
    )

    first_low = min(
        safe_float(x["low"])
        for x in first_half
    )

    second_low = min(
        safe_float(x["low"])
        for x in second_half
    )

    if (
        second_high < first_high
        and
        second_low < first_low
    ):
        return "BEARISH_STRUCTURE"

    if (
        second_high > first_high
        and
        second_low > first_low
    ):
        return "BULLISH_STRUCTURE"

    return "RANGE"


# ============================================================
# LIQUIDITY LEVELS
# ============================================================

def liquidity_levels(data, lookback=20):

    if len(data) < lookback:
        return None, None

    recent = data[-lookback:]

    high = max(
        safe_float(x["high"])
        for x in recent
    )

    low = min(
        safe_float(x["low"])
        for x in recent
    )

    return high, low


# ============================================================
# BREAKOUT DETECTION
# ============================================================

def detect_breakout(data, liquidity_high, liquidity_low):

    if len(data) < 3:
        return "NONE"

    previous = data[-2]
    current = data[-1]

    previous_close = safe_float(previous["close"])
    current_close = safe_float(current["close"])

    current_high = safe_float(current["high"])
    current_low = safe_float(current["low"])

    if None in (
        previous_close,
        current_close,
        current_high,
        current_low,
        liquidity_high,
        liquidity_low,
    ):
        return "NONE"

    # Strong bearish break
    if (
        previous_close >= liquidity_low
        and
        current_close < liquidity_low
    ):
        return "BEARISH_BREAKOUT"

    # Strong bullish break
    if (
        previous_close <= liquidity_high
        and
        current_close > liquidity_high
    ):
        return "BULLISH_BREAKOUT"

    return "NONE"


# ============================================================
# LIQUIDITY SWEEP
# ============================================================

def detect_liquidity_sweep(
    data,
    liquidity_high,
    liquidity_low,
):

    if len(data) < 3:
        return "NONE"

    current = data[-1]

    high = safe_float(current["high"])
    low = safe_float(current["low"])
    close = safe_float(current["close"])

    if None in (
        high,
        low,
        close,
        liquidity_high,
        liquidity_low,
    ):
        return "NONE"

    # Sweep above high but close back below
    if (
        high > liquidity_high
        and
        close < liquidity_high
    ):
        return "BEARISH_SWEEP"

    # Sweep below low but close back above
    if (
        low < liquidity_low
        and
        close > liquidity_low
    ):
        return "BULLISH_SWEEP"

    return "NONE"


# ============================================================
# RETEST DETECTION
# ============================================================

def detect_retest(
    data,
    liquidity_high,
    liquidity_low,
    atr_value,
):

    if len(data) < 5:
        return "NONE"

    if atr_value is None or atr_value <= 0:
        return "NONE"

    current = data[-1]

    close = safe_float(current["close"])
    high = safe_float(current["high"])
    low = safe_float(current["low"])

    tolerance = atr_value * 0.35

    # Search recent candles for breakout and retest.
    recent = data[-6:]

    bearish_break_seen = False
    bullish_break_seen = False

    for candle in recent[:-1]:

        candle_close = safe_float(candle["close"])
        candle_high = safe_float(candle["high"])
        candle_low = safe_float(candle["low"])

        if None in (
            candle_close,
            candle_high,
            candle_low,
        ):
            continue

        if candle_close < liquidity_low:
            bearish_break_seen = True

        if candle_close > liquidity_high:
            bullish_break_seen = True

    # Bearish breakout then retest from below
    if bearish_break_seen:

        if (
            abs(high - liquidity_low) <= tolerance
            and
            close < liquidity_low
        ):
            return "BEARISH_RETEST"

    # Bullish breakout then retest from above
    if bullish_break_seen:

        if (
            abs(low - liquidity_high) <= tolerance
            and
            close > liquidity_high
        ):
            return "BULLISH_RETEST"

    return "NONE"


# ============================================================
# MOMENTUM
# ============================================================

def momentum_state(
    current_price,
    ema20_value,
    ema50_value,
    rsi_value,
):

    if None in (
        current_price,
        ema20_value,
        ema50_value,
        rsi_value,
    ):
        return "NEUTRAL"

    bullish_points = 0
    bearish_points = 0

    if current_price > ema20_value:
        bullish_points += 1
    else:
        bearish_points += 1

    if ema20_value > ema50_value:
        bullish_points += 1
    else:
        bearish_points += 1

    if rsi_value > 55:
        bullish_points += 1

    if rsi_value < 45:
        bearish_points += 1

    if bullish_points >= 3:
        return "BULLISH"

    if bearish_points >= 3:
        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# TREND
# ============================================================

def trend_state(
    current_price,
    ema20_value,
    ema50_value,
):

    if None in (
        current_price,
        ema20_value,
        ema50_value,
    ):
        return "UNKNOWN"

    if (
        current_price < ema20_value
        and
        ema20_value < ema50_value
    ):
        return "BEARISH"

    if (
        current_price > ema20_value
        and
        ema20_value > ema50_value
    ):
        return "BULLISH"

    return "RANGE"


# ============================================================
# VOLATILITY
# ============================================================

def volatility_state(
    atr_value,
    current_price,
):

    if None in (
        atr_value,
        current_price,
    ):
        return "UNKNOWN"

    percentage = (
        atr_value / current_price
    ) * 100

    if percentage > 0.18:
        return "HIGH"

    if percentage < 0.07:
        return "LOW"

    return "NORMAL"


# ============================================================
# SIGNAL ENGINE 2.0
# ============================================================

def build_signal(data):

    if len(data) < 60:

        return {
            "signal": "WAIT",
            "confidence": 0,
            "strength": 0,
            "setup": "INSUFFICIENT_DATA",
            "market_state": "UNKNOWN",
            "entry_zone": None,
            "invalidation": None,
            "target_1": None,
            "target_2": None,
            "risk_reward": None,
            "reasons": [
                "Waiting for enough market data"
            ],
        }

    close_values = [
        safe_float(x["close"])
        for x in data
    ]

    current = close_values[-1]

    ema20_value = ema(
        close_values,
        20
    )

    ema50_value = ema(
        close_values,
        50
    )

    rsi_value = rsi(
        close_values,
        14
    )

    atr_value = atr(
        data,
        14
    )

    structure = market_structure(data)

    liquidity_high, liquidity_low = liquidity_levels(
        data,
        20
    )

    breakout = detect_breakout(
        data,
        liquidity_high,
        liquidity_low,
    )

    sweep = detect_liquidity_sweep(
        data,
        liquidity_high,
        liquidity_low,
    )

    retest = detect_retest(
        data,
        liquidity_high,
        liquidity_low,
        atr_value,
    )

    trend = trend_state(
        current,
        ema20_value,
        ema50_value,
    )

    momentum = momentum_state(
        current,
        ema20_value,
        ema50_value,
        rsi_value,
    )

    volatility = volatility_state(
        atr_value,
        current,
    )

    # ========================================================
    # SCORE
    # ========================================================

    bullish_score = 0
    bearish_score = 0

    reasons = []

    # ----------------------------
    # TREND
    # ----------------------------

    if trend == "BULLISH":

        bullish_score += 2

        reasons.append(
            "Price above EMA20 and EMA20 above EMA50"
        )

    elif trend == "BEARISH":

        bearish_score += 2

        reasons.append(
            "Price below EMA20 and EMA20 below EMA50"
        )

    else:

        reasons.append(
            "Trend structure is mixed"
        )

    # ----------------------------
    # MOMENTUM
    # ----------------------------

    if momentum == "BULLISH":

        bullish_score += 1

        reasons.append(
            "Momentum bullish"
        )

    elif momentum == "BEARISH":

        bearish_score += 1

        reasons.append(
            "Momentum bearish"
        )

    # ----------------------------
    # STRUCTURE
    # ----------------------------

    if structure == "BULLISH_STRUCTURE":

        bullish_score += 2

        reasons.append(
            "Market structure bullish"
        )

    elif structure == "BEARISH_STRUCTURE":

        bearish_score += 2

        reasons.append(
            "Market structure bearish"
        )

    else:

        reasons.append(
            "Market structure is ranging"
        )

    # ----------------------------
    # BREAKOUT
    # ----------------------------

    if breakout == "BULLISH_BREAKOUT":

        bullish_score += 2

        reasons.append(
            "Price broke above liquidity high"
        )

    elif breakout == "BEARISH_BREAKOUT":

        bearish_score += 2

        reasons.append(
            "Price broke below liquidity low"
        )

    # ----------------------------
    # RETEST
    # ----------------------------

    if retest == "BULLISH_RETEST":

        bullish_score += 3

        reasons.append(
            "Bullish breakout retest confirmed"
        )

    elif retest == "BEARISH_RETEST":

        bearish_score += 3

        reasons.append(
            "Bearish breakout retest confirmed"
        )

    # ----------------------------
    # SWEEP
    # ----------------------------

    if sweep == "BULLISH_SWEEP":

        bullish_score += 2

        reasons.append(
            "Liquidity swept below and reclaimed"
        )

    elif sweep == "BEARISH_SWEEP":

        bearish_score += 2

        reasons.append(
            "Liquidity swept above and rejected"
        )

    # ========================================================
    # RSI EXHAUSTION / CHASE FILTER
    # ========================================================

    oversold = (
        rsi_value is not None
        and
        rsi_value < 30
    )

    overbought = (
        rsi_value is not None
        and
        rsi_value > 70
    )

    extreme_oversold = (
        rsi_value is not None
        and
        rsi_value < 25
    )

    extreme_overbought = (
        rsi_value is not None
        and
        rsi_value > 75
    )

    chase_risk = False

    if oversold:

        reasons.append(
            "RSI oversold - avoid chasing SELL"
        )

        chase_risk = True

    if overbought:

        reasons.append(
            "RSI overbought - avoid chasing BUY"
        )

        chase_risk = True

    # ========================================================
    # CONFLICT FILTER
    # ========================================================

    score_difference = abs(
        bullish_score - bearish_score
    )

    conflict = False

    if score_difference <= 2:

        conflict = True

        reasons.append(
            "Bullish and bearish factors are conflicting"
        )

    # ========================================================
    # SETUP CLASSIFICATION
    # ========================================================

    setup = "NO_CLEAR_SETUP"

    if retest == "BULLISH_RETEST":

        setup = "BULLISH_CONTINUATION"

    elif retest == "BEARISH_RETEST":

        setup = "BEARISH_CONTINUATION"

    elif sweep == "BULLISH_SWEEP":

        setup = "POSSIBLE_BULLISH_REVERSAL"

    elif sweep == "BEARISH_SWEEP":

        setup = "POSSIBLE_BEARISH_REVERSAL"

    elif breakout == "BULLISH_BREAKOUT":

        setup = "BULLISH_BREAKOUT_WAIT_RETEST"

    elif breakout == "BEARISH_BREAKOUT":

        setup = "BEARISH_BREAKOUT_WAIT_RETEST"

    elif trend == "BULLISH":

        setup = "BULLISH_TREND"

    elif trend == "BEARISH":

        setup = "BEARISH_TREND"

    # ========================================================
    # FINAL SIGNAL
    # ========================================================

    signal = "WAIT"

    strength = 0

    # --------------------------------------------------------
    # BULLISH CONTINUATION
    # --------------------------------------------------------

    if (
        bullish_score >= 7
        and
        bullish_score > bearish_score
    ):

        if extreme_overbought:

            signal = "WAIT"

            reasons.append(
                "Bullish setup exists but price is extremely overbought"
            )

        elif conflict:

            signal = "WAIT"

        else:

            signal = "BUY"

    # --------------------------------------------------------
    # BEARISH CONTINUATION
    # --------------------------------------------------------

    elif (
        bearish_score >= 7
        and
        bearish_score > bullish_score
    ):

        if extreme_oversold:

            signal = "WAIT"

            reasons.append(
                "Bearish setup exists but price is extremely oversold"
            )

        elif (
            oversold
            and
            retest != "BEARISH_RETEST"
        ):

            signal = "WAIT"

            reasons.append(
                "Bearish trend exists, but fresh SELL lacks retest confirmation"
            )

        elif conflict:

            signal = "WAIT"

        else:

            signal = "SELL"

    # --------------------------------------------------------
    # MODERATE BULLISH
    # --------------------------------------------------------

    elif (
        bullish_score >= 5
        and
        bullish_score > bearish_score + 2
    ):

        if (
            not overbought
            and
            not conflict
        ):

            signal = "BUY"

        else:

            signal = "WAIT"

    # --------------------------------------------------------
    # MODERATE BEARISH
    # --------------------------------------------------------

    elif (
        bearish_score >= 5
        and
        bearish_score > bullish_score + 2
    ):

        if (
            not oversold
            and
            not conflict
        ):

            signal = "SELL"

        else:

            signal = "WAIT"

    # ========================================================
    # EXTRA WAIT FILTERS
    # ========================================================

    if volatility == "LOW":

        reasons.append(
            "Low volatility - breakout quality may be weak"
        )

        if signal in ("BUY", "SELL"):

            signal = "WAIT"

    if structure == "RANGE":

        reasons.append(
            "Market is ranging - directional signal needs stronger confirmation"
        )

        if retest == "NONE":

            signal = "WAIT"

    # ========================================================
    # STRENGTH
    # ========================================================

    raw_strength = (
        bullish_score - bearish_score
    )

    strength = raw_strength

    # ========================================================
    # CONFIDENCE
    # ========================================================

    total_score = (
        bullish_score +
        bearish_score
    )

    if total_score <= 0:

        confidence = 0

    else:

        directional_edge = (
            abs(
                bullish_score -
                bearish_score
            )
            /
            max(total_score, 1)
        )

        confidence = 50 + (
            directional_edge * 50
        )

        # Confirmation bonus
        if retest in (
            "BULLISH_RETEST",
            "BEARISH_RETEST",
        ):
            confidence += 8

        # Sweep bonus
        if sweep != "NONE":
            confidence += 3

        # Conflict penalty
        if conflict:
            confidence -= 15

        # Chase penalty
        if chase_risk:
            confidence -= 12

        confidence = int(
            clamp(
                confidence,
                0,
                95
            )
        )

    # WAIT confidence should indicate information quality,
    # not trade certainty.
    if signal == "WAIT":

        confidence = int(
            clamp(
                confidence,
                0,
                90
            )
        )

    # ========================================================
    # ENTRY / INVALIDATION / TARGETS
    # ========================================================

    entry_zone = None
    invalidation = None
    target_1 = None
    target_2 = None
    risk_reward = None

    if atr_value is not None and atr_value > 0:

        # BUY setup
        if signal == "BUY":

            entry_low = current - (
                atr_value * 0.20
            )

            entry_high = current + (
                atr_value * 0.10
            )

            stop = min(
                liquidity_low,
                current - (
                    atr_value * 1.20
                )
            )

            risk = current - stop

            t1 = current + (
                risk * 1.5
            )

            t2 = current + (
                risk * 2.5
            )

            entry_zone = {
                "low": round(entry_low, 2),
                "high": round(entry_high, 2),
            }

            invalidation = round(
                stop,
                2
            )

            target_1 = round(
                t1,
                2
            )

            target_2 = round(
                t2,
                2
            )

            if risk > 0:
                risk_reward = {
                    "target_1": round(
                        (t1 - current) / risk,
                        2
                    ),
                    "target_2": round(
                        (t2 - current) / risk,
                        2
                    ),
                }

        # SELL setup
        elif signal == "SELL":

            entry_low = current - (
                atr_value * 0.10
            )

            entry_high = current + (
                atr_value * 0.20
            )

            stop = max(
                liquidity_high,
                current + (
                    atr_value * 1.20
                )
            )

            risk = stop - current

            t1 = current - (
                risk * 1.5
            )

            t2 = current - (
                risk * 2.5
            )

            entry_zone = {
                "low": round(entry_low, 2),
                "high": round(entry_high, 2),
            }

            invalidation = round(
                stop,
                2
            )

            target_1 = round(
                t1,
                2
            )

            target_2 = round(
                t2,
                2
            )

            if risk > 0:
                risk_reward = {
                    "target_1": round(
                        (current - t1) / risk,
                        2
                    ),
                    "target_2": round(
                        (current - t2) / risk,
                        2
                    ),
                }

        # WAIT setup gets potential zones, but clearly
        # marked as non-confirmed.
        else:

            if trend == "BULLISH":

                potential_entry = current

                potential_stop = (
                    current -
                    atr_value * 1.2
                )

                potential_t1 = (
                    current +
                    atr_value * 1.5
                )

                potential_t2 = (
                    current +
                    atr_value * 2.5
                )

                entry_zone = {
                    "low": round(
                        potential_entry -
                        atr_value * 0.15,
                        2
                    ),
                    "high": round(
                        potential_entry +
                        atr_value * 0.15,
                        2
                    ),
                }

                invalidation = round(
                    potential_stop,
                    2
                )

                target_1 = round(
                    potential_t1,
                    2
                )

                target_2 = round(
                    potential_t2,
                    2
                )

            elif trend == "BEARISH":

                potential_entry = current

                potential_stop = (
                    current +
                    atr_value * 1.2
                )

                potential_t1 = (
                    current -
                    atr_value * 1.5
                )

                potential_t2 = (
                    current -
                    atr_value * 2.5
                )

                entry_zone = {
                    "low": round(
                        potential_entry -
                        atr_value * 0.15,
                        2
                    ),
                    "high": round(
                        potential_entry +
                        atr_value * 0.15,
                        2
                    ),
                }

                invalidation = round(
                    potential_stop,
                    2
                )

                target_1 = round(
                    potential_t1,
                    2
                )

                target_2 = round(
                    potential_t2,
                    2
                )

    # ========================================================
    # HUMAN-READABLE EXTRA REASONS
    # ========================================================

    if signal == "WAIT":

        if setup == "BEARISH_BREAKOUT_WAIT_RETEST":

            reasons.append(
                "Breakout detected; waiting for bearish retest"
            )

        elif setup == "BULLISH_BREAKOUT_WAIT_RETEST":

            reasons.append(
                "Breakout detected; waiting for bullish retest"
            )

        else:

            reasons.append(
                "Confirmation is not strong enough for a fresh trade"
            )

    # Remove duplicate reasons
    unique_reasons = []

    for reason in reasons:

        if reason not in unique_reasons:
            unique_reasons.append(reason)

    # ========================================================
    # FINAL OBJECT
    # ========================================================

    return {
        "signal": signal,
        "confidence": confidence,
        "strength": strength,

        "setup": setup,

        "market_state": (
            "BULLISH"
            if bullish_score > bearish_score + 2
            else
            "BEARISH"
            if bearish_score > bullish_score + 2
            else
            "CONFLICTED"
        ),

        "trend": trend,
        "momentum": momentum,
        "structure": structure,

        "breakout": breakout,
        "retest": retest,
        "liquidity_sweep": sweep,

        "volatility": volatility,

        "ema20": (
            round(ema20_value, 2)
            if ema20_value is not None
            else None
        ),

        "ema50": (
            round(ema50_value, 2)
            if ema50_value is not None
            else None
        ),

        "rsi": (
            round(rsi_value, 2)
            if rsi_value is not None
            else None
        ),

        "atr": (
            round(atr_value, 2)
            if atr_value is not None
            else None
        ),

        "liquidity_high": (
            round(liquidity_high, 2)
            if liquidity_high is not None
            else None
        ),

        "liquidity_low": (
            round(liquidity_low, 2)
            if liquidity_low is not None
            else None
        ),

        "entry_zone": entry_zone,

        "invalidation": invalidation,

        "target_1": target_1,

        "target_2": target_2,

        "risk_reward": risk_reward,

        "bullish_score": bullish_score,
        "bearish_score": bearish_score,

        "chase_risk": chase_risk,

        "reasons": unique_reasons,
    }


# ============================================================
# MARKET SNAPSHOT
# ============================================================

def get_market_snapshot():

    with state_lock:

        local_candles = list(candles)

        current_price = price

        local_connected = connected
        local_subscribed = subscribed
        local_history_loaded = history_loaded
        local_error = last_error
        local_last_tick = last_tick_time

    if current_price is None and local_candles:

        current_price = safe_float(
            local_candles[-1]["close"]
        )

    analysis = build_signal(
        local_candles
    )

    return {
        "app": APP_NAME,

        "symbol": SYMBOL,

        "interval": INTERVAL,

        "price": (
            round(current_price, 2)
            if current_price is not None
            else None
        ),

        "timestamp": utc_now(),

        "connected": local_connected,

        "subscribed": local_subscribed,

        "history_loaded": local_history_loaded,

        "candle_count": len(local_candles),

        "last_error": local_error,

        "last_tick": (
            datetime.fromtimestamp(
                local_last_tick,
                tz=timezone.utc
            ).isoformat()
            if local_last_tick
            else None
        ),

        **analysis,
    }


# ============================================================
# HTML DASHBOARD
# ============================================================

HTML = r"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>Trading AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #0b1020;
    color: #f4f7ff;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 1250px;
    margin: auto;
    padding: 20px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 20px;
    flex-wrap: wrap;
    margin-bottom: 20px;
}

.logo {
    font-size: 28px;
    font-weight: bold;
}

.subtitle {
    color: #9ca7bd;
    margin-top: 5px;
}

.status {
    padding: 10px 14px;
    border-radius: 10px;
    background: #18213a;
    font-size: 13px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(210px, 1fr));
    gap: 14px;
}

.card {
    background: #121a2d;
    border: 1px solid #24304a;
    border-radius: 14px;
    padding: 18px;
}

.label {
    color: #9ca7bd;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
}

.value {
    font-size: 24px;
    font-weight: bold;
    margin-top: 8px;
}

.signal-card {
    margin-top: 18px;
    padding: 24px;
    border-radius: 16px;
    background: #121a2d;
    border: 1px solid #2b3958;
}

.signal {
    font-size: 38px;
    font-weight: bold;
    margin-bottom: 8px;
}

.confidence {
    font-size: 18px;
    color: #c5cee0;
}

.section {
    margin-top: 18px;
}

.section-title {
    font-size: 18px;
    font-weight: bold;
    margin-bottom: 10px;
}

.reasons {
    display: grid;
    gap: 8px;
}

.reason {
    background: #0d1527;
    border-radius: 9px;
    padding: 10px;
    color: #cbd4e8;
}

.trade-grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
}

.trade-box {
    background: #0d1527;
    border-radius: 10px;
    padding: 14px;
}

.small {
    color: #9ca7bd;
    font-size: 12px;
}

.error {
    color: #ff9f9f;
    margin-top: 10px;
    word-break: break-word;
}

.footer {
    color: #66728a;
    font-size: 12px;
    margin-top: 25px;
    line-height: 1.6;
}

</style>

</head>


<body>

<div class="container">

    <div class="header">

        <div>
            <div class="logo">
                TRADING AI
            </div>

            <div class="subtitle">
                Multi-Factor Market Intelligence
            </div>
        </div>

        <div class="status"
             id="status">
            Connecting...
        </div>

    </div>


    <div class="grid">

        <div class="card">
            <div class="label">Symbol</div>
            <div class="value"
                 id="symbol">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">Live Price</div>
            <div class="value"
                 id="price">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">Trend</div>
            <div class="value"
                 id="trend">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">Momentum</div>
            <div class="value"
                 id="momentum">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">Structure</div>
            <div class="value"
                 id="structure">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">RSI</div>
            <div class="value"
                 id="rsi">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">EMA 20</div>
            <div class="value"
                 id="ema20">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">EMA 50</div>
            <div class="value"
                 id="ema50">
                --
            </div>
        </div>

    </div>


    <div class="signal-card">

        <div class="label">
            FINAL SIGNAL
        </div>

        <div class="signal"
             id="signal">
            WAIT
        </div>

        <div class="confidence"
             id="confidence">
            Confidence: --
        </div>

        <div class="section">

            <div class="trade-grid">

                <div class="trade-box">
                    <div class="small">
                        Setup
                    </div>

                    <div id="setup">
                        --
                    </div>
                </div>

                <div class="trade-box">
                    <div class="small">
                        Market State
                    </div>

                    <div id="market_state">
                        --
                    </div>
                </div>

                <div class="trade-box">
                    <div class="small">
                        Breakout
                    </div>

                    <div id="breakout">
                        --
                    </div>
                </div>

                <div class="trade-box">
                    <div class="small">
                        Retest
                    </div>

                    <div id="retest">
                        --
                    </div>
                </div>

                <div class="trade-box">
                    <div class="small">
                        Liquidity Sweep
                    </div>

                    <div id="sweep">
                        --
                    </div>
                </div>

                <div class="trade-box">
                    <div class="small">
                        Volatility
                    </div>

                    <div id="volatility">
                        --
                    </div>
                </div>

            </div>

        </div>

    </div>


    <div class="section">

        <div class="section-title">
            Trade Map
        </div>

        <div class="trade-grid">

            <div class="trade-box">
                <div class="small">
                    Entry Zone
                </div>

                <div id="entry">
                    --
                </div>
            </div>

            <div class="trade-box">
                <div class="small">
                    Invalidation
                </div>

                <div id="invalid">
                    --
                </div>
            </div>

            <div class="trade-box">
                <div class="small">
                    Target 1
                </div>

                <div id="target1">
                    --
                </div>
            </div>

            <div class="trade-box">
                <div class="small">
                    Target 2
                </div>

                <div id="target2">
                    --
                </div>
            </div>

            <div class="trade-box">
                <div class="small">
                    Risk / Reward
                </div>

                <div id="rr">
                    --
                </div>
            </div>

        </div>

    </div>


    <div class="section">

        <div class="section-title">
            Engine Reasoning
        </div>

        <div class="reasons"
             id="reasons">
        </div>

    </div>


    <div class="section">

        <div class="grid">

            <div class="card">
                <div class="label">
                    Liquidity High
                </div>

                <div class="value"
                     id="liqHigh">
                    --
                </div>
            </div>

            <div class="card">
                <div class="label">
                    Liquidity Low
                </div>

                <div class="value"
                     id="liqLow">
                    --
                </div>
            </div>

            <div class="card">
                <div class="label">
                    ATR
                </div>

                <div class="value"
                     id="atr">
                    --
                </div>
            </div>

            <div class="card">
                <div class="label">
                    Candles
                </div>

                <div class="value"
                     id="candles">
                    --
                </div>
            </div>

        </div>

    </div>


    <div class="error"
         id="error">
    </div>


    <div class="footer">

        Trading AI provides market analysis and signals for
        informational purposes. It does not guarantee profits.
        Always manage risk independently.

    </div>

</div>


<script>

function value(v) {

    if (
        v === null ||
        v === undefined
    ) {
        return "--";
    }

    return v;
}


function formatEntry(zone) {

    if (!zone) {
        return "--";
    }

    return zone.low + " - " + zone.high;
}


function formatRR(rr) {

    if (!rr) {
        return "--";
    }

    return (
        "T1: " +
        rr.target_1 +
        " | T2: " +
        rr.target_2
    );
}


async function updateMarket() {

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


        document.getElementById(
            "symbol"
        ).textContent =
            value(data.symbol);


        document.getElementById(
            "price"
        ).textContent =
            value(data.price);


        document.getElementById(
            "trend"
        ).textContent =
            value(data.trend);


        document.getElementById(
            "momentum"
        ).textContent =
            value(data.momentum);


        document.getElementById(
            "structure"
        ).textContent =
            value(data.structure);


        document.getElementById(
            "rsi"
        ).textContent =
            value(data.rsi);


        document.getElementById(
            "ema20"
        ).textContent =
            value(data.ema20);


        document.getElementById(
            "ema50"
        ).textContent =
            value(data.ema50);


        document.getElementById(
            "signal"
        ).textContent =
            value(data.signal);


        document.getElementById(
            "confidence"
        ).textContent =
            "Confidence: " +
            value(data.confidence) +
            "%";


        document.getElementById(
            "setup"
        ).textContent =
            value(data.setup);


        document.getElementById(
            "market_state"
        ).textContent =
            value(data.market_state);


        document.getElementById(
            "breakout"
        ).textContent =
            value(data.breakout);


        document.getElementById(
            "retest"
        ).textContent =
            value(data.retest);


        document.getElementById(
            "sweep"
        ).textContent =
            value(data.liquidity_sweep);


        document.getElementById(
            "volatility"
        ).textContent =
            value(data.volatility);


        document.getElementById(
            "entry"
        ).textContent =
            formatEntry(
                data.entry_zone
            );


        document.getElementById(
            "invalid"
        ).textContent =
            value(data.invalidation);


        document.getElementById(
            "target1"
        ).textContent =
            value(data.target_1);


        document.getElementById(
            "target2"
        ).textContent =
            value(data.target_2);


        document.getElementById(
            "rr"
        ).textContent =
            formatRR(
                data.risk_reward
            );


        document.getElementById(
            "liqHigh"
        ).textContent =
            value(data.liquidity_high);


        document.getElementById(
            "liqLow"
        ).textContent =
            value(data.liquidity_low);


        document.getElementById(
            "atr"
        ).textContent =
            value(data.atr);


        document.getElementById(
            "candles"
        ).textContent =
            value(data.candle_count);


        const reasons =
            document.getElementById(
                "reasons"
            );

        reasons.innerHTML = "";


        if (
            data.reasons &&
            data.reasons.length
        ) {

            data.reasons.forEach(
                function(reason) {

                    const div =
                        document.createElement(
                            "div"
                        );

                    div.className =
                        "reason";

                    div.textContent =
                        reason;

                    reasons.appendChild(
                        div
                    );

                }
            );

        }


        const status =
            document.getElementById(
                "status"
            );


        if (
            data.connected &&
            data.subscribed
        ) {

            status.textContent =
                "LIVE • WebSocket Connected";

        } else if (
            data.history_loaded
        ) {

            status.textContent =
                "HISTORY LIVE • Reconnecting WebSocket";

        } else {

            status.textContent =
                "CONNECTING...";

        }


        const error =
            document.getElementById(
                "error"
            );

        if (data.last_error) {

            error.textContent =
                "Engine notice: " +
                data.last_error;

        } else {

            error.textContent = "";

        }

    }

    catch (error) {

        document.getElementById(
            "status"
        ).textContent =
            "Dashboard connection error";

        document.getElementById(
            "error"
        ).textContent =
            error.toString();

    }

}


updateMarket();

setInterval(
    updateMarket,
    2000
);

</script>

</body>

</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "app": APP_NAME,
        "symbol": SYMBOL,
        "history_loaded": history_loaded,
        "connected": connected,
        "subscribed": subscribed,
        "candles": len(candles),
        "timestamp": utc_now(),
    })


@app.route("/api/market")
def api_market():

    return jsonify(
        get_market_snapshot()
    )


@app.route("/api/candles")
def api_candles():

    with state_lock:
        data = list(candles)

    return jsonify({
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "count": len(data),
        "candles": data,
    })


# ============================================================
# ENGINE START
# ============================================================

def start_engine():

    log("=" * 60)
    log("TRADING AI ENGINE STARTING")
    log("=" * 60)

    if TWELVE_DATA_API_KEY:

        log("API KEY PRESENT: True")
        log(
            f"API KEY LENGTH: "
            f"{len(TWELVE_DATA_API_KEY)}"
        )

    else:

        log("API KEY PRESENT: False")
        log("API KEY LENGTH: 0")

    # Load history once
    load_history(
        force=True
    )

    # Start websocket
    ws_thread = threading.Thread(
        target=websocket_worker,
        daemon=True,
    )

    ws_thread.start()

    log("WebSocket worker started")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_engine()

    log(
        f"Starting Flask on port {PORT}"
    )

    app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True,
        use_reloader=False,
    )
