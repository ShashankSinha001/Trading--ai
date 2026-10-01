import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string


# ============================================================
# TRADING AI
# XAU/USD GOLD
# LIVE WEBSOCKET + 5M HISTORY
# MULTI-FACTOR ANALYSIS
# ============================================================

APP_NAME = "Trading AI"

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

SYMBOL = "XAU/USD"
INTERVAL = "5min"

REST_URL = "https://api.twelvedata.com"
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

HISTORY_SIZE = 300

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.RLock()

candles = []

live_price = None
live_timestamp = None

ws_connected = False
ws_subscribed = False

last_ws_message = None
last_error = None

history_loaded = False

api_blocked_until = 0

workers_started = False


# ============================================================
# LOGGING
# ============================================================

def log(message):
    print(f"[TRADING-AI] {message}", flush=True)


# ============================================================
# NUMBER HELPERS
# ============================================================

def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def safe_round(value, digits=3):
    if value is None:
        return None

    try:
        return round(float(value), digits)
    except Exception:
        return None


# ============================================================
# REST API
# ============================================================

def td_get(endpoint, params=None):
    global api_blocked_until
    global last_error

    if not API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is missing")

    now = time.time()

    if now < api_blocked_until:
        remaining = int(api_blocked_until - now)
        raise RuntimeError(
            f"Twelve Data temporarily blocked after 429. "
            f"Retry in {remaining}s."
        )

    params = dict(params or {})
    params["apikey"] = API_KEY

    url = f"{REST_URL}/{endpoint}"

    try:
        response = requests.get(
            url,
            params=params,
            timeout=20
        )

        if response.status_code == 429:
            api_blocked_until = time.time() + 3600

            raise RuntimeError(
                f"Twelve Data HTTP 429: {response.text[:500]}"
            )

        response.raise_for_status()

        data = response.json()

        if isinstance(data, dict):
            if data.get("status") == "error":
                raise RuntimeError(
                    f"Twelve Data error: {data}"
                )

        return data

    except Exception as exc:
        last_error = str(exc)
        raise


# ============================================================
# HISTORY
# ============================================================

def load_history():
    global candles
    global history_loaded
    global last_error

    log("Loading XAU/USD 5-minute history...")

    try:
        data = td_get(
            "time_series",
            {
                "symbol": SYMBOL,
                "interval": INTERVAL,
                "outputsize": HISTORY_SIZE,
                "timezone": "UTC",
                "order": "asc",
            }
        )

        values = data.get("values", [])

        parsed = []

        for item in values:
            dt = item.get("datetime")

            o = to_float(item.get("open"))
            h = to_float(item.get("high"))
            l = to_float(item.get("low"))
            c = to_float(item.get("close"))

            if not dt or None in (o, h, l, c):
                continue

            parsed.append(
                {
                    "datetime": dt,
                    "timestamp": parse_datetime(dt),
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": to_float(item.get("volume")) or 0.0,
                }
            )

        if not parsed:
            raise RuntimeError(
                "Twelve Data returned no usable candles."
            )

        with state_lock:
            candles = parsed[-HISTORY_SIZE:]
            history_loaded = True

        log(
            f"History loaded successfully: "
            f"{len(candles)} candles"
        )

        last = candles[-1]

        log(
            f"LAST HISTORY CANDLE: "
            f"{last['datetime']} "
            f"close={last['close']}"
        )

    except Exception as exc:
        history_loaded = False
        last_error = str(exc)

        log(f"HISTORY ERROR: {repr(exc)}")


# ============================================================
# DATETIME
# ============================================================

def parse_datetime(value):
    try:
        text = str(value).strip()

        if text.endswith("Z"):
            text = text[:-1]

        if "T" in text:
            dt = datetime.fromisoformat(text)
        else:
            dt = datetime.strptime(
                text,
                "%Y-%m-%d %H:%M:%S"
            )

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.timestamp()

    except Exception:
        return time.time()


# ============================================================
# 5 MINUTE BUCKET
# ============================================================

def candle_bucket(timestamp):
    return int(timestamp // 300) * 300


def bucket_datetime(timestamp):
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# LIVE CANDLE UPDATE
# ============================================================

def update_live_candle(price, timestamp=None):
    global live_price
    global live_timestamp
    global candles

    price = to_float(price)

    if price is None or price <= 0:
        return False

    if timestamp is None:
        timestamp = time.time()

    bucket = candle_bucket(timestamp)

    with state_lock:

        live_price = price
        live_timestamp = timestamp

        if not candles:

            candles.append(
                {
                    "datetime": bucket_datetime(bucket),
                    "timestamp": bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0.0,
                }
            )

            return True

        last = candles[-1]

        last_bucket = candle_bucket(
            last.get("timestamp", timestamp)
        )

        # ----------------------------------------------------
        # SAME 5 MINUTE CANDLE
        # ----------------------------------------------------

        if bucket == last_bucket:

            last["high"] = max(
                float(last["high"]),
                price
            )

            last["low"] = min(
                float(last["low"]),
                price
            )

            last["close"] = price

        # ----------------------------------------------------
        # NEW 5 MINUTE CANDLE
        # ----------------------------------------------------

        elif bucket > last_bucket:

            new_candle = {
                "datetime": bucket_datetime(bucket),
                "timestamp": bucket,
                "open": last["close"],
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
            }

            candles.append(new_candle)

            if len(candles) > HISTORY_SIZE:
                candles = candles[-HISTORY_SIZE:]

        # ----------------------------------------------------
        # OUT OF ORDER MESSAGE
        # ----------------------------------------------------

        else:
            return False

    return True


# ============================================================
# WEBSOCKET MESSAGE PARSER
# ============================================================

def process_ws_message(raw_message):
    global last_ws_message
    global last_error
    global ws_subscribed

    last_ws_message = raw_message

    try:
        data = json.loads(raw_message)

    except Exception as exc:
        last_error = f"WS JSON parse error: {exc}"
        log(f"WEBSOCKET JSON ERROR: {raw_message[:500]}")
        return

    if not isinstance(data, dict):
        return

    event = data.get("event")

    # ========================================================
    # SUBSCRIPTION STATUS
    # ========================================================

    if event == "subscribe-status":

        status = data.get("status")

        log(
            f"SUBSCRIBE STATUS: {json.dumps(data)[:800]}"
        )

        if status in ("ok", "success"):
            ws_subscribed = True

        return

    # ========================================================
    # PRICE EVENT
    # ========================================================

    if event == "price":

        price = (
            data.get("price")
            or data.get("close")
        )

        timestamp = (
            data.get("timestamp")
            or data.get("ts")
            or time.time()
        )

        timestamp = to_float(timestamp)

        # Some APIs can return milliseconds.
        if timestamp and timestamp > 10_000_000_000:
            timestamp = timestamp / 1000.0

        price = to_float(price)

        if price is None:
            log(
                "PRICE EVENT RECEIVED BUT PRICE IS MISSING: "
                + str(data)[:1000]
            )
            return

        log(
            f"PRICE RECEIVED: "
            f"{SYMBOL} = {price}"
        )

        ok = update_live_candle(
            price,
            timestamp
        )

        if ok:
            log(
                f"LIVE CANDLE UPDATED: "
                f"{price}"
            )

        return

    # ========================================================
    # ERROR EVENT
    # ========================================================

    if event == "error":

        last_error = str(data)

        log(
            f"WEBSOCKET SERVER ERROR: "
            f"{json.dumps(data)[:1000]}"
        )

        return

    # ========================================================
    # HEARTBEAT / OTHER
    # ========================================================

    if event in ("heartbeat", "hearbeat"):
        return

    # ========================================================
    # UNKNOWN MESSAGE
    # ========================================================

    if "price" in data:

        price = to_float(data.get("price"))

        if price is not None:

            log(
                f"PRICE RECEIVED (fallback): "
                f"{SYMBOL} = {price}"
            )

            update_live_candle(
                price,
                time.time()
            )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def websocket_open(ws):
    global ws_connected
    global ws_subscribed
    global last_error

    ws_connected = True
    ws_subscribed = False
    last_error = None

    log("WebSocket connected")

    subscribe_message = {
        "action": "subscribe",
        "params": {
            "symbols": SYMBOL
        }
    }

    try:

        ws.send(
            json.dumps(subscribe_message)
        )

        log(
            f"Subscribed to {SYMBOL}"
        )

    except Exception as exc:

        last_error = str(exc)

        log(
            f"WEBSOCKET SUBSCRIBE ERROR: "
            f"{repr(exc)}"
        )


# ============================================================
# WEBSOCKET CLOSE
# ============================================================

def websocket_close(ws, close_status_code, close_msg):
    global ws_connected
    global ws_subscribed

    ws_connected = False
    ws_subscribed = False

    log(
        f"WebSocket closed: "
        f"{close_status_code} "
        f"{close_msg}"
    )


# ============================================================
# WEBSOCKET ERROR
# ============================================================

def websocket_error(ws, error):
    global ws_connected
    global last_error

    ws_connected = False
    last_error = str(error)

    log(
        f"WEBSOCKET ERROR: {repr(error)}"
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    global ws_connected
    global ws_subscribed

    if not API_KEY:

        log(
            "WEBSOCKET NOT STARTED: "
            "TWELVE_DATA_API_KEY missing"
        )

        return

    log(
        "Starting WebSocket for "
        f"{SYMBOL} "
        f"(API key present: yes, "
        f"length: {len(API_KEY)})"
    )

    while True:

        try:

            ws_url = (
                f"{WS_URL}"
                f"?apikey={API_KEY}"
            )

            ws = websocket.WebSocketApp(
                ws_url,

                on_open=websocket_open,

                on_message=lambda ws, message:
                    process_ws_message(message),

                on_error=websocket_error,

                on_close=websocket_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                ping_payload="ping",
            )

        except Exception as exc:

            ws_connected = False
            ws_subscribed = False

            log(
                f"WEBSOCKET LOOP ERROR: "
                f"{repr(exc)}"
            )

        ws_connected = False
        ws_subscribed = False

        log(
            "WebSocket reconnecting in 10 seconds..."
        )

        time.sleep(10)


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(
        values[:period]
    ) / period

    for price in values[period:]:

        result = (
            (price - result) * multiplier
        ) + result

    return result


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        diff = (
            values[i] -
            values[i - 1]
        )

        gains.append(
            max(diff, 0)
        )

        losses.append(
            max(-diff, 0)
        )

    avg_gain = (
        sum(gains[:period]) /
        period
    )

    avg_loss = (
        sum(losses[:period]) /
        period
    )

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

    return 100 - (
        100 / (1 + rs)
    )


def atr(candle_data, period=14):

    if len(candle_data) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(candle_data)):

        current = candle_data[i]
        previous = candle_data[i - 1]

        high = float(current["high"])
        low = float(current["low"])
        prev_close = float(previous["close"])

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return (
        sum(true_ranges[-period:]) /
        period
    )


# ============================================================
# MARKET ANALYSIS
# ============================================================

def calculate_analysis():

    with state_lock:

        data = list(candles)
        price = live_price

    if len(data) < 55:

        return {
            "signal": "WAIT",
            "strength": 0,
            "trend": "WAIT",
            "momentum": "WAIT",
            "structure": "INSUFFICIENT",
            "rsi": None,
            "ema20": None,
            "ema50": None,
            "atr": None,
            "liquidity_high": None,
            "liquidity_low": None,
            "breakout": "WAIT",
            "volatility": "WAIT",
            "candle_count": len(data),
        }

    closes = [
        float(x["close"])
        for x in data
    ]

    highs = [
        float(x["high"])
        for x in data
    ]

    lows = [
        float(x["low"])
        for x in data
    ]

    ema20 = ema(
        closes,
        20
    )

    ema50 = ema(
        closes,
        50
    )

    rsi14 = rsi(
        closes,
        14
    )

    atr14 = atr(
        data,
        14
    )

    current_price = (
        price
        if price is not None
        else closes[-1]
    )

    # ========================================================
    # TREND
    # ========================================================

    trend = "WAIT"

    if ema20 is not None and ema50 is not None:

        if (
            current_price > ema20
            and ema20 > ema50
        ):
            trend = "BULLISH"

        elif (
            current_price < ema20
            and ema20 < ema50
        ):
            trend = "BEARISH"

        elif current_price > ema50:
            trend = "MIXED-UP"

        elif current_price < ema50:
            trend = "MIXED-DOWN"

    # ========================================================
    # MOMENTUM
    # ========================================================

    momentum = "WAIT"

    if rsi14 is not None:

        if rsi14 >= 60:
            momentum = "BULLISH"

        elif rsi14 <= 40:
            momentum = "BEARISH"

        else:
            momentum = "NEUTRAL"

    # ========================================================
    # MARKET STRUCTURE
    # ========================================================

    recent = data[-20:]

    recent_high = max(
        float(x["high"])
        for x in recent
    )

    recent_low = min(
        float(x["low"])
        for x in recent
    )

    previous = data[-10:-1]

    previous_high = max(
        float(x["high"])
        for x in previous
    )

    previous_low = min(
        float(x["low"])
        for x in previous
    )

    if (
        current_price > recent_high
        and current_price > previous_high
    ):
        structure = "BREAKOUT-UP"

    elif (
        current_price < recent_low
        and current_price < previous_low
    ):
        structure = "BREAKOUT-DOWN"

    elif (
        recent_high > previous_high
        and recent_low > previous_low
    ):
        structure = "HIGHER-HIGH / LOW"

    elif (
        recent_high < previous_high
        and recent_low < previous_low
    ):
        structure = "LOWER-HIGH / LOW"

    else:
        structure = "RANGE"

    # ========================================================
    # LIQUIDITY
    # ========================================================

    liquidity_high = max(
        highs[-50:]
    )

    liquidity_low = min(
        lows[-50:]
    )

    # ========================================================
    # BREAKOUT
    # ========================================================

    breakout = "WAIT"

    if current_price > liquidity_high:
        breakout = "UP"

    elif current_price < liquidity_low:
        breakout = "DOWN"

    # ========================================================
    # VOLATILITY
    # ========================================================

    volatility = "NORMAL"

    if atr14 is not None:

        last_range = (
            highs[-1] -
            lows[-1]
        )

        if last_range > atr14 * 1.8:
            volatility = "HIGH"

        elif last_range < atr14 * 0.5:
            volatility = "LOW"

    # ========================================================
    # MULTI-FACTOR SCORE
    # ========================================================

    buy_score = 0
    sell_score = 0

    # Trend
    if trend == "BULLISH":
        buy_score += 25

    elif trend == "BEARISH":
        sell_score += 25

    elif trend == "MIXED-UP":
        buy_score += 10

    elif trend == "MIXED-DOWN":
        sell_score += 10

    # Momentum
    if momentum == "BULLISH":
        buy_score += 20

    elif momentum == "BEARISH":
        sell_score += 20

    # Structure
    if (
        structure == "BREAKOUT-UP"
        or structure == "HIGHER-HIGH / LOW"
    ):
        buy_score += 25

    elif (
        structure == "BREAKOUT-DOWN"
        or structure == "LOWER-HIGH / LOW"
    ):
        sell_score += 25

    # RSI confirmation
    if rsi14 is not None:

        if 55 <= rsi14 <= 70:
            buy_score += 15

        elif 30 <= rsi14 <= 45:
            sell_score += 15

        # Avoid blindly chasing extreme RSI
        elif rsi14 > 75:
            buy_score -= 10

        elif rsi14 < 25:
            sell_score -= 10

    # Price vs EMA50
    if ema50 is not None:

        if current_price > ema50:
            buy_score += 10

        elif current_price < ema50:
            sell_score += 10

    # ========================================================
    # CONFLICT FILTER
    # ========================================================

    difference = abs(
        buy_score - sell_score
    )

    strength = max(
        buy_score,
        sell_score
    )

    signal = "WAIT"

    # Need enough agreement between factors
    if (
        buy_score >= 60
        and difference >= 20
    ):
        signal = "BUY"

    elif (
        sell_score >= 60
        and difference >= 20
    ):
        signal = "SELL"

    else:
        signal = "WAIT"

    return {
        "signal": signal,
        "strength": int(
            max(
                0,
                min(
                    100,
                    strength
                )
            )
        ),

        "buy_score": buy_score,
        "sell_score": sell_score,

        "trend": trend,
        "momentum": momentum,
        "structure": structure,

        "rsi": safe_round(
            rsi14,
            2
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
            atr14,
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

        "breakout": breakout,
        "volatility": volatility,

        "candle_count": len(data),
    }


# ============================================================
# MARKET API
# ============================================================

@app.route("/api/market")
def market_api():

    analysis = calculate_analysis()

    with state_lock:

        price = live_price

        result = {
            "app": APP_NAME,
            "symbol": SYMBOL,

            "price": safe_round(
                price,
                3
            ),

            "timestamp": live_timestamp,

            "connected": ws_connected,

            "subscribed": ws_subscribed,

            "history_loaded": history_loaded,

            "last_error": last_error,

            **analysis,
        }

    return jsonify(result)


# ============================================================
# CANDLES API
# ============================================================

@app.route("/api/candles")
def candles_api():

    with state_lock:

        result = []

        for candle in candles:

            result.append(
                {
                    "datetime": candle.get(
                        "datetime"
                    ),

                    "open": candle.get(
                        "open"
                    ),

                    "high": candle.get(
                        "high"
                    ),

                    "low": candle.get(
                        "low"
                    ),

                    "close": candle.get(
                        "close"
                    ),

                    "volume": candle.get(
                        "volume",
                        0
                    ),
                }
            )

    return jsonify(
        {
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "count": len(result),
            "candles": result,
        }
    )


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    return jsonify(
        {
            "status": "ok",
            "app": APP_NAME,
            "symbol": SYMBOL,
            "api_key_present": bool(API_KEY),
            "api_key_length": len(API_KEY),
            "ws_connected": ws_connected,
            "ws_subscribed": ws_subscribed,
            "history_loaded": history_loaded,
            "candle_count": len(candles),
            "live_price": live_price,
            "last_error": last_error,
        }
    )


# ============================================================
# DASHBOARD
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
    background: #07111f;
    color: #eaf2ff;
    font-family: Arial, Helvetica, sans-serif;
}

.container {
    max-width: 1200px;
    margin: auto;
    padding: 24px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 22px;
}

.logo {
    font-size: 28px;
    font-weight: 800;
}

.symbol {
    color: #9eb2cc;
    margin-top: 5px;
}

.connection {
    padding: 8px 14px;
    border-radius: 20px;
    background: #18263a;
    font-weight: bold;
    font-size: 13px;
}

.price-card {
    background: #0c1a2b;
    border: 1px solid #1e3149;
    border-radius: 18px;
    padding: 28px;
    margin-bottom: 18px;
}

.label {
    color: #91a4bc;
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.price {
    font-size: 52px;
    font-weight: 800;
    margin-top: 8px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(180px, 1fr));
    gap: 14px;
}

.card {
    background: #0c1a2b;
    border: 1px solid #1e3149;
    border-radius: 16px;
    padding: 20px;
}

.card-title {
    color: #91a4bc;
    font-size: 12px;
    text-transform: uppercase;
}

.card-value {
    font-size: 22px;
    font-weight: 700;
    margin-top: 10px;
}

.signal {
    font-size: 34px;
    font-weight: 900;
}

.wait {
    color: #d7dee8;
}

.small {
    color: #8195ad;
    font-size: 12px;
    margin-top: 7px;
}

.status {
    margin-top: 20px;
    color: #91a4bc;
    font-size: 13px;
}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <div>

            <div class="logo">
                Trading AI
            </div>

            <div class="symbol">
                XAU/USD • GOLD
            </div>

        </div>

        <div
            id="connection"
            class="connection">
            CONNECTING
        </div>

    </div>


    <div class="price-card">

        <div class="label">
            Live Gold Price
        </div>

        <div
            id="price"
            class="price">
            --
        </div>

        <div
            id="priceStatus"
            class="small">
            Waiting for live candle data...
        </div>

    </div>


    <div class="card"
         style="margin-bottom:18px;">

        <div class="label">
            AI Market Signal
        </div>

        <div
            id="signal"
            class="signal wait">
            WAIT
        </div>

        <div
            id="strength"
            class="small">
            Strength: 0%
        </div>

    </div>


    <div class="grid">

        <div class="card">

            <div class="card-title">
                Trend
            </div>

            <div
                id="trend"
                class="card-value">
                WAIT
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                Momentum
            </div>

            <div
                id="momentum"
                class="card-value">
                WAIT
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                Structure
            </div>

            <div
                id="structure"
                class="card-value">
                INSUFFICIENT
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                RSI
            </div>

            <div
                id="rsi"
                class="card-value">
                --
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                EMA 20
            </div>

            <div
                id="ema20"
                class="card-value">
                --
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                EMA 50
            </div>

            <div
                id="ema50"
                class="card-value">
                --
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                Liquidity High
            </div>

            <div
                id="liqHigh"
                class="card-value">
                --
            </div>

        </div>


        <div class="card">

            <div class="card-title">
                Liquidity Low
            </div>

            <div
                id="liqLow"
                class="card-value">
                --
            </div>

        </div>

    </div>


    <div
        id="status"
        class="status">
        Initializing market engine...
    </div>

</div>


<script>

function setText(id, value) {

    const el = document.getElementById(id);

    if (el) {
        el.textContent =
            value === null ||
            value === undefined
            ? "--"
            : value;
    }
}


function updateMarket(data) {

    const connected =
        data.connected === true;

    const subscribed =
        data.subscribed === true;

    const connection =
        document.getElementById(
            "connection"
        );

    if (connected && subscribed) {

        connection.textContent =
            "LIVE";

    } else if (connected) {

        connection.textContent =
            "CONNECTED";

    } else {

        connection.textContent =
            "RECONNECTING";
    }


    setText(
        "price",
        data.price !== null &&
        data.price !== undefined
            ? Number(data.price).toFixed(3)
            : "--"
    );


    setText(
        "signal",
        data.signal || "WAIT"
    );


    setText(
        "strength",
        "Strength: " +
        (data.strength || 0) +
        "%"
    );


    setText(
        "trend",
        data.trend || "WAIT"
    );


    setText(
        "momentum",
        data.momentum || "WAIT"
    );


    setText(
        "structure",
        data.structure ||
        "INSUFFICIENT"
    );


    setText(
        "rsi",
        data.rsi !== null &&
        data.rsi !== undefined
            ? Number(data.rsi).toFixed(2)
            : "--"
    );


    setText(
        "ema20",
        data.ema20 !== null &&
        data.ema20 !== undefined
            ? Number(data.ema20).toFixed(3)
            : "--"
    );


    setText(
        "ema50",
        data.ema50 !== null &&
        data.ema50 !== undefined
            ? Number(data.ema50).toFixed(3)
            : "--"
    );


    setText(
        "liqHigh",
        data.liquidity_high !== null &&
        data.liquidity_high !== undefined
            ? Number(
                data.liquidity_high
              ).toFixed(3)
            : "--"
    );


    setText(
        "liqLow",
        data.liquidity_low !== null &&
        data.liquidity_low !== undefined
            ? Number(
                data.liquidity_low
              ).toFixed(3)
            : "--"
    );


    const priceStatus =
        document.getElementById(
            "priceStatus"
        );

    if (data.price !== null &&
        data.price !== undefined) {

        priceStatus.textContent =
            "Live price feed active";

    } else if (data.history_loaded) {

        priceStatus.textContent =
            "History loaded • waiting for live tick";

    } else {

        priceStatus.textContent =
            "Waiting for live candle data...";
    }


    let status =
        "Candles: " +
        (data.candle_count || 0);


    if (data.last_error) {

        status +=
            " • Last error: " +
            data.last_error;
    }

    document.getElementById(
        "status"
    ).textContent = status;
}


async function loadMarket() {

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

        updateMarket(data);

    } catch (error) {

        document.getElementById(
            "connection"
        ).textContent =
            "API ERROR";

        document.getElementById(
            "status"
        ).textContent =
            error.toString();
    }
}


loadMarket();

setInterval(
    loadMarket,
    2000
);

</script>

</body>

</html>
"""


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    return render_template_string(
        HTML
    )


# ============================================================
# START BACKGROUND WORKERS
# ============================================================

def start_workers():

    global workers_started

    if workers_started:
        return

    workers_started = True

    log("")
    log("======================================")
    log("TRADING AI STARTING")
    log("======================================")

    log(
        f"API KEY PRESENT: {bool(API_KEY)}"
    )

    log(
        f"API KEY LENGTH: {len(API_KEY)}"
    )

    # --------------------------------------------------------
    # HISTORY THREAD
    # --------------------------------------------------------

    history_thread = threading.Thread(
        target=load_history,
        daemon=True
    )

    history_thread.start()

    # --------------------------------------------------------
    # WEBSOCKET THREAD
    # --------------------------------------------------------

    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    websocket_thread.start()

    log("Background workers started")


# ============================================================
# START WORKERS AT IMPORT
# ============================================================

start_workers()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
