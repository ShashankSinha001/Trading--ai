import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, Response


# ============================================================
# TRADING AI - LIVE SINGLE PROCESS MARKET ENGINE
# Goal:
# Better information + fewer false signals
#
# Market:
# XAU/USD
#
# Data:
# Twelve Data REST + WebSocket
#
# Indicators:
# EMA 20
# EMA 50
# RSI 14
# ATR 14
#
# Analysis:
# Multi-factor trend
# Momentum
# Market structure
# Liquidity
# Breakout
# Volatility
# Conflict filtering
# Signal quality
#
# Output:
# BUY / SELL / WAIT
# ============================================================


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

SYMBOL = "XAU/USD"
INTERVAL = "5min"

REST_BASE = "https://api.twelvedata.com"
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

PORT = int(os.getenv("PORT", "10000"))


# ============================================================
# SHARED STATE
# ============================================================

state_lock = threading.RLock()

candles = []

live_price = None
live_timestamp = None

ws_connected = False
ws_subscribed = False

history_loaded = False

last_error = None
last_ws_message = None

engine_started = False


# ============================================================
# LOGGING
# ============================================================

def log(message):
    print(f"[TRADING-AI] {message}", flush=True)


# ============================================================
# TIME
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def candle_bucket(timestamp=None):
    """
    Returns the 5-minute bucket timestamp.
    """
    if timestamp is None:
        timestamp = time.time()

    bucket = int(timestamp // 300) * 300
    return bucket


# ============================================================
# API KEY CHECK
# ============================================================

def check_api_key():
    if not API_KEY:
        log("ERROR: TWELVE_DATA_API_KEY is missing")
        return False

    log("API KEY PRESENT: True")
    log(f"API KEY LENGTH: {len(API_KEY)}")

    return True


# ============================================================
# LOAD HISTORICAL DATA
# ============================================================

def load_history():
    global candles
    global history_loaded
    global last_error

    log("Loading historical XAU/USD 5-minute candles...")

    if not API_KEY:
        last_error = "TWELVE_DATA_API_KEY missing"
        return False

    url = f"{REST_BASE}/time_series"

    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "outputsize": 300,
        "apikey": API_KEY,
        "format": "JSON",
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=20,
        )

        if response.status_code != 200:
            error_text = response.text[:1000]

            last_error = (
                f"Twelve Data HTTP {response.status_code}: "
                f"{error_text}"
            )

            log(f"REST HISTORY ERROR: {last_error}")

            return False

        data = response.json()

        if "status" in data and data["status"] == "error":
            last_error = str(data)

            log(f"REST HISTORY ERROR: {last_error}")

            return False

        values = data.get("values", [])

        if not values:
            last_error = "Twelve Data returned no historical candles"

            log(last_error)

            return False

        parsed = []

        for item in reversed(values):

            try:
                timestamp_string = item.get("datetime")

                if timestamp_string:

                    try:
                        dt = datetime.fromisoformat(
                            timestamp_string.replace("Z", "+00:00")
                        )

                        timestamp = dt.timestamp()

                    except Exception:
                        timestamp = time.time()

                else:
                    timestamp = time.time()

                parsed.append(
                    {
                        "timestamp": timestamp,
                        "datetime": timestamp_string,
                        "open": float(item["open"]),
                        "high": float(item["high"]),
                        "low": float(item["low"]),
                        "close": float(item["close"]),
                        "volume": float(item.get("volume", 0) or 0),
                    }
                )

            except Exception as exc:
                log(f"Skipping malformed candle: {exc}")

        with state_lock:

            candles = parsed[-300:]

            history_loaded = len(candles) > 0

            last_error = None

        log(
            f"History loaded successfully: "
            f"{len(candles)} candles"
        )

        return True

    except Exception as exc:

        last_error = f"History exception: {repr(exc)}"

        log(f"REST HISTORY EXCEPTION: {repr(exc)}")

        return False


# ============================================================
# LIVE CANDLE UPDATE
# ============================================================

def update_live_candle(price, timestamp=None):

    global candles
    global live_price
    global live_timestamp

    if timestamp is None:
        timestamp = time.time()

    try:
        price = float(price)
    except Exception:
        return

    bucket = candle_bucket(timestamp)

    with state_lock:

        live_price = price
        live_timestamp = timestamp

        if not candles:

            candles.append(
                {
                    "timestamp": bucket,
                    "datetime": datetime.fromtimestamp(
                        bucket,
                        timezone.utc
                    ).isoformat(),
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0,
                }
            )

            return

        last = candles[-1]

        last_bucket = candle_bucket(
            last.get("timestamp", timestamp)
        )

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

        elif bucket > last_bucket:

            previous_close = float(last["close"])

            candles.append(
                {
                    "timestamp": bucket,
                    "datetime": datetime.fromtimestamp(
                        bucket,
                        timezone.utc
                    ).isoformat(),
                    "open": previous_close,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0,
                }
            )

            candles = candles[-300:]


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def websocket_message(ws, message):

    global ws_subscribed
    global last_ws_message
    global last_error

    try:

        last_ws_message = message[:2000]

        data = json.loads(message)

        event = data.get("event")

        # ----------------------------------------------------
        # SUBSCRIBE STATUS
        # ----------------------------------------------------

        if event == "subscribe-status":

            status = data.get("status")

            log(
                f"SUBSCRIBE STATUS: {status}"
            )

            if status == "ok":

                ws_subscribed = True

                last_error = None

                log(
                    f"Subscribed to {SYMBOL}"
                )

            return

        # ----------------------------------------------------
        # ERROR
        # ----------------------------------------------------

        if event == "error":

            last_error = str(data)

            log(
                f"WEBSOCKET ERROR: {data}"
            )

            return

        # ----------------------------------------------------
        # PRICE
        # ----------------------------------------------------

        if event == "price":

            symbol = data.get("symbol")

            price = (
                data.get("price")
                or data.get("close")
            )

            if price is not None:

                price = float(price)

                timestamp = time.time()

                if data.get("timestamp"):

                    try:
                        timestamp = float(
                            data["timestamp"]
                        )

                    except Exception:
                        pass

                log(
                    f"PRICE RECEIVED: "
                    f"{symbol or SYMBOL} = {price}"
                )

                update_live_candle(
                    price,
                    timestamp
                )

                log(
                    f"LIVE CANDLE UPDATED: {price}"
                )

                return

        # ----------------------------------------------------
        # FALLBACK PRICE DETECTION
        # ----------------------------------------------------

        if "price" in data:

            price = data.get("price")

            if price is not None:

                price = float(price)

                log(
                    f"PRICE RECEIVED: "
                    f"{data.get('symbol', SYMBOL)} = {price}"
                )

                update_live_candle(
                    price,
                    time.time()
                )

                log(
                    f"LIVE CANDLE UPDATED: {price}"
                )

    except Exception as exc:

        last_error = (
            f"WebSocket message exception: "
            f"{repr(exc)}"
        )

        log(
            f"WEBSOCKET MESSAGE EXCEPTION: "
            f"{repr(exc)}"
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
            f"Subscription request sent for {SYMBOL}"
        )

    except Exception as exc:

        last_error = (
            f"Subscribe send error: {repr(exc)}"
        )

        log(
            f"SUBSCRIBE SEND ERROR: {repr(exc)}"
        )


# ============================================================
# WEBSOCKET CLOSE
# ============================================================

def websocket_close(
    ws,
    close_status_code,
    close_msg
):

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

    last_error = (
        f"WebSocket error: {repr(error)}"
    )

    log(
        f"WEBSOCKET ERROR: {repr(error)}"
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

    global ws_connected
    global ws_subscribed

    while True:

        try:

            log(
                "Connecting to Twelve Data WebSocket..."
            )

            ws = websocket.WebSocketApp(
                f"{WS_URL}?apikey={API_KEY}",

                on_open=websocket_open,

                on_message=websocket_message,

                on_error=websocket_error,

                on_close=websocket_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:

            ws_connected = False
            ws_subscribed = False

            log(
                f"WebSocket loop exception: "
                f"{repr(exc)}"
            )

        log(
            "WebSocket reconnecting in 5 seconds..."
        )

        time.sleep(5)


# ============================================================
# INDICATORS
# ============================================================

def closes():

    with state_lock:

        return [
            float(c["close"])
            for c in candles
            if "close" in c
        ]


def ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    current = sum(
        values[:period]
    ) / period

    for price in values[period:]:

        current = (
            (price - current)
            * multiplier
            + current
        )

    return current


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i]
            - values[i - 1]
        )

        if change > 0:

            gains.append(change)
            losses.append(0)

        else:

            gains.append(0)
            losses.append(abs(change))

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
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
        previous_close = float(
            previous["close"]
        )

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return (
        sum(true_ranges[-period:])
        / period
    )


# ============================================================
# MARKET STRUCTURE
# ============================================================

def market_structure(candle_data):

    if len(candle_data) < 10:
        return "INSUFFICIENT"

    recent = candle_data[-10:]

    highs = [
        float(c["high"])
        for c in recent
    ]

    lows = [
        float(c["low"])
        for c in recent
    ]

    first_half = recent[:5]
    second_half = recent[5:]

    first_high = max(
        float(c["high"])
        for c in first_half
    )

    second_high = max(
        float(c["high"])
        for c in second_half
    )

    first_low = min(
        float(c["low"])
        for c in first_half
    )

    second_low = min(
        float(c["low"])
        for c in second_half
    )

    if (
        second_high > first_high
        and second_low > first_low
    ):
        return "BULLISH_STRUCTURE"

    if (
        second_high < first_high
        and second_low < first_low
    ):
        return "BEARISH_STRUCTURE"

    return "RANGE"


# ============================================================
# LIQUIDITY
# ============================================================

def liquidity_levels(candle_data):

    if len(candle_data) < 20:
        return None, None

    previous = candle_data[-21:-1]

    highs = [
        float(c["high"])
        for c in previous
    ]

    lows = [
        float(c["low"])
        for c in previous
    ]

    return max(highs), min(lows)


# ============================================================
# BREAKOUT
# ============================================================

def breakout_status(
    price,
    liquidity_high,
    liquidity_low
):

    if price is None:
        return "WAIT"

    if (
        liquidity_high is not None
        and price > liquidity_high
    ):
        return "BULLISH_BREAKOUT"

    if (
        liquidity_low is not None
        and price < liquidity_low
    ):
        return "BEARISH_BREAKOUT"

    return "WAIT"


# ============================================================
# TREND
# ============================================================

def trend_status(
    price,
    ema20_value,
    ema50_value
):

    if (
        price is None
        or ema20_value is None
        or ema50_value is None
    ):
        return "WAIT"

    if (
        price > ema20_value
        and ema20_value > ema50_value
    ):
        return "BULLISH"

    if (
        price < ema20_value
        and ema20_value < ema50_value
    ):
        return "BEARISH"

    return "MIXED"


# ============================================================
# MOMENTUM
# ============================================================

def momentum_status(rsi_value):

    if rsi_value is None:
        return "WAIT"

    if rsi_value >= 55:
        return "BULLISH"

    if rsi_value <= 45:
        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# VOLATILITY
# ============================================================

def volatility_status(
    price,
    atr_value
):

    if (
        price is None
        or atr_value is None
        or price == 0
    ):
        return "WAIT"

    atr_percent = (
        atr_value
        / price
        * 100
    )

    if atr_percent >= 0.20:
        return "HIGH"

    if atr_percent >= 0.08:
        return "NORMAL"

    return "LOW"


# ============================================================
# SIGNAL ENGINE
# ============================================================

def calculate_signal(
    price,
    ema20_value,
    ema50_value,
    rsi_value,
    structure,
    breakout,
    liquidity_high,
    liquidity_low
):

    if price is None:
        return "WAIT", 0, []

    score = 0
    reasons = []

    # --------------------------------------------------------
    # TREND
    # --------------------------------------------------------

    trend = trend_status(
        price,
        ema20_value,
        ema50_value
    )

    if trend == "BULLISH":

        score += 2

        reasons.append(
            "Price above EMA20 and EMA20 above EMA50"
        )

    elif trend == "BEARISH":

        score -= 2

        reasons.append(
            "Price below EMA20 and EMA20 below EMA50"
        )

    # --------------------------------------------------------
    # RSI MOMENTUM
    # --------------------------------------------------------

    if rsi_value is not None:

        if 55 <= rsi_value < 70:

            score += 1

            reasons.append(
                "Bullish RSI momentum"
            )

        elif 30 < rsi_value <= 45:

            score -= 1

            reasons.append(
                "Bearish RSI momentum"
            )

        elif rsi_value >= 70:

            reasons.append(
                "RSI overbought - avoid chasing"
            )

        elif rsi_value <= 30:

            reasons.append(
                "RSI oversold - avoid chasing"
            )

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    if structure == "BULLISH_STRUCTURE":

        score += 2

        reasons.append(
            "Market structure bullish"
        )

    elif structure == "BEARISH_STRUCTURE":

        score -= 2

        reasons.append(
            "Market structure bearish"
        )

    # --------------------------------------------------------
    # BREAKOUT
    # --------------------------------------------------------

    if breakout == "BULLISH_BREAKOUT":

        score += 2

        reasons.append(
            "Price broke above liquidity high"
        )

    elif breakout == "BEARISH_BREAKOUT":

        score -= 2

        reasons.append(
            "Price broke below liquidity low"
        )

    # --------------------------------------------------------
    # LIQUIDITY CHASE FILTER
    # --------------------------------------------------------

    near_high = False
    near_low = False

    if liquidity_high is not None:

        distance_high = (
            abs(price - liquidity_high)
            / price
        )

        near_high = distance_high < 0.001

    if liquidity_low is not None:

        distance_low = (
            abs(price - liquidity_low)
            / price
        )

        near_low = distance_low < 0.001

    # Don't blindly buy directly below liquidity high.
    if (
        near_high
        and score > 0
        and breakout != "BULLISH_BREAKOUT"
    ):

        score -= 1

        reasons.append(
            "Near resistance/liquidity - buy chase filtered"
        )

    # Don't blindly sell directly above liquidity low.
    if (
        near_low
        and score < 0
        and breakout != "BEARISH_BREAKOUT"
    ):

        score += 1

        reasons.append(
            "Near support/liquidity - sell chase filtered"
        )

    # --------------------------------------------------------
    # CONFLICT FILTER
    # --------------------------------------------------------

    bullish_count = 0
    bearish_count = 0

    if trend == "BULLISH":
        bullish_count += 1

    if trend == "BEARISH":
        bearish_count += 1

    if structure == "BULLISH_STRUCTURE":
        bullish_count += 1

    if structure == "BEARISH_STRUCTURE":
        bearish_count += 1

    if breakout == "BULLISH_BREAKOUT":
        bullish_count += 1

    if breakout == "BEARISH_BREAKOUT":
        bearish_count += 1

    if bullish_count > 0 and bearish_count > 0:

        reasons.append(
            "Market factors are conflicting"
        )

        return (
            "WAIT",
            score,
            reasons
        )

    # --------------------------------------------------------
    # FINAL DECISION
    # --------------------------------------------------------

    if score >= 5:

        return (
            "BUY",
            score,
            reasons
        )

    if score <= -5:

        return (
            "SELL",
            score,
            reasons
        )

    return (
        "WAIT",
        score,
        reasons
    )


# ============================================================
# COMPLETE MARKET ANALYSIS
# ============================================================

def market_analysis():

    with state_lock:

        candle_data = [
            dict(c)
            for c in candles
        ]

        current_price = live_price

        connected = ws_connected
        subscribed = ws_subscribed
        loaded = history_loaded

        error = last_error

    if (
        current_price is None
        and candle_data
    ):

        current_price = float(
            candle_data[-1]["close"]
        )

    close_values = [
        float(c["close"])
        for c in candle_data
    ]

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
        candle_data,
        14
    )

    structure = market_structure(
        candle_data
    )

    liquidity_high, liquidity_low = (
        liquidity_levels(
            candle_data
        )
    )

    breakout = breakout_status(
        current_price,
        liquidity_high,
        liquidity_low
    )

    trend = trend_status(
        current_price,
        ema20_value,
        ema50_value
    )

    momentum = momentum_status(
        rsi_value
    )

    volatility = volatility_status(
        current_price,
        atr_value
    )

    signal, strength, reasons = (
        calculate_signal(
            current_price,
            ema20_value,
            ema50_value,
            rsi_value,
            structure,
            breakout,
            liquidity_high,
            liquidity_low,
        )
    )

    return {
        "app": "Trading AI",

        "symbol": SYMBOL,

        "timestamp": (
            datetime.fromtimestamp(
                live_timestamp,
                timezone.utc
            ).isoformat()
            if live_timestamp
            else None
        ),

        "price": current_price,

        "candle_count": len(candle_data),

        "history_loaded": loaded,

        "connected": connected,

        "subscribed": subscribed,

        "last_error": error,

        "ema20": (
            round(ema20_value, 5)
            if ema20_value is not None
            else None
        ),

        "ema50": (
            round(ema50_value, 5)
            if ema50_value is not None
            else None
        ),

        "rsi": (
            round(rsi_value, 2)
            if rsi_value is not None
            else None
        ),

        "atr": (
            round(atr_value, 5)
            if atr_value is not None
            else None
        ),

        "trend": trend,

        "momentum": momentum,

        "structure": structure,

        "liquidity_high": (
            round(liquidity_high, 5)
            if liquidity_high is not None
            else None
        ),

        "liquidity_low": (
            round(liquidity_low, 5)
            if liquidity_low is not None
            else None
        ),

        "breakout": breakout,

        "volatility": volatility,

        "signal": signal,

        "strength": strength,

        "reasons": reasons,
    }


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    html = """
<!DOCTYPE html>
<html>
<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>Trading AI</title>

<style>

body {
    margin: 0;
    font-family: Arial, sans-serif;
    background: #0b1020;
    color: white;
}

.container {
    max-width: 1100px;
    margin: auto;
    padding: 25px;
}

h1 {
    margin-bottom: 5px;
}

.subtitle {
    color: #9ca3af;
    margin-bottom: 25px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(220px, 1fr));
    gap: 15px;
}

.card {
    background: #151b2f;
    border: 1px solid #28314d;
    border-radius: 14px;
    padding: 18px;
}

.label {
    color: #9ca3af;
    font-size: 13px;
    margin-bottom: 8px;
}

.value {
    font-size: 25px;
    font-weight: bold;
}

.signal {
    font-size: 36px;
    font-weight: bold;
}

.reason {
    padding: 8px 0;
    color: #d1d5db;
}

.status {
    margin-bottom: 20px;
}

.good {
    color: #4ade80;
}

.bad {
    color: #f87171;
}

.neutral {
    color: #facc15;
}

</style>

</head>

<body>

<div class="container">

<h1>Trading AI</h1>

<div class="subtitle">
XAU/USD • Live Multi-Factor Analysis
</div>

<div id="status" class="status">
Loading...
</div>

<div class="grid">

<div class="card">
<div class="label">LIVE PRICE</div>
<div id="price" class="value">--</div>
</div>

<div class="card">
<div class="label">SIGNAL</div>
<div id="signal" class="signal">WAIT</div>
</div>

<div class="card">
<div class="label">STRENGTH</div>
<div id="strength" class="value">0</div>
</div>

<div class="card">
<div class="label">TREND</div>
<div id="trend" class="value">--</div>
</div>

<div class="card">
<div class="label">MOMENTUM</div>
<div id="momentum" class="value">--</div>
</div>

<div class="card">
<div class="label">STRUCTURE</div>
<div id="structure" class="value">--</div>
</div>

<div class="card">
<div class="label">RSI 14</div>
<div id="rsi" class="value">--</div>
</div>

<div class="card">
<div class="label">EMA 20</div>
<div id="ema20" class="value">--</div>
</div>

<div class="card">
<div class="label">EMA 50</div>
<div id="ema50" class="value">--</div>
</div>

<div class="card">
<div class="label">ATR 14</div>
<div id="atr" class="value">--</div>
</div>

<div class="card">
<div class="label">LIQUIDITY HIGH</div>
<div id="liquidityHigh" class="value">--</div>
</div>

<div class="card">
<div class="label">LIQUIDITY LOW</div>
<div id="liquidityLow" class="value">--</div>
</div>

<div class="card">
<div class="label">BREAKOUT</div>
<div id="breakout" class="value">--</div>
</div>

<div class="card">
<div class="label">VOLATILITY</div>
<div id="volatility" class="value">--</div>
</div>

</div>

<div class="card" style="margin-top:20px;">

<div class="label">
ANALYSIS REASONS
</div>

<div id="reasons">
Waiting for analysis...
</div>

</div>

</div>

<script>

function setText(id, value) {

    const element =
        document.getElementById(id);

    if (element) {
        element.textContent =
            value === null ||
            value === undefined
                ? "--"
                : value;
    }
}


function signalClass(signal) {

    const element =
        document.getElementById("signal");

    element.className =
        "signal";

    if (signal === "BUY") {
        element.classList.add("good");
    }

    else if (signal === "SELL") {
        element.classList.add("bad");
    }

    else {
        element.classList.add("neutral");
    }
}


async function updateDashboard() {

    try {

        const response =
            await fetch(
                "/api/market?t=" +
                Date.now(),
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();

        setText(
            "price",
            data.price !== null
                ? Number(data.price).toFixed(5)
                : "--"
        );

        setText(
            "signal",
            data.signal || "WAIT"
        );

        setText(
            "strength",
            data.strength ?? 0
        );

        setText(
            "trend",
            data.trend || "--"
        );

        setText(
            "momentum",
            data.momentum || "--"
        );

        setText(
            "structure",
            data.structure || "--"
        );

        setText(
            "rsi",
            data.rsi !== null
                ? Number(data.rsi).toFixed(2)
                : "--"
        );

        setText(
            "ema20",
            data.ema20 !== null
                ? Number(data.ema20).toFixed(5)
                : "--"
        );

        setText(
            "ema50",
            data.ema50 !== null
                ? Number(data.ema50).toFixed(5)
                : "--"
        );

        setText(
            "atr",
            data.atr !== null
                ? Number(data.atr).toFixed(5)
                : "--"
        );

        setText(
            "liquidityHigh",
            data.liquidity_high !== null
                ? Number(
                    data.liquidity_high
                  ).toFixed(5)
                : "--"
        );

        setText(
            "liquidityLow",
            data.liquidity_low !== null
                ? Number(
                    data.liquidity_low
                  ).toFixed(5)
                : "--"
        );

        setText(
            "breakout",
            data.breakout || "--"
        );

        setText(
            "volatility",
            data.volatility || "--"
        );

        signalClass(
            data.signal
        );

        const status =
            document.getElementById(
                "status"
            );

        if (
            data.connected &&
            data.subscribed
        ) {

            status.textContent =
                "● LIVE • " +
                data.symbol +
                " • " +
                data.candle_count +
                " candles";

            status.className =
                "status good";

        } else if (
            data.history_loaded
        ) {

            status.textContent =
                "● HISTORY READY • " +
                "Waiting for live connection";

            status.className =
                "status neutral";

        } else {

            status.textContent =
                "● CONNECTING / LOADING";

            status.className =
                "status neutral";
        }


        const reasons =
            document.getElementById(
                "reasons"
            );

        if (
            Array.isArray(data.reasons) &&
            data.reasons.length > 0
        ) {

            reasons.innerHTML =
                data.reasons
                    .map(
                        reason =>
                            '<div class="reason">• ' +
                            reason +
                            '</div>'
                    )
                    .join("");

        } else {

            reasons.innerHTML =
                '<div class="reason">' +
                'Waiting for sufficient market data...' +
                '</div>';
        }

    }

    catch (error) {

        const status =
            document.getElementById(
                "status"
            );

        status.textContent =
            "● API CONNECTION ERROR";

        status.className =
            "status bad";

        console.error(error);
    }
}


updateDashboard();

setInterval(
    updateDashboard,
    2000
);

</script>

</body>
</html>
"""

    return Response(
        html,
        mimetype="text/html"
    )


@app.route("/health")
def health():

    with state_lock:

        return jsonify(
            {
                "status": "ok",
                "app": "Trading AI",
                "symbol": SYMBOL,
                "history_loaded": history_loaded,
                "candle_count": len(candles),
                "websocket_connected": ws_connected,
                "websocket_subscribed": ws_subscribed,
                "live_price": live_price,
                "timestamp": utc_now(),
            }
        )


@app.route("/api/market")
def api_market():

    return jsonify(
        market_analysis()
    )


@app.route("/api/candles")
def api_candles():

    with state_lock:

        data = [
            dict(c)
            for c in candles
        ]

    return jsonify(
        {
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "count": len(data),
            "candles": data,
        }
    )


# ============================================================
# ENGINE START
# ============================================================

def start_engine():

    global engine_started

    if engine_started:
        return

    engine_started = True

    log("========================================")
    log("TRADING AI SINGLE PROCESS ENGINE")
    log("========================================")

    check_api_key()

    # --------------------------------------------------------
    # HISTORY THREAD
    # --------------------------------------------------------

    history_thread = threading.Thread(
        target=load_history,
        daemon=True,
        name="history-loader"
    )

    history_thread.start()

    # --------------------------------------------------------
    # WEBSOCKET THREAD
    # --------------------------------------------------------

    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True,
        name="twelve-data-websocket"
    )

    websocket_thread.start()

    log("Market engine started")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_engine()

    log(
        f"HTTP server starting on port {PORT}"
    )

    app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True,
        use_reloader=False,
    )
