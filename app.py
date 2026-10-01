import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, Response

try:
    import websocket
except ImportError:
    websocket = None


# ============================================================
# TRADING AI
# STABLE MARKET DATA ENGINE
# XAU/USD
# ============================================================

app = Flask(__name__)

# -----------------------------
# CONFIG
# -----------------------------

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

BASE_URL = "https://api.twelvedata.com"
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

SYMBOL = "XAU/USD"

# Keep REST usage LOW.
HISTORY_INTERVAL = "5min"
HISTORY_OUTPUTSIZE = 300

# 429 protection
REST_LOCK = threading.Lock()
REST_BLOCKED_UNTIL = 0

# Live state
STATE_LOCK = threading.Lock()

market_state = {
    "symbol": SYMBOL,
    "price": None,
    "timestamp": None,

    "connection": "STARTING",

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

    "sweep": "NONE",

    "candles": [],

    "data_status": "WAITING",
    "error": None,
}


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ts():
    return int(time.time())


def log(message):
    print(f"[TRADING-AI] {message}", flush=True)


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def api_key_status():
    if not API_KEY:
        return {
            "present": False,
            "length": 0
        }

    return {
        "present": True,
        "length": len(API_KEY)
    }


# ============================================================
# REST CONTROL
# ============================================================

def rest_allowed():
    global REST_BLOCKED_UNTIL

    with REST_LOCK:
        return time.time() >= REST_BLOCKED_UNTIL


def block_rest(seconds=3600):
    global REST_BLOCKED_UNTIL

    with REST_LOCK:
        REST_BLOCKED_UNTIL = time.time() + seconds

    log(f"REST temporarily blocked for {seconds} seconds")


def td_get(endpoint, params=None, timeout=15):
    if not API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is missing")

    if not rest_allowed():
        raise RuntimeError("REST temporarily blocked after previous API limit")

    params = dict(params or {})
    params["apikey"] = API_KEY

    url = f"{BASE_URL}/{endpoint}"

    try:
        response = requests.get(
            url,
            params=params,
            timeout=timeout
        )
    except Exception as exc:
        raise RuntimeError(f"REST connection error: {exc}")

    # API quota protection
    if response.status_code == 429:
        block_rest(3600)
        raise RuntimeError("Twelve Data HTTP 429 - REST quota exceeded")

    try:
        data = response.json()
    except Exception:
        raise RuntimeError(
            f"Twelve Data invalid response HTTP {response.status_code}"
        )

    # Twelve Data can return an error in JSON even with HTTP 200.
    if isinstance(data, dict):
        status = str(data.get("status", "")).lower()

        if status == "error":
            code = data.get("code")
            message = data.get("message", "Twelve Data error")

            if str(code) == "429":
                block_rest(3600)

            raise RuntimeError(
                f"Twelve Data error {code}: {message}"
            )

    if response.status_code != 200:
        raise RuntimeError(
            f"Twelve Data HTTP {response.status_code}: {data}"
        )

    return data


# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(values[:period]) / period

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
        change = values[i] - values[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


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
            abs(low - prev_close)
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


# ============================================================
# CANDLE NORMALIZATION
# ============================================================

def normalize_candle(row):
    try:
        return {
            "time": int(
                datetime.strptime(
                    row["datetime"],
                    "%Y-%m-%d %H:%M:%S"
                ).replace(
                    tzinfo=timezone.utc
                ).timestamp()
            ),

            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }

    except Exception:
        return None


# ============================================================
# HISTORY
# ============================================================

def load_history():
    if not API_KEY:
        log("ERROR: TWELVE_DATA_API_KEY is missing")
        return False

    if not rest_allowed():
        log("History skipped: REST currently blocked")
        return False

    log("Loading XAU/USD 5-minute history...")

    try:
        data = td_get(
            "time_series",
            {
                "symbol": SYMBOL,
                "interval": HISTORY_INTERVAL,
                "outputsize": HISTORY_OUTPUTSIZE,
                "timezone": "UTC",
            }
        )

        values = data.get("values", [])

        if not values:
            raise RuntimeError(
                f"No candle data returned: {data}"
            )

        candles = []

        for row in reversed(values):
            candle = normalize_candle(row)

            if candle:
                candles.append(candle)

        if not candles:
            raise RuntimeError("No valid candles after parsing")

        with STATE_LOCK:
            market_state["candles"] = candles
            market_state["data_status"] = "HISTORY_READY"
            market_state["error"] = None

        log(
            f"History loaded successfully: {len(candles)} candles"
        )

        calculate_analysis()

        return True

    except Exception as exc:
        log(f"HISTORY ERROR: {exc}")

        with STATE_LOCK:
            market_state["error"] = str(exc)

        return False


# ============================================================
# LIVE CANDLE UPDATE
# ============================================================

def update_live_candle(price, timestamp=None):
    price = safe_float(price)

    if price is None:
        return

    if timestamp is None:
        timestamp = now_ts()

    # 5-minute bucket
    bucket = timestamp - (timestamp % 300)

    with STATE_LOCK:
        candles = market_state["candles"]

        if not candles:
            candle = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }

            candles.append(candle)

        else:
            last = candles[-1]

            if last["time"] == bucket:
                last["high"] = max(last["high"], price)
                last["low"] = min(last["low"], price)
                last["close"] = price

            elif bucket > last["time"]:

                candle = {
                    "time": bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                }

                candles.append(candle)

                # Keep memory reasonable
                if len(candles) > 500:
                    del candles[:-500]

        market_state["price"] = price
        market_state["timestamp"] = timestamp
        market_state["connection"] = "LIVE"
        market_state["data_status"] = "LIVE"
        market_state["error"] = None

    calculate_analysis()


# ============================================================
# ANALYSIS
# ============================================================

def calculate_analysis():
    with STATE_LOCK:
        candles = list(market_state["candles"])

    if len(candles) < 20:
        with STATE_LOCK:
            market_state["structure"] = "INSUFFICIENT"

        return

    closes = [c["close"] for c in candles]

    current_price = closes[-1]

    ema20_value = ema(closes, 20)
    ema50_value = ema(closes, 50)
    rsi_value = rsi(closes, 14)
    atr_value = atr(candles, 14)

    trend = "WAIT"

    if ema20_value is not None and ema50_value is not None:
        if current_price > ema20_value > ema50_value:
            trend = "BULLISH"

        elif current_price < ema20_value < ema50_value:
            trend = "BEARISH"

        else:
            trend = "MIXED"

    momentum = "WAIT"

    if rsi_value is not None:

        if rsi_value >= 60:
            momentum = "BULLISH"

        elif rsi_value <= 40:
            momentum = "BEARISH"

        else:
            momentum = "NEUTRAL"

    # Structure from recent swing area
    recent = candles[-20:]

    recent_high = max(c["high"] for c in recent)
    recent_low = min(c["low"] for c in recent)

    structure = "RANGE"

    if current_price > recent_high:
        structure = "BREAKOUT_UP"

    elif current_price < recent_low:
        structure = "BREAKOUT_DOWN"

    # Liquidity
    liquidity_high = max(
        c["high"] for c in candles[-50:]
    )

    liquidity_low = min(
        c["low"] for c in candles[-50:]
    )

    signal = "WAIT"
    strength = 0

    bullish_points = 0
    bearish_points = 0

    # Trend
    if trend == "BULLISH":
        bullish_points += 2

    elif trend == "BEARISH":
        bearish_points += 2

    # Momentum
    if momentum == "BULLISH":
        bullish_points += 1

    elif momentum == "BEARISH":
        bearish_points += 1

    # Price vs EMA20
    if ema20_value is not None:

        if current_price > ema20_value:
            bullish_points += 1

        elif current_price < ema20_value:
            bearish_points += 1

    # RSI confirmation
    if rsi_value is not None:

        if 50 < rsi_value < 70:
            bullish_points += 1

        elif 30 < rsi_value < 50:
            bearish_points += 1

    total = bullish_points + bearish_points

    if total >= 3:

        if bullish_points > bearish_points:
            signal = "BUY"
            strength = min(
                95,
                int(
                    50 +
                    ((bullish_points - bearish_points) * 12)
                )
            )

        elif bearish_points > bullish_points:
            signal = "SELL"
            strength = min(
                95,
                int(
                    50 +
                    ((bearish_points - bullish_points) * 12)
                )
            )

        else:
            signal = "WAIT"
            strength = 40

    with STATE_LOCK:
        market_state["ema20"] = (
            round(ema20_value, 4)
            if ema20_value is not None
            else None
        )

        market_state["ema50"] = (
            round(ema50_value, 4)
            if ema50_value is not None
            else None
        )

        market_state["rsi"] = (
            round(rsi_value, 2)
            if rsi_value is not None
            else None
        )

        market_state["atr"] = (
            round(atr_value, 4)
            if atr_value is not None
            else None
        )

        market_state["trend"] = trend
        market_state["momentum"] = momentum
        market_state["structure"] = structure

        market_state["liquidity_high"] = round(
            liquidity_high, 4
        )

        market_state["liquidity_low"] = round(
            liquidity_low, 4
        )

        market_state["signal"] = signal
        market_state["strength"] = strength


# ============================================================
# WEBSOCKET
# ============================================================

def websocket_message(ws, message):
    try:
        data = json.loads(message)

        # Twelve Data message variants
        price = (
            data.get("price")
            or data.get("close")
        )

        if price is not None:
            update_live_candle(
                price,
                now_ts()
            )

    except Exception as exc:
        log(f"WebSocket message parse error: {exc}")


def websocket_error(ws, error):
    log(f"WEBSOCKET ERROR: {error}")

    with STATE_LOCK:
        market_state["connection"] = "ERROR"
        market_state["error"] = str(error)


def websocket_close(ws, close_status_code, close_msg):
    log(
        f"WebSocket closed: "
        f"{close_status_code} {close_msg}"
    )

    with STATE_LOCK:
        market_state["connection"] = "RECONNECTING"


def websocket_open(ws):
    log("WebSocket connected")

    with STATE_LOCK:
        market_state["connection"] = "CONNECTED"
        market_state["error"] = None

    # Twelve Data subscription
    subscribe_message = {
        "action": "subscribe",
        "params": {
            "symbols": SYMBOL
        }
    }

    try:
        ws.send(json.dumps(subscribe_message))
        log(f"Subscribed to {SYMBOL}")

    except Exception as exc:
        log(f"WebSocket subscribe error: {exc}")


def websocket_loop():
    if websocket is None:
        log(
            "ERROR: websocket-client package is not installed"
        )
        return

    if not API_KEY:
        log(
            "ERROR: TWELVE_DATA_API_KEY missing. "
            "WebSocket cannot start."
        )

        with STATE_LOCK:
            market_state["connection"] = "NO_API_KEY"

        return

    log(
        f"Starting WebSocket for {SYMBOL} "
        f"(API key present: yes, length: {len(API_KEY)})"
    )

    while True:

        try:
            with STATE_LOCK:
                market_state["connection"] = "CONNECTING"

            # API key in query parameter.
            ws_url = f"{WS_URL}?apikey={API_KEY}"

            ws = websocket.WebSocketApp(
                ws_url,
                on_open=websocket_open,
                on_message=websocket_message,
                on_error=websocket_error,
                on_close=websocket_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            log(f"WebSocket loop error: {exc}")

            with STATE_LOCK:
                market_state["connection"] = "RECONNECTING"
                market_state["error"] = str(exc)

        log("WebSocket reconnecting in 15 seconds...")
        time.sleep(15)


# ============================================================
# STARTUP WORKERS
# ============================================================

STARTED = False
START_LOCK = threading.Lock()


def start_workers():
    global STARTED

    with START_LOCK:

        if STARTED:
            return

        STARTED = True

    log("========================================")
    log("TRADING AI STARTING")
    log("========================================")

    status = api_key_status()

    log(
        f"API KEY PRESENT: {status['present']}"
    )

    log(
        f"API KEY LENGTH: {status['length']}"
    )

    if not status["present"]:
        log(
            "WARNING: TWELVE_DATA_API_KEY is not set"
        )

    # History worker
    history_thread = threading.Thread(
        target=load_history,
        daemon=True
    )

    history_thread.start()

    # WebSocket worker
    websocket_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    websocket_thread.start()

    log("Background workers started")


# Start when Flask module loads under Gunicorn
start_workers()


# ============================================================
# API ROUTES
# ============================================================

@app.route("/")
def home():

    html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width,initial-scale=1">

    <title>Trading AI</title>

    <style>

        body {
            margin: 0;
            background: #0b1020;
            color: white;
            font-family: Arial, sans-serif;
        }

        .container {
            max-width: 1100px;
            margin: auto;
            padding: 25px;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }

        .title {
            font-size: 28px;
            font-weight: bold;
        }

        .status {
            padding: 8px 14px;
            border-radius: 20px;
            background: #222a40;
            font-size: 13px;
        }

        .price {
            font-size: 42px;
            font-weight: bold;
            margin-top: 30px;
        }

        .signal {
            margin-top: 25px;
            padding: 25px;
            background: #151d34;
            border-radius: 16px;
        }

        .signal-name {
            font-size: 34px;
            font-weight: bold;
        }

        .grid {
            display: grid;
            grid-template-columns:
                repeat(auto-fit, minmax(160px, 1fr));

            gap: 12px;
            margin-top: 20px;
        }

        .card {
            background: #151d34;
            padding: 18px;
            border-radius: 14px;
        }

        .label {
            color: #8e9ab5;
            font-size: 12px;
            margin-bottom: 8px;
        }

        .value {
            font-size: 20px;
            font-weight: bold;
        }

        #chart {
            margin-top: 25px;
            height: 400px;
            background: #11182c;
            border-radius: 16px;
            padding: 10px;
        }

        .error {
            margin-top: 20px;
            color: #ff8c8c;
            font-size: 13px;
        }

    </style>
</head>

<body>

<div class="container">

    <div class="header">

        <div>
            <div class="title">Trading AI</div>
            <div>XAU/USD • GOLD</div>
        </div>

        <div id="connection"
             class="status">
            CONNECTING
        </div>

    </div>

    <div id="price"
         class="price">
        --
    </div>

    <div>
        Live Gold Price
    </div>

    <div class="signal">

        <div class="label">
            AI MARKET SIGNAL
        </div>

        <div id="signal"
             class="signal-name">
            WAIT
        </div>

        <div>
            Strength:
            <span id="strength">
                0
            </span>%
        </div>

    </div>

    <div class="grid">

        <div class="card">
            <div class="label">TREND</div>
            <div id="trend"
                 class="value">
                WAIT
            </div>
        </div>

        <div class="card">
            <div class="label">MOMENTUM</div>
            <div id="momentum"
                 class="value">
                WAIT
            </div>
        </div>

        <div class="card">
            <div class="label">STRUCTURE</div>
            <div id="structure"
                 class="value">
                INSUFFICIENT
            </div>
        </div>

        <div class="card">
            <div class="label">RSI</div>
            <div id="rsi"
                 class="value">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">EMA 20</div>
            <div id="ema20"
                 class="value">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">EMA 50</div>
            <div id="ema50"
                 class="value">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">LIQUIDITY HIGH</div>
            <div id="liqHigh"
                 class="value">
                --
            </div>
        </div>

        <div class="card">
            <div class="label">LIQUIDITY LOW</div>
            <div id="liqLow"
                 class="value">
                --
            </div>
        </div>

    </div>

    <div id="chart">
        Waiting for candle data...
    </div>

    <div id="error"
         class="error">
    </div>

</div>


<script>

async function update() {

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
            "connection"
        ).textContent =
            data.connection || "WAITING";

        document.getElementById(
            "price"
        ).textContent =
            data.price !== null &&
            data.price !== undefined
                ? Number(data.price).toFixed(2)
                : "--";

        document.getElementById(
            "signal"
        ).textContent =
            data.signal || "WAIT";

        document.getElementById(
            "strength"
        ).textContent =
            data.strength || 0;

        document.getElementById(
            "trend"
        ).textContent =
            data.trend || "WAIT";

        document.getElementById(
            "momentum"
        ).textContent =
            data.momentum || "WAIT";

        document.getElementById(
            "structure"
        ).textContent =
            data.structure || "INSUFFICIENT";

        document.getElementById(
            "rsi"
        ).textContent =
            data.rsi !== null &&
            data.rsi !== undefined
                ? Number(data.rsi).toFixed(2)
                : "--";

        document.getElementById(
            "ema20"
        ).textContent =
            data.ema20 !== null &&
            data.ema20 !== undefined
                ? Number(data.ema20).toFixed(2)
                : "--";

        document.getElementById(
            "ema50"
        ).textContent =
            data.ema50 !== null &&
            data.ema50 !== undefined
                ? Number(data.ema50).toFixed(2)
                : "--";

        document.getElementById(
            "liqHigh"
        ).textContent =
            data.liquidity_high !== null &&
            data.liquidity_high !== undefined
                ? Number(
                    data.liquidity_high
                  ).toFixed(2)
                : "--";

        document.getElementById(
            "liqLow"
        ).textContent =
            data.liquidity_low !== null &&
            data.liquidity_low !== undefined
                ? Number(
                    data.liquidity_low
                  ).toFixed(2)
                : "--";

        document.getElementById(
            "error"
        ).textContent =
            data.error || "";

    }

    catch (error) {

        document.getElementById(
            "error"
        ).textContent =
            "Dashboard connection error: "
            + error;

    }

}


async function updateChartInfo() {

    try {

        const response =
            await fetch(
                "/api/candles",
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();

        const candles =
            data.candles || [];

        const chart =
            document.getElementById(
                "chart"
            );

        if (!candles.length) {

            chart.textContent =
                "Waiting for live candle data...";

            return;
        }

        const last =
            candles[candles.length - 1];

        chart.innerHTML =
            "<div style='padding:20px'>" +

            "<b>Gold — 5 Minute Structure</b>" +

            "<br><br>" +

            "Candles: " +
            candles.length +

            "<br>" +

            "Open: " +
            Number(last.open).toFixed(2) +

            "<br>" +

            "High: " +
            Number(last.high).toFixed(2) +

            "<br>" +

            "Low: " +
            Number(last.low).toFixed(2) +

            "<br>" +

            "Close: " +
            Number(last.close).toFixed(2) +

            "</div>";

    }

    catch (error) {

        console.log(error);

    }

}


update();

updateChartInfo();

setInterval(update, 3000);

setInterval(
    updateChartInfo,
    5000
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

    with STATE_LOCK:
        connection = market_state["connection"]
        price = market_state["price"]
        data_status = market_state["data_status"]

    return jsonify({
        "status": "ok",
        "symbol": SYMBOL,
        "connection": connection,
        "price": price,
        "data_status": data_status,
        "api_key_present": bool(API_KEY),
        "time": datetime.now(
            timezone.utc
        ).isoformat()
    })


@app.route("/api/market")
def api_market():

    with STATE_LOCK:
        data = dict(market_state)

    # Don't send unnecessary internal error details
    # if there is no error.
    return jsonify({
        "symbol": data["symbol"],
        "price": data["price"],
        "timestamp": data["timestamp"],

        "connection": data["connection"],

        "signal": data["signal"],
        "strength": data["strength"],

        "trend": data["trend"],
        "momentum": data["momentum"],
        "structure": data["structure"],

        "rsi": data["rsi"],
        "ema20": data["ema20"],
        "ema50": data["ema50"],
        "atr": data["atr"],

        "liquidity_high":
            data["liquidity_high"],

        "liquidity_low":
            data["liquidity_low"],

        "sweep": data["sweep"],

        "data_status":
            data["data_status"],

        "error":
            data["error"],
    })


@app.route("/api/candles")
def api_candles():

    with STATE_LOCK:
        candles = list(
            market_state["candles"]
        )

    return jsonify({
        "symbol": SYMBOL,
        "interval": "5min",
        "candles": candles
    })


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
