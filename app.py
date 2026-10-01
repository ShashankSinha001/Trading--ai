import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string


# ============================================================
# TRADING AI - SINGLE PROCESS MARKET ENGINE
# XAU/USD GOLD
# ============================================================

APP_NAME = "Trading AI"
SYMBOL = "XAU/USD"
INTERVAL = "5min"

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

REST_URL = "https://api.twelvedata.com"
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

HISTORY_SIZE = 300

app = Flask(__name__)

# ============================================================
# SHARED STATE
# ============================================================

lock = threading.RLock()

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
# LOG
# ============================================================

def log(message):
    print(f"[TRADING-AI] {message}", flush=True)


# ============================================================
# HELPERS
# ============================================================

def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def round_value(value, digits=3):
    if value is None:
        return None

    try:
        return round(float(value), digits)
    except Exception:
        return None


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


def candle_bucket(timestamp):
    return int(timestamp // 300) * 300


def bucket_datetime(timestamp):
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# REST - HISTORY ONLY
# ============================================================

def load_history():

    global candles
    global history_loaded
    global last_error

    if not API_KEY:
        last_error = "TWELVE_DATA_API_KEY missing"
        log(last_error)
        return

    log("Loading XAU/USD 5-minute history...")

    try:

        response = requests.get(
            f"{REST_URL}/time_series",
            params={
                "symbol": SYMBOL,
                "interval": INTERVAL,
                "outputsize": HISTORY_SIZE,
                "timezone": "UTC",
                "order": "asc",
                "apikey": API_KEY,
            },
            timeout=20
        )

        if response.status_code == 429:
            raise RuntimeError(
                "Twelve Data HTTP 429 - daily API limit reached"
            )

        response.raise_for_status()

        data = response.json()

        if data.get("status") == "error":
            raise RuntimeError(str(data))

        values = data.get("values", [])

        parsed = []

        for item in values:

            dt_text = item.get("datetime")

            o = to_float(item.get("open"))
            h = to_float(item.get("high"))
            l = to_float(item.get("low"))
            c = to_float(item.get("close"))

            if not dt_text:
                continue

            if None in (o, h, l, c):
                continue

            timestamp = parse_datetime(dt_text)

            parsed.append(
                {
                    "datetime": dt_text,
                    "timestamp": timestamp,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": to_float(
                        item.get("volume")
                    ) or 0.0,
                }
            )

        if not parsed:
            raise RuntimeError(
                "No usable historical candles returned"
            )

        with lock:

            candles = parsed[-HISTORY_SIZE:]

            history_loaded = True

        log(
            f"History loaded successfully: "
            f"{len(candles)} candles"
        )

        log(
            f"LAST HISTORY CLOSE: "
            f"{candles[-1]['close']}"
        )

    except Exception as exc:

        history_loaded = False
        last_error = str(exc)

        log(
            f"HISTORY ERROR: {repr(exc)}"
        )


# ============================================================
# LIVE CANDLE
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

    # milliseconds → seconds
    if timestamp > 10_000_000_000:
        timestamp = timestamp / 1000.0

    bucket = candle_bucket(timestamp)

    with lock:

        live_price = price
        live_timestamp = timestamp

        # No history yet
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
            last["timestamp"]
        )

        # ----------------------------------------------------
        # SAME 5 MIN CANDLE
        # ----------------------------------------------------

        if bucket == last_bucket:

            last["high"] = max(
                last["high"],
                price
            )

            last["low"] = min(
                last["low"],
                price
            )

            last["close"] = price

        # ----------------------------------------------------
        # NEW 5 MIN CANDLE
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

        else:
            return False

    return True


# ============================================================
# WEBSOCKET
# ============================================================

def websocket_open(ws):

    global ws_connected
    global ws_subscribed
    global last_error

    ws_connected = True
    ws_subscribed = False
    last_error = None

    log("WebSocket connected")

    message = {
        "action": "subscribe",
        "params": {
            "symbols": SYMBOL
        }
    }

    try:

        ws.send(
            json.dumps(message)
        )

        log(
            f"Subscribed to {SYMBOL}"
        )

    except Exception as exc:

        last_error = str(exc)

        log(
            f"SUBSCRIBE ERROR: {repr(exc)}"
        )


def websocket_message(ws, message):

    global ws_subscribed
    global last_ws_message
    global last_error

    last_ws_message = message

    try:

        data = json.loads(message)

    except Exception as exc:

        last_error = str(exc)

        log(
            f"WEBSOCKET JSON ERROR: "
            f"{repr(exc)}"
        )

        return

    if not isinstance(data, dict):
        return

    event = data.get("event")

    # --------------------------------------------------------
    # SUBSCRIPTION
    # --------------------------------------------------------

    if event == "subscribe-status":

        log(
            "SUBSCRIBE STATUS: "
            + json.dumps(data)[:700]
        )

        status = str(
            data.get("status", "")
        ).lower()

        if status in (
            "ok",
            "success",
            "subscribed"
        ):
            ws_subscribed = True

        return

    # --------------------------------------------------------
    # PRICE
    # --------------------------------------------------------

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

        price = to_float(price)
        timestamp = to_float(timestamp)

        if price is None:
            log(
                "PRICE EVENT WITHOUT PRICE: "
                + json.dumps(data)[:1000]
            )
            return

        if timestamp is None:
            timestamp = time.time()

        if timestamp > 10_000_000_000:
            timestamp /= 1000.0

        log(
            f"PRICE RECEIVED: "
            f"{SYMBOL} = {price}"
        )

        updated = update_live_candle(
            price,
            timestamp
        )

        if updated:

            log(
                f"LIVE CANDLE UPDATED: "
                f"{price}"
            )

        return

    # --------------------------------------------------------
    # ERROR
    # --------------------------------------------------------

    if event == "error":

        last_error = str(data)

        log(
            "WEBSOCKET SERVER ERROR: "
            + json.dumps(data)[:1000]
        )

        return

    # --------------------------------------------------------
    # FALLBACK PRICE
    # --------------------------------------------------------

    if "price" in data:

        price = to_float(
            data.get("price")
        )

        if price is not None:

            log(
                f"PRICE RECEIVED FALLBACK: "
                f"{price}"
            )

            update_live_candle(
                price,
                time.time()
            )


def websocket_error(ws, error):

    global ws_connected
    global last_error

    ws_connected = False
    last_error = str(error)

    log(
        f"WEBSOCKET ERROR: {repr(error)}"
    )


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


def websocket_loop():

    global ws_connected
    global ws_subscribed

    if not API_KEY:

        log(
            "WebSocket not started: API key missing"
        )

        return

    log(
        "Starting WebSocket for "
        f"{SYMBOL}"
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

                on_message=websocket_message,

                on_error=websocket_error,

                on_close=websocket_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:

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
# INDICATORS
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = (
        sum(values[:period]) /
        period
    )

    for value in values[period:]:

        result = (
            (value - result) *
            multiplier
        ) + result

    return result


def calculate_rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i] -
            values[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    avg_gain = (
        sum(gains[:period]) /
        period
    )

    avg_loss = (
        sum(losses[:period]) /
        period
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain *
                (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss *
                (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


def calculate_atr(data, period=14):

    if len(data) < period + 1:
        return None

    ranges = []

    for i in range(1, len(data)):

        current = data[i]
        previous = data[i - 1]

        high = float(current["high"])
        low = float(current["low"])
        previous_close = float(
            previous["close"]
        )

        true_range = max(
            high - low,
            abs(
                high -
                previous_close
            ),
            abs(
                low -
                previous_close
            ),
        )

        ranges.append(true_range)

    if len(ranges) < period:
        return None

    return (
        sum(ranges[-period:]) /
        period
    )


# ============================================================
# ANALYSIS ENGINE
# ============================================================

def calculate_analysis():

    with lock:

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

            "buy_score": 0,
            "sell_score": 0,

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

    current_price = (
        price
        if price is not None
        else closes[-1]
    )

    ema20 = calculate_ema(
        closes,
        20
    )

    ema50 = calculate_ema(
        closes,
        50
    )

    rsi14 = calculate_rsi(
        closes,
        14
    )

    atr14 = calculate_atr(
        data,
        14
    )

    # ========================================================
    # TREND
    # ========================================================

    trend = "WAIT"

    if (
        ema20 is not None
        and ema50 is not None
    ):

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
    # STRUCTURE
    # ========================================================

    recent = data[-20:]
    previous = data[-40:-20]

    recent_high = max(
        x["high"]
        for x in recent
    )

    recent_low = min(
        x["low"]
        for x in recent
    )

    previous_high = max(
        x["high"]
        for x in previous
    )

    previous_low = min(
        x["low"]
        for x in previous
    )

    if (
        current_price >
        recent_high
    ):

        structure = "BREAKOUT-UP"

    elif (
        current_price <
        recent_low
    ):

        structure = "BREAKOUT-DOWN"

    elif (
        recent_high >
        previous_high
        and
        recent_low >
        previous_low
    ):

        structure = "HIGHER-HIGH / LOW"

    elif (
        recent_high <
        previous_high
        and
        recent_low <
        previous_low
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
    # SCORE
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
    if structure in (
        "BREAKOUT-UP",
        "HIGHER-HIGH / LOW"
    ):

        buy_score += 25

    elif structure in (
        "BREAKOUT-DOWN",
        "LOWER-HIGH / LOW"
    ):

        sell_score += 25

    # RSI
    if rsi14 is not None:

        if 55 <= rsi14 <= 70:

            buy_score += 15

        elif 30 <= rsi14 <= 45:

            sell_score += 15

        elif rsi14 > 75:

            buy_score -= 10

        elif rsi14 < 25:

            sell_score -= 10

    # EMA50
    if ema50 is not None:

        if current_price > ema50:

            buy_score += 10

        elif current_price < ema50:

            sell_score += 10

    # ========================================================
    # CONFLICT FILTER
    # ========================================================

    difference = abs(
        buy_score -
        sell_score
    )

    strength = max(
        buy_score,
        sell_score
    )

    signal = "WAIT"

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

        "rsi": round_value(
            rsi14,
            2
        ),

        "ema20": round_value(
            ema20,
            3
        ),

        "ema50": round_value(
            ema50,
            3
        ),

        "atr": round_value(
            atr14,
            3
        ),

        "liquidity_high": round_value(
            liquidity_high,
            3
        ),

        "liquidity_low": round_value(
            liquidity_low,
            3
        ),

        "breakout": breakout,
        "volatility": volatility,

        "candle_count": len(data),
    }


# ============================================================
# API
# ============================================================

@app.route("/api/market")
def market_api():

    analysis = calculate_analysis()

    with lock:

        result = {
            "app": APP_NAME,
            "symbol": SYMBOL,

            "price": round_value(
                live_price,
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


@app.route("/api/candles")
def candles_api():

    with lock:

        result = []

        for candle in candles:

            result.append(
                {
                    "datetime":
                        candle["datetime"],

                    "open":
                        candle["open"],

                    "high":
                        candle["high"],

                    "low":
                        candle["low"],

                    "close":
                        candle["close"],

                    "volume":
                        candle.get(
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


@app.route("/health")
def health():

    with lock:

        return jsonify(
            {
                "status": "ok",
                "app": APP_NAME,
                "symbol": SYMBOL,

                "api_key_present":
                    bool(API_KEY),

                "api_key_length":
                    len(API_KEY),

                "ws_connected":
                    ws_connected,

                "ws_subscribed":
                    ws_subscribed,

                "history_loaded":
                    history_loaded,

                "candle_count":
                    len(candles),

                "live_price":
                    live_price,

                "last_error":
                    last_error,
            }
        )


# ============================================================
# DASHBOARD
# ============================================================

HTML = """
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
    color: #edf4ff;
    font-family: Arial, sans-serif;
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
    margin-bottom: 20px;
}

.logo {
    font-size: 28px;
    font-weight: 800;
}

.symbol {
    color: #8ea4be;
    margin-top: 5px;
}

.connection {
    padding: 8px 14px;
    border-radius: 20px;
    background: #17283d;
    font-size: 12px;
    font-weight: bold;
}

.price-card,
.card {
    background: #0c1a2b;
    border: 1px solid #1e334c;
    border-radius: 16px;
}

.price-card {
    padding: 28px;
    margin-bottom: 16px;
}

.card {
    padding: 20px;
}

.label {
    color: #8ea4be;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.price {
    font-size: 52px;
    font-weight: 800;
    margin-top: 8px;
}

.signal {
    font-size: 32px;
    font-weight: 900;
    margin-top: 8px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(180px, 1fr));

    gap: 14px;
}

.card-value {
    font-size: 21px;
    font-weight: 700;
    margin-top: 10px;
}

.small {
    color: #7f94ad;
    font-size: 12px;
    margin-top: 8px;
}

.status {
    margin-top: 18px;
    color: #8297b0;
    font-size: 12px;
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


<div
class="card"
style="margin-bottom:16px;">

<div class="label">
AI Market Signal
</div>

<div
id="signal"
class="signal">
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

<div class="label">
Trend
</div>

<div
id="trend"
class="card-value">
WAIT
</div>

</div>


<div class="card">

<div class="label">
Momentum
</div>

<div
id="momentum"
class="card-value">
WAIT
</div>

</div>


<div class="card">

<div class="label">
Structure
</div>

<div
id="structure"
class="card-value">
INSUFFICIENT
</div>

</div>


<div class="card">

<div class="label">
RSI
</div>

<div
id="rsi"
class="card-value">
--
</div>

</div>


<div class="card">

<div class="label">
EMA 20
</div>

<div
id="ema20"
class="card-value">
--
</div>

</div>


<div class="card">

<div class="label">
EMA 50
</div>

<div
id="ema50"
class="card-value">
--
</div>

</div>


<div class="card">

<div class="label">
Liquidity High
</div>

<div
id="liqHigh"
class="card-value">
--
</div>

</div>


<div class="card">

<div class="label">
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
Starting market engine...
</div>

</div>


<script>

function text(id, value) {

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


async function update() {

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


        // Connection
        if (
            data.connected &&
            data.subscribed
        ) {

            text(
                "connection",
                "LIVE"
            );

        } else if (
            data.connected
        ) {

            text(
                "connection",
                "CONNECTED"
            );

        } else {

            text(
                "connection",
                "RECONNECTING"
            );
        }


        // Price
        if (
            data.price !== null &&
            data.price !== undefined
        ) {

            text(
                "price",
                Number(
                    data.price
                ).toFixed(3)
            );

            text(
                "priceStatus",
                "Live price feed active"
            );

        } else {

            text(
                "price",
                "--"
            );

            text(
                "priceStatus",
                "Waiting for live candle data..."
            );
        }


        text(
            "signal",
            data.signal || "WAIT"
        );

        text(
            "strength",
            "Strength: " +
            (data.strength || 0) +
            "%"
        );

        text(
            "trend",
            data.trend || "WAIT"
        );

        text(
            "momentum",
            data.momentum || "WAIT"
        );

        text(
            "structure",
            data.structure ||
            "INSUFFICIENT"
        );


        text(
            "rsi",
            data.rsi === null ||
            data.rsi === undefined
            ? "--"
            : Number(
                data.rsi
            ).toFixed(2)
        );


        text(
            "ema20",
            data.ema20 === null ||
            data.ema20 === undefined
            ? "--"
            : Number(
                data.ema20
            ).toFixed(3)
        );


        text(
            "ema50",
            data.ema50 === null ||
            data.ema50 === undefined
            ? "--"
            : Number(
                data.ema50
            ).toFixed(3)
        );


        text(
            "liqHigh",
            data.liquidity_high === null ||
            data.liquidity_high === undefined
            ? "--"
            : Number(
                data.liquidity_high
            ).toFixed(3)
        );


        text(
            "liqLow",
            data.liquidity_low === null ||
            data.liquidity_low === undefined
            ? "--"
            : Number(
                data.liquidity_low
            ).toFixed(3)
        );


        let status =
            "Candles: " +
            (data.candle_count || 0);


        if (data.last_error) {

            status +=
                " • " +
                data.last_error;
        }


        text(
            "status",
            status
        );

    } catch (error) {

        text(
            "connection",
            "API ERROR"
        );

        text(
            "status",
            error.toString()
        );
    }
}


update();

setInterval(
    update,
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
    return render_template_string(HTML)


# ============================================================
# START ENGINE
# ============================================================

def start_engine():

    global engine_started

    if engine_started:
        return

    engine_started = True

    log("")
    log("========================================")
    log("TRADING AI SINGLE PROCESS ENGINE")
    log("========================================")

    log(
        f"API KEY PRESENT: {bool(API_KEY)}"
    )

    log(
        f"API KEY LENGTH: {len(API_KEY)}"
    )

    # History
    history_thread = threading.Thread(
        target=load_history,
        daemon=True
    )

    history_thread.start()

    # WebSocket
    ws_thread = threading.Thread(
        target=websocket_loop,
        daemon=True
    )

    ws_thread.start()

    log(
        "Market engine started"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_engine()

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    log(
        f"HTTP server starting on port {port}"
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False
    )

else:

    # Important:
    # Do NOT start workers during Gunicorn import.
    # This application is intended to run directly
    # as a single Python process.
    pass
