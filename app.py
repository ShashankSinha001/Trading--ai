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

# =========================================================
# CONFIG
# =========================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# Your current Twelve Data Basic plan does not provide WTI/USD.
# Keep this False so Oil cannot break the Gold connection.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

CANDLE_INTERVALS = [
    "5min",
    "15min",
    "1h",
    "1day"
]

# =========================================================
# GLOBAL STATE
# =========================================================

lock = threading.RLock()

workers_started = False

clients = []

clients_lock = threading.Lock()

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

        "connection": "CONNECTING",

        "updated": None,

        "timeframes": {
            "5min": {},
            "15min": {},
            "1h": {},
            "1day": {}
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
            "WTI/USD is not available on the current "
            "Twelve Data plan."
        )
    }
}


# =========================================================
# HELPERS
# =========================================================

def now_text():
    return time.strftime("%H:%M:%S")


def number(value):
    try:
        return float(value)
    except Exception:
        return None


def broadcast():
    """
    Send the latest state to every connected browser.
    """
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


def update_gold_price(price):
    price = number(price)

    if price is None:
        return

    with lock:
        state["gold"]["price"] = price
        state["gold"]["updated"] = now_text()
        state["gold"]["connection"] = "CONNECTED"
        state["gold"]["error"] = None

    broadcast()

    print(
        f"STATE UPDATED: GOLD = {price}"
    )


# =========================================================
# TECHNICAL INDICATORS
# =========================================================

def ema(values, period):
    if not values or len(values) < period:
        return None

    multiplier = 2.0 / (period + 1.0)

    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            (value - result) * multiplier
        ) + result

    return result


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change >= 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            ((avg_gain * (period - 1)) + gains[i])
            / period
        )

        avg_loss = (
            ((avg_loss * (period - 1)) + losses[i])
            / period
        )

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (100.0 / (1.0 + rs))


# =========================================================
# TWELVE DATA CANDLES
# =========================================================

def get_candles(
    symbol,
    interval,
    outputsize=100
):
    if not API_KEY:
        print("ERROR: TWELVE_DATA_API_KEY missing")
        return []

    try:
        response = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": API_KEY
            },
            timeout=15
        )

        if response.status_code != 200:
            print(
                "CANDLE HTTP ERROR:",
                interval,
                response.status_code
            )
            return []

        data = response.json()

        if data.get("status") == "error":
            print(
                "CANDLE API ERROR:",
                interval,
                data.get("message")
            )
            return []

        values = data.get("values", [])

        if not values:
            return []

        # Twelve Data normally returns newest first.
        values = list(reversed(values))

        return values

    except Exception as exc:
        print(
            "CANDLE ERROR:",
            interval,
            repr(exc)
        )
        return []


# =========================================================
# ANALYZE ONE TIMEFRAME
# =========================================================

def analyze_timeframe(candles):
    if not candles:
        return {}

    closes = []
    highs = []
    lows = []

    for candle in candles:

        close = number(
            candle.get("close")
        )

        high = number(
            candle.get("high")
        )

        low = number(
            candle.get("low")
        )

        if close is not None:
            closes.append(close)

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    if len(closes) < 20:
        return {}

    current = closes[-1]

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

    recent_highs = highs[-20:]
    recent_lows = lows[-20:]

    support = (
        min(recent_lows)
        if recent_lows
        else None
    )

    resistance = (
        max(recent_highs)
        if recent_highs
        else None
    )

    if ema20 is not None and ema50 is not None:

        if current > ema20 > ema50:
            trend = "BULLISH"

        elif current < ema20 < ema50:
            trend = "BEARISH"

        else:
            trend = "NEUTRAL"

    else:
        trend = "NEUTRAL"


    if current_rsi is None:
        momentum = "NEUTRAL"

    elif current_rsi >= 55:
        momentum = "BUYING"

    elif current_rsi <= 45:
        momentum = "SELLING"

    else:
        momentum = "NEUTRAL"


    recent = closes[-10:]

    if len(recent) >= 10:

        old_average = sum(
            recent[:5]
        ) / 5

        new_average = sum(
            recent[-5:]
        ) / 5

        if new_average > old_average:
            structure = "HIGHER"

        elif new_average < old_average:
            structure = "LOWER"

        else:
            structure = "RANGE"

    else:
        structure = "RANGE"


    # -----------------------------------------------------
    # Liquidity / sweep detection
    # -----------------------------------------------------

    liquidity_high = (
        max(highs[-10:])
        if len(highs) >= 10
        else resistance
    )

    liquidity_low = (
        min(lows[-10:])
        if len(lows) >= 10
        else support
    )

    sweep = "NONE"

    if len(candles) >= 3:

        previous_high = max(
            highs[-3:-1]
        )

        previous_low = min(
            lows[-3:-1]
        )

        latest_high = highs[-1]
        latest_low = lows[-1]
        latest_close = closes[-1]

        # High liquidity sweep
        if (
            latest_high > previous_high
            and latest_close < previous_high
        ):
            sweep = "HIGH SWEEP"

        # Low liquidity sweep
        elif (
            latest_low < previous_low
            and latest_close > previous_low
        ):
            sweep = "LOW SWEEP"


    return {
        "price": current,

        "trend": trend,
        "momentum": momentum,
        "structure": structure,

        "rsi": (
            round(current_rsi, 2)
            if current_rsi is not None
            else None
        ),

        "ema20": (
            round(ema20, 3)
            if ema20 is not None
            else None
        ),

        "ema50": (
            round(ema50, 3)
            if ema50 is not None
            else None
        ),

        "support": (
            round(support, 3)
            if support is not None
            else None
        ),

        "resistance": (
            round(resistance, 3)
            if resistance is not None
            else None
        ),

        "liquidity_high": (
            round(liquidity_high, 3)
            if liquidity_high is not None
            else None
        ),

        "liquidity_low": (
            round(liquidity_low, 3)
            if liquidity_low is not None
            else None
        ),

        "sweep": sweep
    }


# =========================================================
# AI MARKET ENGINE
# =========================================================

def build_ai_analysis(timeframes, live_price):
    """
    Local Trading-AI scoring engine.
    This is the stable base before connecting
    an external AI model API.
    """

    score = 0

    five = timeframes.get(
        "5min",
        {}
    )

    fifteen = timeframes.get(
        "15min",
        {}
    )

    one_hour = timeframes.get(
        "1h",
        {}
    )


    # -----------------------------------------------------
    # 5 MIN
    # -----------------------------------------------------

    if five.get("trend") == "BULLISH":
        score += 2

    elif five.get("trend") == "BEARISH":
        score -= 2


    if five.get("momentum") == "BUYING":
        score += 1

    elif five.get("momentum") == "SELLING":
        score -= 1


    # -----------------------------------------------------
    # 15 MIN
    # -----------------------------------------------------

    if fifteen.get("trend") == "BULLISH":
        score += 2

    elif fifteen.get("trend") == "BEARISH":
        score -= 2


    if fifteen.get("structure") == "HIGHER":
        score += 1

    elif fifteen.get("structure") == "LOWER":
        score -= 1


    # -----------------------------------------------------
    # 1 HOUR
    # -----------------------------------------------------

    if one_hour.get("trend") == "BULLISH":
        score += 2

    elif one_hour.get("trend") == "BEARISH":
        score -= 2


    # -----------------------------------------------------
    # Liquidity sweep
    # -----------------------------------------------------

    sweep = five.get(
        "sweep",
        "NONE"
    )

    if sweep == "LOW SWEEP":
        score += 1

    elif sweep == "HIGH SWEEP":
        score -= 1


    # -----------------------------------------------------
    # Keep score between -10 and +10
    # -----------------------------------------------------

    score = max(
        -10,
        min(10, score)
    )


    # -----------------------------------------------------
    # Decision
    # -----------------------------------------------------

    if score >= 6:
        decision = "BUY"

    elif score <= -6:
        decision = "SELL"

    else:
        decision = "WAIT"


    confidence = 50 + (
        abs(score) * 5
    )

    confidence = max(
        50,
        min(95, confidence)
    )


    return {
        "signal": decision,
        "confidence": confidence,
        "score": score
    }


# =========================================================
# GOLD ANALYSIS LOOP
# =========================================================

def gold_analysis_loop():

    while True:

        try:

            timeframe_data = {}

            for interval in CANDLE_INTERVALS:

                candles = get_candles(
                    GOLD_SYMBOL,
                    interval,
                    100
                )

                result = analyze_timeframe(
                    candles
                )

                if result:
                    timeframe_data[
                        interval
                    ] = result

                # Small delay to avoid unnecessary
                # burst requests.
                time.sleep(0.5)


            if timeframe_data:

                with lock:

                    state["gold"][
                        "timeframes"
                    ] = timeframe_data


                live_price = None

                with lock:
                    live_price = (
                        state["gold"]["price"]
                    )


                if live_price is None:

                    live_price = (
                        timeframe_data
                        .get("5min", {})
                        .get("price")
                    )


                if live_price is not None:

                    ai = build_ai_analysis(
                        timeframe_data,
                        live_price
                    )


                    five = (
                        timeframe_data
                        .get("5min", {})
                    )


                    with lock:

                        state["gold"].update({

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
                                five.get("rsi"),

                            "ema20":
                                five.get("ema20"),

                            "ema50":
                                five.get("ema50"),

                            "support":
                                five.get("support"),

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
                                ai["signal"],

                            "confidence":
                                ai["confidence"],

                            "score":
                                ai["score"],

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


        except Exception as exc:

            print(
                "AI LOOP ERROR:",
                repr(exc)
            )


        # Analysis once per minute.
        time.sleep(60)


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_websocket_loop():

    reconnect_delay = 5

    while True:

        ws = None

        try:

            if not API_KEY:

                raise RuntimeError(
                    "TWELVE_DATA_API_KEY is missing"
                )


            print(
                "Connecting Twelve Data Gold WebSocket..."
            )


            ws = websocket.create_connection(
                WS_URL
                + "?apikey="
                + API_KEY,
                timeout=20
            )


            subscribe = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }


            ws.send(
                json.dumps(subscribe)
            )


            print(
                "TWELVE DATA GOLD SUBSCRIBE SENT"
            )


            with lock:

                state["gold"][
                    "connection"
                ] = "CONNECTED"

                state["gold"][
                    "error"
                ] = None


            broadcast()


            while True:

                raw = ws.recv()

                if not raw:
                    raise RuntimeError(
                        "WebSocket closed"
                    )


                try:
                    message = json.loads(
                        raw
                    )
                except Exception:
                    continue


                print(
                    "GOLD WS MESSAGE:",
                    message
                )


                event = message.get(
                    "event"
                )


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
                            price
                        )


                elif event == "subscribe-status":

                    print(
                        "GOLD SUBSCRIBE STATUS:",
                        message
                    )


                elif event == "heartbeat":

                    # Twelve Data heartbeat.
                    pass


                elif event == "error":

                    print(
                        "GOLD WS ERROR:",
                        message
                    )


        except Exception as exc:

            print(
                "GOLD WEBSOCKET ERROR:",
                repr(exc)
            )


            with lock:

                state["gold"][
                    "connection"
                ] = "RECONNECTING"

                state["gold"][
                    "error"
                ] = str(exc)


            broadcast()


        finally:

            try:

                if ws is not None:
                    ws.close()

            except Exception:
                pass


        print(
            f"Gold WebSocket reconnecting "
            f"in {reconnect_delay}s..."
        )

        time.sleep(
            reconnect_delay
        )


# =========================================================
# START WORKERS ONLY ONCE
# =========================================================

def start_workers():

    global workers_started

    with lock:

        if workers_started:
            return

        workers_started = True


    print(
        "Starting Trading-AI workers..."
    )


    ws_thread = threading.Thread(
        target=gold_websocket_loop,
        daemon=True,
        name="GoldWebSocket"
    )

    ws_thread.start()


    ai_thread = threading.Thread(
        target=gold_analysis_loop,
        daemon=True,
        name="GoldAnalysis"
    )

    ai_thread.start()


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    start_workers()

    return render_template_string(
        HTML
    )


# =========================================================
# API
# =========================================================

@app.route("/api/market")
def api_market():

    start_workers()

    with lock:

        data = json.loads(
            json.dumps(state)
        )

    return jsonify(data)


# =========================================================
# SSE STREAM
# =========================================================

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

            # Immediately send current state.
            with lock:
                initial = json.dumps(
                    state,
                    separators=(",", ":")
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

                    # Keep browser connection alive.
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


# =========================================================
# HEALTH
# =========================================================

@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "service": "Trading-AI",
        "gold_connection":
            state["gold"]["connection"],
        "gold_price":
            state["gold"]["price"],
        "time":
            now_text()
    })


# =========================================================
# OLD STYLE DASHBOARD
# =========================================================

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


<style>

* {
    box-sizing: border-box;
}


body {

    margin: 0;

    min-height: 100vh;

    font-family:
        Arial,
        Helvetica,
        sans-serif;

    color: white;

    background:
        radial-gradient(
            circle at top,
            #183d75 0%,
            #08172f 45%,
            #020817 100%
        );
}


.page {

    width: 92%;

    max-width: 1100px;

    margin: auto;

    padding:
        25px 0 45px;
}


.header {

    text-align: center;

    margin-bottom: 24px;
}


.header h1 {

    margin: 0;

    font-size: 30px;

}


.header p {

    margin: 7px 0 0;

    color: #7fa9e8;

    font-size: 14px;

}


.card {

    padding: 23px;

    margin-bottom: 22px;

    border-radius: 18px;

    border:
        1px solid
        rgba(110,160,230,.20);

    background:
        linear-gradient(
            145deg,
            rgba(20,43,82,.96),
            rgba(5,19,42,.97)
        );

    box-shadow:
        0 18px 45px
        rgba(0,0,0,.32);

}


.asset {

    font-size: 20px;

    font-weight: 700;

}


.price {

    margin-top: 10px;

    font-size: 38px;

    font-weight: 700;

}


.live {

    margin-top: 4px;

    color: #2be48e;

    font-size: 13px;

    font-weight: 700;

}


.connection {

    margin-top: 4px;

    color: #719bd4;

    font-size: 11px;

}


.metrics {

    display: grid;

    grid-template-columns:
        repeat(4, 1fr);

    gap: 9px;

    margin-top: 22px;

}


.metric {

    padding: 13px;

    min-height: 72px;

    border-radius: 11px;

    background:
        rgba(11,35,70,.72);

    border:
        1px solid
        rgba(100,150,220,.13);

}


.label {

    color: #78a5e5;

    font-size: 10px;

    margin-bottom: 7px;

}


.value {

    font-size: 16px;

    font-weight: 700;

}


.analysis {

    margin-top: 14px;

    padding: 17px;

    border-radius: 13px;

    background:
        linear-gradient(
            100deg,
            #12529f,
            #0b3977
        );

}


.analysis-label {

    color: #82b0f5;

    font-size: 10px;

    font-weight: 700;

}


.signal {

    margin-top: 4px;

    font-size: 27px;

    font-weight: 800;

}


.confidence {

    margin-top: 5px;

    font-size: 14px;

}


.score {

    margin-top: 7px;

    font-size: 14px;

    font-weight: 700;

}


.liquidity {

    margin-top: 13px;

    padding: 12px;

    border-radius: 10px;

    background:
        rgba(4,17,37,.50);

    font-size: 12px;

    color: #9bb9e8;

}


.error {

    margin-top: 12px;

    color: #ffb3b3;

    font-size: 12px;

}


.footer {

    text-align: center;

    color: #6489ba;

    font-size: 11px;

}


@media(max-width:800px) {

    .metrics {

        grid-template-columns:
            repeat(2,1fr);

    }

}


@media(max-width:480px) {

    .page {

        width: 95%;

    }

    .card {

        padding: 17px;

    }

    .price {

        font-size: 31px;

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
    </p>

</div>


<div id="gold"></div>

<div id="oil"></div>


<div class="footer">

    LIVE MARKET DATA • TRADING-AI

</div>


</div>


<script>


function fmt(value, decimals=3) {

    if (
        value === null ||
        value === undefined
    ) {
        return "—";
    }

    const n = Number(value);

    if (Number.isNaN(n)) {
        return "—";
    }

    return n.toFixed(decimals);
}


function renderCard(
    asset,
    data
) {

    const gold =
        asset === "gold";


    const title =
        gold
        ? "🥇 Gold — XAU/USD"
        : "🛢️ Crude Oil — WTI";


    const unavailable =
        data.connection === "PLAN LIMIT";


    return `

    <div class="card">

        <div class="asset">
            ${title}
        </div>


        <div class="price">

            ${
                unavailable
                ? "—"
                : fmt(data.price)
            }

        </div>


        <div class="live">

            ${
                unavailable
                ? "● PLAN LIMIT"
                : "● LIVE"
            }

        </div>


        <div class="connection">

            Connection:
            ${data.connection || "CONNECTING"}

        </div>


        <div class="metrics">


            <div class="metric">

                <div class="label">
                    TREND
                </div>

                <div class="value">
                    ${data.trend}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    MOMENTUM
                </div>

                <div class="value">
                    ${data.momentum}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    STRUCTURE
                </div>

                <div class="value">
                    ${data.structure}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    RSI
                </div>

                <div class="value">
                    ${fmt(data.rsi,2)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    EMA 20
                </div>

                <div class="value">
                    ${fmt(data.ema20)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    EMA 50
                </div>

                <div class="value">
                    ${fmt(data.ema50)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    SUPPORT
                </div>

                <div class="value">
                    ${fmt(data.support)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    RESISTANCE
                </div>

                <div class="value">
                    ${fmt(data.resistance)}
                </div>

            </div>


        </div>


        <div class="liquidity">

            Liquidity High:
            ${fmt(data.liquidity_high)}

            &nbsp; | &nbsp;

            Liquidity Low:
            ${fmt(data.liquidity_low)}

            <br><br>

            Sweep:
            ${data.sweep || "NONE"}

        </div>


        <div class="analysis">

            <div class="analysis-label">

                TRADING-AI ANALYSIS

            </div>


            <div class="signal">

                ${data.signal || "WAIT"}

            </div>


            <div class="confidence">

                Confidence:
                ${data.confidence || 0}%

            </div>


            <div class="score">

                AI SCORE:
                ${data.score ?? 0}

            </div>

        </div>


        ${
            data.error
            ? `
                <div class="error">
                    ${data.error}
                </div>
              `
            : ""
        }


    </div>

    `;

}


function render(data) {

    document.getElementById(
        "gold"
    ).innerHTML =
        renderCard(
            "gold",
            data.gold
        );


    document.getElementById(
        "oil"
    ).innerHTML =
        renderCard(
            "oil",
            data.oil
        );

}


/* -------------------------------------------------------
   Initial state
------------------------------------------------------- */

fetch(
    "/api/market",
    {
        cache: "no-store"
    }
)
.then(
    response => response.json()
)
.then(
    data => render(data)
)
.catch(
    error =>
        console.error(
            "Initial market error:",
            error
        )
);


/* -------------------------------------------------------
   LIVE SSE
------------------------------------------------------- */

let source;


function connectStream() {

    source = new EventSource(
        "/stream"
    );


    source.addEventListener(
        "market",
        function(event) {

            try {

                const data =
                    JSON.parse(
                        event.data
                    );

                render(data);

            } catch(error) {

                console.error(
                    "SSE parse error:",
                    error
                );

            }

        }
    );


    source.onerror =
        function() {

            /*
             Browser EventSource
             automatically reconnects.
             */

            console.log(
                "SSE reconnecting..."
            );

        };

}


connectStream();

</script>


</body>

</html>
"""


# =========================================================
# START APP
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    start_workers()

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
