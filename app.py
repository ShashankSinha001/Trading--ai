from flask import Flask, render_template_string, Response
import os
import json
import time
import threading
import queue
import websocket
import requests

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

latest = {
    "gold": {
        "price": None,
        "status": "CONNECTING"
    },
    "oil": {
        "price": None,
        "status": "DATA SOURCE REQUIRED"
    }
}

history = []

clients = []
clients_lock = threading.Lock()
state_lock = threading.Lock()
engine_lock = threading.Lock()

engine_started = False


# =========================================================
# BROADCAST
# =========================================================

def broadcast(data):

    message = json.dumps(
        data,
        separators=(",", ":")
    )

    with clients_lock:

        for client_queue in list(clients):

            try:
                client_queue.put_nowait(message)

            except Exception:
                pass


# =========================================================
# ANALYSIS ENGINE
# =========================================================

def calculate_analysis():

    with state_lock:

        prices = list(history)

    if len(prices) < 10:

        return {
            "trend": "CALCULATING",
            "momentum": "CALCULATING",
            "structure": "BUILDING DATA",
            "support": None,
            "resistance": None,
            "ema20": None,
            "ema50": None,
            "rsi": None,
            "signal": "WAIT",
            "confidence": 0,
            "summary": "Collecting market data..."
        }

    recent = prices[-100:]

    # -------------------------
    # EMA
    # -------------------------

    def ema(values, period):

        if len(values) < period:
            return sum(values) / len(values)

        multiplier = 2 / (period + 1)

        value = sum(values[:period]) / period

        for price in values[period:]:

            value = (
                (price - value) * multiplier
                + value
            )

        return value

    ema20 = ema(recent, 20)

    ema50 = ema(recent, 50)


    # -------------------------
    # RSI
    # -------------------------

    changes = []

    for i in range(1, len(recent)):

        changes.append(
            recent[i] - recent[i - 1]
        )

    gains = [
        x for x in changes
        if x > 0
    ]

    losses = [
        abs(x) for x in changes
        if x < 0
    ]

    avg_gain = (
        sum(gains) / len(gains)
        if gains else 0
    )

    avg_loss = (
        sum(losses) / len(losses)
        if losses else 0
    )

    if avg_loss == 0:

        rsi = 100

    else:

        rs = avg_gain / avg_loss

        rsi = 100 - (
            100 / (1 + rs)
        )


    # -------------------------
    # SUPPORT / RESISTANCE
    # -------------------------

    support = min(recent)

    resistance = max(recent)

    current = recent[-1]


    # -------------------------
    # TREND
    # -------------------------

    if current > ema20 > ema50:

        trend = "BULLISH"

    elif current < ema20 < ema50:

        trend = "BEARISH"

    else:

        trend = "SIDEWAYS"


    # -------------------------
    # MOMENTUM
    # -------------------------

    if rsi >= 60:

        momentum = "STRONG BUYING"

    elif rsi >= 52:

        momentum = "BUYING"

    elif rsi <= 40:

        momentum = "STRONG SELLING"

    elif rsi <= 48:

        momentum = "SELLING"

    else:

        momentum = "NEUTRAL"


    # -------------------------
    # MARKET STRUCTURE
    # -------------------------

    if len(recent) >= 10:

        old = sum(recent[-10:-5]) / 5
        new = sum(recent[-5:]) / 5

        if new > old:

            structure = "HIGHER"

        elif new < old:

            structure = "LOWER"

        else:

            structure = "RANGE"

    else:

        structure = "BUILDING"


    # -------------------------
    # SIGNAL
    # -------------------------

    score = 0

    if trend == "BULLISH":
        score += 2

    elif trend == "BEARISH":
        score -= 2


    if momentum == "STRONG BUYING":
        score += 2

    elif momentum == "BUYING":
        score += 1

    elif momentum == "STRONG SELLING":
        score -= 2

    elif momentum == "SELLING":
        score -= 1


    if structure == "HIGHER":
        score += 1

    elif structure == "LOWER":
        score -= 1


    if score >= 4:

        signal = "BUY"
        confidence = min(90, 60 + score * 5)

    elif score <= -4:

        signal = "SELL"
        confidence = min(90, 60 + abs(score) * 5)

    else:

        signal = "WAIT"
        confidence = 50


    # -------------------------
    # SUMMARY
    # -------------------------

    if signal == "BUY":

        summary = (
            "Buying pressure is currently stronger "
            "than selling pressure."
        )

    elif signal == "SELL":

        summary = (
            "Selling pressure is currently stronger "
            "than buying pressure."
        )

    else:

        summary = (
            "Market conditions are mixed. "
            "Waiting for stronger confirmation."
        )


    return {

        "trend": trend,

        "momentum": momentum,

        "structure": structure,

        "support": round(
            support,
            3
        ),

        "resistance": round(
            resistance,
            3
        ),

        "ema20": round(
            ema20,
            3
        ),

        "ema50": round(
            ema50,
            3
        ),

        "rsi": round(
            rsi,
            2
        ),

        "signal": signal,

        "confidence": confidence,

        "summary": summary
    }


# =========================================================
# UPDATE GOLD
# =========================================================

def update_gold(price):

    with state_lock:

        latest["gold"]["price"] = price
        latest["gold"]["status"] = "LIVE"

        history.append(price)

        if len(history) > 500:

            del history[:-500]

    analysis = calculate_analysis()

    broadcast({

        "type": "gold_update",

        "price": price,

        "analysis": analysis,

        "timestamp": time.time()
    })


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_worker():

    print(
        "GOLD ENGINE STARTING",
        flush=True
    )

    if not API_KEY:

        print(
            "TWELVE_DATA_API_KEY MISSING",
            flush=True
        )

        return

    while True:

        try:

            url = (
                "wss://ws.twelvedata.com/v1/quotes/price?apikey="
                + API_KEY
            )

            def on_open(ws):

                print(
                    "TWELVE DATA GOLD CONNECTED",
                    flush=True
                )

                subscribe = {

                    "action": "subscribe",

                    "params": {
                        "symbols": "XAU/USD"
                    }
                }

                ws.send(
                    json.dumps(subscribe)
                )

            def on_message(ws, message):

                try:

                    data = json.loads(message)

                    if data.get("event") == "price":

                        price = data.get("price")

                        if price is not None:

                            print(
                                "GOLD:",
                                price,
                                flush=True
                            )

                            update_gold(
                                float(price)
                            )

                    elif data.get(
                        "event"
                    ) == "subscribe-status":

                        print(
                            "GOLD SUBSCRIPTION:",
                            data,
                            flush=True
                        )

                except Exception as error:

                    print(
                        "GOLD MESSAGE ERROR:",
                        error,
                        flush=True
                    )

            def on_error(ws, error):

                print(
                    "GOLD WEBSOCKET ERROR:",
                    error,
                    flush=True
                )

            def on_close(ws, code, message):

                print(
                    "GOLD WEBSOCKET CLOSED:",
                    code,
                    message,
                    flush=True
                )

            ws = websocket.WebSocketApp(

                url,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            ws.run_forever(

                ping_interval=20,

                ping_timeout=10
            )

        except Exception as error:

            print(
                "GOLD ENGINE ERROR:",
                error,
                flush=True
            )

        time.sleep(5)


# =========================================================
# OIL PLACEHOLDER
# =========================================================

def oil_worker():

    print(
        "OIL ENGINE STARTING",
        flush=True
    )


# =========================================================
# START ENGINE
# =========================================================

def start_live_engine():

    global engine_started

    with engine_lock:

        if engine_started:

            return

        engine_started = True

        threading.Thread(
            target=gold_worker,
            daemon=True
        ).start()

        threading.Thread(
            target=oil_worker,
            daemon=True
        ).start()

        print(
            "LIVE ENGINE STARTED",
            flush=True
        )


@app.before_request
def start_engine():

    start_live_engine()


# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1">

<title>Trading-AI</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background: #05070b;

    color: white;

    font-family: Arial, sans-serif;
}

.header {

    text-align: center;

    padding: 25px;

    border-bottom:
    1px solid #202631;
}

.logo {

    font-size: 30px;

    font-weight: bold;
}

.subtitle {

    color: #8d96a5;

    margin-top: 7px;
}

.container {

    max-width: 1100px;

    margin: auto;

    padding: 25px;
}

.price-card {

    background: #0d1118;

    border:
    1px solid #202631;

    border-radius: 18px;

    padding: 25px;

    margin-bottom: 20px;
}

.asset {

    font-size: 22px;

    font-weight: bold;
}

.price {

    font-size: 42px;

    font-weight: bold;

    margin-top: 15px;
}

.live {

    color: #45e08b;

    margin-top: 10px;
}

.analysis {

    display: grid;

    grid-template-columns:
    repeat(
        auto-fit,
        minmax(180px, 1fr)
    );

    gap: 15px;

    margin-top: 20px;
}

.box {

    background: #111722;

    border:
    1px solid #202631;

    border-radius: 14px;

    padding: 18px;
}

.label {

    color: #8d96a5;

    font-size: 13px;

    text-transform: uppercase;
}

.value {

    font-size: 20px;

    font-weight: bold;

    margin-top: 8px;
}

.signal {

    margin-top: 20px;

    padding: 20px;

    border:
    1px solid #202631;

    border-radius: 15px;

    background: #111722;
}

.signal-title {

    color: #8d96a5;

    font-size: 13px;
}

.signal-value {

    font-size: 30px;

    font-weight: bold;

    margin-top: 8px;
}

.summary {

    margin-top: 10px;

    color: #c5cbd5;

    line-height: 1.5;
}

</style>

</head>

<body>

<div class="header">

<div class="logo">
Trading-AI
</div>

<div class="subtitle">
Real-Time Market Intelligence
</div>

</div>

<div class="container">

<div class="price-card">

<div class="asset">
🥇 Gold — XAU/USD
</div>

<div
id="goldPrice"
class="price">
Waiting...
</div>

<div
id="goldStatus"
class="live">
Connecting...
</div>

<div class="analysis">

<div class="box">

<div class="label">
Trend
</div>

<div
id="trend"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
Momentum
</div>

<div
id="momentum"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
Structure
</div>

<div
id="structure"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
RSI
</div>

<div
id="rsi"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
EMA 20
</div>

<div
id="ema20"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
EMA 50
</div>

<div
id="ema50"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
Support
</div>

<div
id="support"
class="value">
--
</div>

</div>

<div class="box">

<div class="label">
Resistance
</div>

<div
id="resistance"
class="value">
--
</div>

</div>

</div>

<div class="signal">

<div class="signal-title">
TRADING-AI ANALYSIS
</div>

<div
id="signal"
class="signal-value">
WAIT
</div>

<div
id="confidence"
class="value">
Confidence: --
</div>

<div
id="summary"
class="summary">
Collecting market data...
</div>

</div>

</div>


<div class="price-card">

<div class="asset">
🛢️ Crude Oil — WTI
</div>

<div
class="price">
--
</div>

<div class="live">
DATA SOURCE REQUIRED
</div>

</div>

</div>


<script>

const goldPrice =
document.getElementById(
    "goldPrice"
);

const goldStatus =
document.getElementById(
    "goldStatus"
);

const trend =
document.getElementById(
    "trend"
);

const momentum =
document.getElementById(
    "momentum"
);

const structure =
document.getElementById(
    "structure"
);

const rsi =
document.getElementById(
    "rsi"
);

const ema20 =
document.getElementById(
    "ema20"
);

const ema50 =
document.getElementById(
    "ema50"
);

const support =
document.getElementById(
    "support"
);

const resistance =
document.getElementById(
    "resistance"
);

const signal =
document.getElementById(
    "signal"
);

const confidence =
document.getElementById(
    "confidence"
);

const summary =
document.getElementById(
    "summary"
);


function updateAnalysis(data) {

    goldPrice.innerText =
        Number(data.price)
        .toFixed(3);

    goldStatus.innerText =
        "LIVE";

    const a =
        data.analysis;

    trend.innerText =
        a.trend;

    momentum.innerText =
        a.momentum;

    structure.innerText =
        a.structure;

    rsi.innerText =
        a.rsi === null
        ? "--"
        : a.rsi;

    ema20.innerText =
        a.ema20 === null
        ? "--"
        : a.ema20;

    ema50.innerText =
        a.ema50 === null
        ? "--"
        : a.ema50;

    support.innerText =
        a.support === null
        ? "--"
        : a.support;

    resistance.innerText =
        a.resistance === null
        ? "--"
        : a.resistance;

    signal.innerText =
        a.signal;

    confidence.innerText =
        "Confidence: "
        + a.confidence
        + "%";

    summary.innerText =
        a.summary;
}


const stream =
new EventSource(
    "/stream"
);


stream.onopen = function() {

    console.log(
        "TRADING-AI STREAM CONNECTED"
    );

};


stream.onmessage =
function(event) {

    try {

        const data =
            JSON.parse(
                event.data
            );

        if (
            data.type ===
            "gold_update"
        ) {

            updateAnalysis(data);

        }

    }

    catch(error) {

        console.log(
            "STREAM ERROR:",
            error
        );

    }

};

</script>

</body>

</html>
"""


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    return render_template_string(
        HTML
    )


# =========================================================
# STREAM
# =========================================================

@app.route("/stream")
def stream():

    client_queue = queue.Queue()

    with clients_lock:

        clients.append(
            client_queue
        )

    def generate():

        try:

            while True:

                try:

                    message = client_queue.get(
                        timeout=15
                    )

                    yield (
                        "data: "
                        + message
                        + "\n\n"
                    )

                except queue.Empty:

                    yield (
                        ": keepalive\n\n"
                    )

        finally:

            with clients_lock:

                if client_queue in clients:

                    clients.remove(
                        client_queue
                    )

    return Response(

        generate(),

        mimetype=
        "text/event-stream",

        headers={

            "Cache-Control":
            "no-cache",

            "X-Accel-Buffering":
            "no",

            "Connection":
            "keep-alive"
        }
    )


# =========================================================
# HEALTH
# =========================================================

@app.route("/health")
def health():

    return {

        "status": "ok",

        "live_engine":
        engine_started,

        "api_key":
        bool(API_KEY),

        "gold_history":
        len(history)
    }


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.environ.get(
                "PORT",
                10000
            )
        ),

        threaded=True
    )
