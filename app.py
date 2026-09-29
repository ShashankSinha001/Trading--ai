from flask import Flask, render_template_string, Response
import os
import json
import time
import threading
import queue
import websocket
import requests

app = Flask(**name**)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

latest = {
"gold": {
"price": None,
"status": "CONNECTING"
},
"oil": {
"price": None,
"status": "UNAVAILABLE"
}
}

clients = []
clients_lock = threading.Lock()
state_lock = threading.Lock()
engine_lock = threading.Lock()

engine_started = False

# =========================================================

# BROADCAST

# =========================================================

def broadcast(data):

```
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
```

# =========================================================

# UPDATE PRICE

# =========================================================

def update_price(asset, symbol, price):

```
with state_lock:

    latest[asset]["price"] = price
    latest[asset]["status"] = "LIVE"

broadcast({
    "type": "price",
    "asset": asset,
    "symbol": symbol,
    "price": price,
    "timestamp": time.time()
})
```

# =========================================================

# GOLD WEBSOCKET

# =========================================================

def gold_worker():

```
print(
    "GOLD ENGINE STARTING",
    flush=True
)

if not API_KEY:

    print(
        "TWELVE_DATA_API_KEY MISSING",
        flush=True
    )

    with state_lock:
        latest["gold"]["status"] = "API KEY MISSING"

    return

while True:

    try:

        with state_lock:
            latest["gold"]["status"] = "CONNECTING"

        broadcast({
            "type": "gold_status",
            "status": "CONNECTING"
        })

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

            with state_lock:
                latest["gold"]["status"] = "CONNECTED"

            broadcast({
                "type": "gold_status",
                "status": "CONNECTED"
            })

        def on_message(ws, message):

            try:

                print(
                    "GOLD MESSAGE:",
                    message,
                    flush=True
                )

                data = json.loads(message)

                event = data.get("event")

                if event == "price":

                    symbol = data.get(
                        "symbol",
                        "XAU/USD"
                    )

                    price = data.get("price")

                    if price is not None:

                        update_price(
                            "gold",
                            symbol,
                            float(price)
                        )

                elif event == "subscribe-status":

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

            with state_lock:
                latest["gold"]["status"] = "RECONNECTING"

            broadcast({
                "type": "gold_status",
                "status": "RECONNECTING"
            })

        def on_close(ws, code, message):

            print(
                "GOLD WEBSOCKET CLOSED:",
                code,
                message,
                flush=True
            )

            with state_lock:
                latest["gold"]["status"] = "RECONNECTING"

            broadcast({
                "type": "gold_status",
                "status": "RECONNECTING"
            })

        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        print(
            "CONNECTING TO TWELVE DATA GOLD",
            flush=True
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
```

# =========================================================

# OIL STATUS

# =========================================================

def oil_worker():

```
print(
    "OIL ENGINE: TWELVE DATA WTI NOT AVAILABLE ON CURRENT PLAN",
    flush=True
)

with state_lock:
    latest["oil"]["status"] = "DATA SOURCE REQUIRED"

broadcast({
    "type": "oil_status",
    "status": "DATA SOURCE REQUIRED"
})
```

# =========================================================

# START LIVE ENGINE

# =========================================================

def start_live_engine():

```
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
```

# =========================================================

# START ENGINE

# =========================================================

@app.before_request
def start_engine():

```
start_live_engine()
```

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
    padding: 25px 15px;
    border-bottom: 1px solid #202631;
}

.logo {
    font-size: 30px;
    font-weight: bold;
}

.subtitle {
    margin-top: 7px;
    color: #8d96a5;
}

.container {
    max-width: 1000px;
    margin: auto;
    padding: 25px;
}

.grid {
    display: grid;
    grid-template-columns:
    repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
}

.card {
    background: #0d1118;
    border: 1px solid #202631;
    border-radius: 18px;
    padding: 25px;
}

.asset {
    font-size: 21px;
    font-weight: bold;
}

.price {
    font-size: 40px;
    font-weight: bold;
    margin-top: 22px;
}

.status {
    margin-top: 15px;
    color: #f0b94b;
}

.live {
    color: #45e08b;
}

.footer {
    text-align: center;
    color: #666f7e;
    margin-top: 30px;
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

<div class="grid">

<div class="card">

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
class="status">
Connecting...
</div>

</div>

<div class="card">

<div class="asset">
🛢️ Crude Oil — WTI
</div>

<div
id="oilPrice"
class="price">
Waiting...
</div>

<div
id="oilStatus"
class="status">
Checking...
</div>

</div>

</div>

<div class="footer">
LIVE MARKET ENGINE
</div>

</div>

<script>

const goldPrice =
document.getElementById("goldPrice");

const oilPrice =
document.getElementById("oilPrice");

const goldStatus =
document.getElementById("goldStatus");

const oilStatus =
document.getElementById("oilStatus");


function setGoldStatus(text) {

    goldStatus.innerText = text;

    if (text === "LIVE") {

        goldStatus.className =
        "status live";

    } else {

        goldStatus.className =
        "status";

    }
}


function setOilStatus(text) {

    oilStatus.innerText = text;

    if (text === "LIVE") {

        oilStatus.className =
        "status live";

    } else {

        oilStatus.className =
        "status";

    }
}


const stream =
new EventSource("/stream");


stream.onopen = function() {

    console.log(
        "TRADING-AI STREAM CONNECTED"
    );

};


stream.onerror = function() {

    console.log(
        "TRADING-AI STREAM ERROR / RECONNECTING"
    );

};


stream.onmessage = function(event) {

    console.log(
        "SSE DATA:",
        event.data
    );

    try {

        const data =
        JSON.parse(event.data);


        if (data.type === "initial") {

            if (data.gold.price !== null) {

                goldPrice.innerText =
                Number(data.gold.price)
                .toFixed(3);

            }

            setGoldStatus(
                data.gold.status
            );


            if (data.oil.price !== null) {

                oilPrice.innerText =
                Number(data.oil.price)
                .toFixed(3);

            }

            setOilStatus(
                data.oil.status
            );

        }


        if (data.type === "price") {

            if (data.asset === "gold") {

                goldPrice.innerText =
                Number(data.price)
                .toFixed(3);

                setGoldStatus(
                    "LIVE"
                );

            }


            if (data.asset === "oil") {

                oilPrice.innerText =
                Number(data.price)
                .toFixed(3);

                setOilStatus(
                    "LIVE"
                );

            }

        }


        if (data.type === "gold_status") {

            setGoldStatus(
                data.status
            );

        }


        if (data.type === "oil_status") {

            setOilStatus(
                data.status
            );

        }

    }

    catch(error) {

        console.log(
            "SSE JSON ERROR:",
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

```
return render_template_string(
    HTML
)
```

# =========================================================

# SSE STREAM

# =========================================================

@app.route("/stream")
def stream():

```
client_queue = queue.Queue()

with clients_lock:

    clients.append(
        client_queue
    )


def generate():

    try:

        with state_lock:

            initial = {
                "type": "initial",
                "gold": dict(
                    latest["gold"]
                ),
                "oil": dict(
                    latest["oil"]
                )
            }


        initial_message = json.dumps(
            initial,
            separators=(",", ":")
        )

        # IMPORTANT:
        # SSE requires a REAL blank line after each event.
        yield (
            "data: "
            + initial_message
            + "\n\n"
        )


        while True:

            try:

                message = client_queue.get(
                    timeout=15
                )

                # IMPORTANT:
                # Use actual newline characters.
                yield (
                    "data: "
                    + message
                    + "\n\n"
                )

            except queue.Empty:

                # SSE keepalive comment
                yield ": keepalive\n\n"

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
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive"
    }

)
```

# =========================================================

# HEALTH

# =========================================================

@app.route("/health")
def health():

```
return {
    "status": "ok",
    "live_engine": engine_started,
    "api_key": bool(API_KEY)
}
```

# =========================================================

# LOCAL START

# =========================================================

if **name** == "**main**":

```
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
```
