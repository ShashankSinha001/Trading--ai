```python
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

# Assets
SYMBOLS = {
    "XAU/USD": "gold",
    "WTI/USD": "oil"
}

latest = {
    "gold": {
        "price": None,
        "status": "CONNECTING"
    },
    "oil": {
        "price": None,
        "status": "CONNECTING"
    }
}

clients = []
clients_lock = threading.Lock()
state_lock = threading.Lock()

engine_started = False
engine_lock = threading.Lock()


# ---------------------------------------------------------
# BROADCAST
# ---------------------------------------------------------

def broadcast(data):
    message = json.dumps(data)

    with clients_lock:
        for client_queue in list(clients):
            try:
                client_queue.put_nowait(message)
            except Exception:
                pass


# ---------------------------------------------------------
# STATUS
# ---------------------------------------------------------

def update_status(status):
    with state_lock:
        latest["gold"]["status"] = status

        # Oil status is managed separately by REST engine

    broadcast({
        "type": "status",
        "status": status
    })


# ---------------------------------------------------------
# UPDATE PRICE
# ---------------------------------------------------------

def update_price(symbol, price, timestamp=None):

    asset = SYMBOLS.get(symbol)

    if not asset:
        return

    with state_lock:
        latest[asset]["price"] = price
        latest[asset]["status"] = "LIVE"

    broadcast({
        "type": "price",
        "asset": asset,
        "symbol": symbol,
        "price": price,
        "timestamp": timestamp or time.time()
    })


# ---------------------------------------------------------
# GOLD WEBSOCKET HEARTBEAT
# ---------------------------------------------------------

def heartbeat(ws):

    while True:

        try:

            time.sleep(10)

            if ws.sock and ws.sock.connected:

                ws.send(
                    json.dumps({
                        "action": "heartbeat"
                    })
                )

        except Exception:
            break


# ---------------------------------------------------------
# GOLD WEBSOCKET
# ---------------------------------------------------------

def websocket_worker():

    print("GOLD WEBSOCKET ENGINE STARTING", flush=True)

    if not API_KEY:

        print("API KEY MISSING", flush=True)

        with state_lock:
            latest["gold"]["status"] = "API KEY MISSING"

        return

    while True:

        try:

            with state_lock:
                latest["gold"]["status"] = "CONNECTING"

            broadcast({
                "type": "status",
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

                subscribe_message = {
                    "action": "subscribe",
                    "params": {
                        "symbols": "XAU/USD"
                    }
                }

                ws.send(
                    json.dumps(subscribe_message)
                )

                with state_lock:
                    latest["gold"]["status"] = "CONNECTED"

                broadcast({
                    "type": "status",
                    "status": "CONNECTED"
                })

                threading.Thread(
                    target=heartbeat,
                    args=(ws,),
                    daemon=True
                ).start()

            def on_message(ws, message):

                try:

                    print(
                        "TWELVE DATA MESSAGE:",
                        message,
                        flush=True
                    )

                    data = json.loads(message)

                    event = data.get("event")

                    if event == "price":

                        symbol = data.get("symbol")

                        price = data.get("price")

                        timestamp = data.get("timestamp")

                        if symbol and price is not None:

                            update_price(
                                symbol,
                                float(price),
                                timestamp
                            )

                    elif event == "subscribe-status":

                        broadcast({
                            "type": "subscription",
                            "data": data
                        })

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

            def on_close(ws, close_status_code, close_msg):

                print(
                    "GOLD WEBSOCKET CLOSED:",
                    close_status_code,
                    close_msg,
                    flush=True
                )

                with state_lock:
                    latest["gold"]["status"] = "RECONNECTING"

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


# ---------------------------------------------------------
# OIL REST API ENGINE
# ---------------------------------------------------------

def oil_price_worker():

    print(
        "OIL REST API ENGINE STARTING",
        flush=True
    )

    if not API_KEY:

        print(
            "OIL API KEY MISSING",
            flush=True
        )

        with state_lock:
            latest["oil"]["status"] = "API KEY MISSING"

        return

    while True:

        try:

            url = "https://api.twelvedata.com/price"

            params = {
                "symbol": "WTI/USD",
                "apikey": API_KEY,
                "dp": 5
            }

            response = requests.get(
                url,
                params=params,
                timeout=15
            )

            data = response.json()

            print(
                "OIL API RESPONSE:",
                data,
                flush=True
            )

            if "price" in data:

                price = float(data["price"])

                update_price(
                    "WTI/USD",
                    price,
                    time.time()
                )

                with state_lock:
                    latest["oil"]["status"] = "LIVE"

            else:

                error_message = data.get(
                    "message",
                    "OIL DATA UNAVAILABLE"
                )

                print(
                    "OIL API ERROR:",
                    error_message,
                    flush=True
                )

                with state_lock:
                    latest["oil"]["status"] = "UNAVAILABLE"

                broadcast({
                    "type": "oil-error",
                    "message": error_message
                })

        except Exception as error:

            print(
                "OIL ENGINE ERROR:",
                error,
                flush=True
            )

            with state_lock:
                latest["oil"]["status"] = "RECONNECTING"

        # Basic plan has 8 API credits/minute.
        # 15 seconds = maximum 4 requests/minute.
        time.sleep(15)


# ---------------------------------------------------------
# START ENGINES
# ---------------------------------------------------------

def start_live_engine():

    global engine_started

    with engine_lock:

        if engine_started:
            return

        engine_started = True

        gold_thread = threading.Thread(
            target=websocket_worker,
            daemon=True
        )

        gold_thread.start()

        oil_thread = threading.Thread(
            target=oil_price_worker,
            daemon=True
        )

        oil_thread.start()

        print(
            "LIVE ENGINE STARTED",
            flush=True
        )


# ---------------------------------------------------------
# START BEFORE REQUEST
# ---------------------------------------------------------

@app.before_request
def ensure_live_engine():

    start_live_engine()


# ---------------------------------------------------------
# HTML
# ---------------------------------------------------------

HTML = """
<!DOCTYPE html>

<html>

<head>

<meta name="viewport"
content="width=device-width, initial-scale=1">

<title>Trading-AI</title>

<style>

body {
    margin: 0;
    background: #05070b;
    color: white;
    font-family: Arial, sans-serif;
}

.header {
    padding: 22px;
    text-align: center;
    border-bottom: 1px solid #20242c;
}

.logo {
    font-size: 28px;
    font-weight: bold;
}

.subtitle {
    color: #8d96a5;
    margin-top: 6px;
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
    padding: 24px;
}

.asset {
    font-size: 22px;
    font-weight: bold;
}

.price {
    font-size: 38px;
    margin-top: 20px;
    font-weight: bold;
}

.status {
    margin-top: 15px;
    color: #8d96a5;
}

.live {
    color: #45e08b;
}

.waiting {
    color: #f0b94b;
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

<div id="goldPrice"
class="price">
Waiting...
</div>

<div id="goldStatus"
class="status waiting">
Connecting...
</div>

</div>


<div class="card">

<div class="asset">
🛢️ Crude Oil — WTI
</div>

<div id="oilPrice"
class="price">
Waiting...
</div>

<div id="oilStatus"
class="status waiting">
Connecting...
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
        "status waiting";
    }
}


function setOilStatus(text) {

    oilStatus.innerText = text;

    if (text === "LIVE") {

        oilStatus.className =
        "status live";

    } else {

        oilStatus.className =
        "status waiting";
    }
}


const stream =
new EventSource("/stream");


stream.onopen = function() {

    console.log(
        "Trading-AI stream connected"
    );

};


stream.onerror = function() {

    console.log(
        "Trading-AI stream reconnecting..."
    );

};


stream.onmessage = function(event) {

    try {

        const data =
        JSON.parse(event.data);


        if (data.type === "initial") {

            if (data.gold.price !== null) {

                goldPrice.innerText =
                Number(data.gold.price)
                .toFixed(3);

            }

            if (data.oil.price !== null) {

                oilPrice.innerText =
                Number(data.oil.price)
                .toFixed(3);

            }

            setGoldStatus(
                data.gold.status
            );

            setOilStatus(
                data.oil.status
            );

        }


        if (data.type === "price") {

            if (data.asset === "gold") {

                goldPrice.innerText =
                Number(data.price)
                .toFixed(3);

                setGoldStatus("LIVE");

            }


            if (data.asset === "oil") {

                oilPrice.innerText =
                Number(data.price)
                .toFixed(3);

                setOilStatus("LIVE");

            }

        }


        if (data.type === "status") {

            setGoldStatus(
                data.status
            );

        }


        if (data.type === "oil-error") {

            setOilStatus(
                "DATA ERROR"
            );

        }

    }

    catch(error) {

        console.log(
            "STREAM PARSE ERROR",
            error
        );

    }

};

</script>

</body>

</html>
"""


# ---------------------------------------------------------
# HOME
# ---------------------------------------------------------

@app.route("/")
def home():

    return render_template_string(
        HTML
    )


# ---------------------------------------------------------
# STREAM
# ---------------------------------------------------------

@app.route("/stream")
def stream():

    client_queue = queue.Queue()

    with clients_lock:

        clients.append(
            client_queue
        )

    def generate():

        try:

            with state_lock:

                initial_state = {
                    "type": "initial",
                    "gold": dict(
                        latest["gold"]
                    ),
                    "oil": dict(
                        latest["oil"]
                    )
                }

            yield (
                "data: "
                + json.dumps(initial_state)
                + "\\n\\n"
            )

            while True:

                try:

                    message = client_queue.get(
                        timeout=15
                    )

                    yield (
                        "data: "
                        + message
                        + "\\n\\n"
                    )

                except queue.Empty:

                    yield ": keepalive\\n\\n"

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


# ---------------------------------------------------------
# HEALTH
# ---------------------------------------------------------

@app.route("/health")
def health():

    return {
        "status": "ok",
        "live_engine": engine_started,
        "api_key": bool(API_KEY)
    }


# ---------------------------------------------------------
# LOCAL START
# ---------------------------------------------------------

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
```



### Ek aur important change

`requests` library use ki hai. Isliye GitHub ki **`requirements.txt`** me ye line honi chahiye:

```text id="v1m2hx"
Flask
gunicorn
websocket-client
requests
```

**Start Command ko bilkul mat badalna:**

```text
gunicorn --workers 1 --threads 8 --timeout 0 --bind 0.0.0.0:$PORT app:app
```

Ab pehle **`app.py` save/commit** karo. Uske baad **`requirements.txt` save/commit** karo. Render automatically deploy karega.

Oil API ko 15-second interval par rakha hai, yani maximum 4 oil requests/minute—tumhare 8 API credits/minute limit ke andar.
