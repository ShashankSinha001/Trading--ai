from flask import Flask, render_template_string, Response
import os
import json
import time
import threading
import queue
import websocket

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

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


def broadcast(data):
    message = json.dumps(data)

    with clients_lock:
        for client_queue in list(clients):
            try:
                client_queue.put_nowait(message)
            except Exception:
                pass


def update_status(status):
    with state_lock:
        latest["gold"]["status"] = status
        latest["oil"]["status"] = status

    broadcast({
        "type": "status",
        "status": status
    })


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


def heartbeat(ws):

    while True:
        try:
            time.sleep(10)

            if ws.sock and ws.sock.connected:
                ws.send(json.dumps({
                    "action": "heartbeat"
                }))

        except Exception:
            break


def websocket_worker():

    if not API_KEY:
        update_status("API KEY MISSING")
        return

    while True:

        try:

            update_status("CONNECTING")

            url = (
                "wss://ws.twelvedata.com/v1/quotes/price"
                "?apikey=" + API_KEY
            )

            def on_open(ws):

                subscribe_message = {
                    "action": "subscribe",
                    "params": {
                        "symbols": "XAU/USD,WTI/USD"
                    }
                }

                ws.send(json.dumps(subscribe_message))

                update_status("CONNECTED")

                threading.Thread(
                    target=heartbeat,
                    args=(ws,),
                    daemon=True
                ).start()

            def on_message(ws, message):

                try:

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

                    print("MESSAGE ERROR:", error)

            def on_error(ws, error):

                print("WEBSOCKET ERROR:", error)
                update_status("CONNECTION ERROR")

            def on_close(ws, close_status_code, close_msg):

                print(
                    "WEBSOCKET CLOSED:",
                    close_status_code,
                    close_msg
                )

                update_status("RECONNECTING")

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

            print("ENGINE ERROR:", error)
            update_status("RECONNECTING")

        time.sleep(5)


def start_live_engine():

    global engine_started

    with engine_lock:

        if engine_started:
            return

        engine_started = True

        thread = threading.Thread(
            target=websocket_worker,
            daemon=True
        )

        thread.start()

        print("LIVE ENGINE STARTED")


# IMPORTANT:
# Gunicorn imports this file instead of running it directly.
# Therefore the live engine must start during import.
start_live_engine()


HTML = """
<!DOCTYPE html>

<html>

<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>Trading-AI Live</title>

<style>

body {
    margin: 0;
    font-family: Arial, sans-serif;
    background: #0f172a;
    color: white;
}

.container {
    max-width: 850px;
    margin: auto;
    padding: 25px 15px;
}

h1 {
    text-align: center;
    color: #60a5fa;
    margin-bottom: 5px;
}

.subtitle {
    text-align: center;
    color: #94a3b8;
    margin-bottom: 25px;
}

.live-status {
    text-align: center;
    background: #1e293b;
    border-radius: 12px;
    padding: 12px;
    margin-bottom: 20px;
}

.dot {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: #facc15;
    margin-right: 7px;
}

.card {
    background: #1e293b;
    border-radius: 16px;
    padding: 22px;
    margin-bottom: 20px;
    box-shadow: 0 5px 18px rgba(0,0,0,0.3);
}

.card h2 {
    margin-top: 0;
}

.price {
    font-size: 38px;
    font-weight: bold;
    margin: 15px 0;
}

.live-label {
    font-size: 13px;
    color: #22c55e;
    font-weight: bold;
}

.row {
    display: flex;
    justify-content: space-between;
    padding: 10px 0;
    border-bottom: 1px solid #334155;
}

.label {
    color: #94a3b8;
}

.status {
    color: #22c55e;
    font-weight: bold;
}

.info {
    color: #94a3b8;
    text-align: center;
    margin-top: 25px;
    font-size: 13px;
}

</style>

</head>

<body>

<div class="container">

<h1>Trading-AI</h1>

<div class="subtitle">
Gold + Crude Oil Live Market Intelligence
</div>

<div class="live-status">

<span class="dot" id="statusDot"></span>

<span id="connectionStatus">
Connecting to live market...
</span>

</div>

<div class="card">

<h2>🛢️ Crude Oil — WTI</h2>

<div class="live-label">
● LIVE STREAM
</div>

<div class="price" id="oilPrice">
Waiting...
</div>

<div class="row">
<span class="label">Connection</span>

<span class="status" id="oilStatus">
Connecting...
</span>
</div>

</div>

<div class="card">

<h2>🥇 Gold</h2>

<div class="live-label">
● LIVE STREAM
</div>

<div class="price" id="goldPrice">
Waiting...
</div>

<div class="row">

<span class="label">Connection</span>

<span class="status" id="goldStatus">
Connecting...
</span>

</div>

</div>

<div class="info">

Trading-AI Live Engine<br>
Price updates are pushed automatically — no page refresh required.

</div>

</div>

<script>

const stream = new EventSource("/stream");

stream.onopen = function() {

    document.getElementById(
        "connectionStatus"
    ).innerText = "LIVE CONNECTION ACTIVE";

    document.getElementById(
        "statusDot"
    ).style.background = "#22c55e";

};


stream.onmessage = function(event) {

    try {

        const data = JSON.parse(event.data);

        if (data.type === "initial") {

            if (
                data.oil &&
                data.oil.price !== null
            ) {

                document.getElementById(
                    "oilPrice"
                ).innerText =
                    "$" +
                    Number(data.oil.price)
                    .toLocaleString(
                        undefined,
                        {
                            minimumFractionDigits: 2,
                            maximumFractionDigits: 2
                        }
                    );

                document.getElementById(
                    "oilStatus"
                ).innerText = "LIVE";

            }

            if (
                data.gold &&
                data.gold.price !== null
            ) {

                document.getElementById(
                    "goldPrice"
                ).innerText =
                    "$" +
                    Number(data.gold.price)
                    .toLocaleString(
                        undefined,
                        {
                            minimumFractionDigits: 2,
                            maximumFractionDigits: 2
                        }
                    );

                document.getElementById(
                    "goldStatus"
                ).innerText = "LIVE";

            }

        }


        if (data.type === "price") {

            const price =
                Number(data.price).toLocaleString(
                    undefined,
                    {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2
                    }
                );


            if (data.asset === "oil") {

                document.getElementById(
                    "oilPrice"
                ).innerText = "$" + price;

                document.getElementById(
                    "oilStatus"
                ).innerText = "LIVE";

            }


            if (data.asset === "gold") {

                document.getElementById(
                    "goldPrice"
                ).innerText = "$" + price;

                document.getElementById(
                    "goldStatus"
                ).innerText = "LIVE";

            }

        }


        if (data.type === "status") {

            document.getElementById(
                "connectionStatus"
            ).innerText = data.status;

        }

    }

    catch(error) {

        console.log(error);

    }

};


stream.onerror = function() {

    document.getElementById(
        "connectionStatus"
    ).innerText =
        "LIVE CONNECTION LOST — RECONNECTING";

    document.getElementById(
        "statusDot"
    ).style.background = "#ef4444";

};

</script>

</body>

</html>
"""


@app.route("/")
def home():

    return render_template_string(HTML)


@app.route("/stream")
def stream():

    client_queue = queue.Queue()

    with clients_lock:
        clients.append(client_queue)

    def generate():

        try:

            with state_lock:

                initial_state = {
                    "type": "initial",
                    "gold": latest["gold"],
                    "oil": latest["oil"]
                }

            yield (
                "data: "
                + json.dumps(initial_state)
                + "\n\n"
            )

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

                    yield ": keepalive\n\n"

        finally:

            with clients_lock:

                if client_queue in clients:
                    clients.remove(client_queue)


    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )


@app.route("/health")
def health():

    return {
        "status": "ok",
        "live_engine": True,
        "api_key": bool(API_KEY)
    }


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )
