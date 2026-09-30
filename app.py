import os
import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timezone

import requests
import websocket

from flask import Flask, jsonify, Response

from signal_engine import Candle, generate_signal, signal_to_dict


# ============================================================
# TRADING AI
# WebSocket-first local candle architecture
# Goal: Better information + fewer false signals
# ============================================================

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.Lock()

live_price = {
    "gold": None,
    "gold_timestamp": None,
}

connection_state = {
    "gold_ws": "DISCONNECTED",
}

local_candles = defaultdict(list)

current_1m = None
last_tick_time = None

live_1m_records = []

bootstrap_state = {
    "5min": {
        "status": "NOT_STARTED",
        "last_attempt": 0,
        "next_attempt": 0,
        "failures": 0,
    },
    "1h": {
        "status": "NOT_STARTED",
        "last_attempt": 0,
        "next_attempt": 0,
        "failures": 0,
    },
}

ai_state = {
    "decision": "WAIT",
    "confidence": 0,
    "score": 0,
    "trend": "UNKNOWN",
    "momentum": "UNKNOWN",
    "structure": "UNKNOWN",
    "liquidity": "UNKNOWN",
    "breakout": "UNKNOWN",
    "reasons": [],
    "warnings": [
        "Waiting for market data..."
    ],
    "data_status": "STARTING",
    "updated_at": None,
}

signal_history = []

_started = False
_start_lock = threading.Lock()

last_recorded_decision = None
last_recorded_time = 0


# ============================================================
# HELPERS
# ============================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def unix_minute(timestamp):
    return int(timestamp // 60) * 60


def candle_from_dict(item):
    try:
        return Candle(
            open=safe_float(item.get("open")),
            high=safe_float(item.get("high")),
            low=safe_float(item.get("low")),
            close=safe_float(item.get("close")),
            volume=safe_float(item.get("volume", 0)),
        )
    except Exception:
        return None


# ============================================================
# CANDLE AGGREGATION
# ============================================================

def aggregate_candles(source, minutes):

    if not source:
        return []

    bucket_seconds = minutes * 60
    buckets = {}

    for item in source:

        timestamp = item.get("timestamp")

        if timestamp is None:
            continue

        candle = candle_from_dict(item)

        if candle is None:
            continue

        bucket = int(timestamp // bucket_seconds) * bucket_seconds

        if bucket not in buckets:

            buckets[bucket] = {
                "timestamp": bucket,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }

        else:

            b = buckets[bucket]

            b["high"] = max(
                b["high"],
                candle.high
            )

            b["low"] = min(
                b["low"],
                candle.low
            )

            b["close"] = candle.close
            b["volume"] += candle.volume

    return [
        buckets[x]
        for x in sorted(buckets.keys())
    ]


# ============================================================
# LIVE 1-MINUTE CANDLE
# ============================================================

def update_live_candle(price, timestamp):

    global current_1m
    global last_tick_time

    minute = unix_minute(timestamp)

    last_tick_time = timestamp

    with state_lock:

        if current_1m is None:

            current_1m = {
                "timestamp": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
            }

            return

        if minute == current_1m["timestamp"]:

            current_1m["high"] = max(
                current_1m["high"],
                price
            )

            current_1m["low"] = min(
                current_1m["low"],
                price
            )

            current_1m["close"] = price

            return

        finished = current_1m.copy()

        live_1m_records.append(finished)

        if len(live_1m_records) > 1500:
            del live_1m_records[:-1500]

        current_1m = {
            "timestamp": minute,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
        }


def build_local_timeframes():

    with state_lock:

        source = list(live_1m_records)

        if current_1m is not None:
            source.append(current_1m.copy())

    if not source:
        return

    for timeframe, minutes in [
        ("1min", 1),
        ("5min", 5),
        ("15min", 15),
        ("30min", 30),
        ("1h", 60),
    ]:

        aggregated = aggregate_candles(
            source,
            minutes
        )

        candles = []

        for item in aggregated:

            candle = candle_from_dict(item)

            if candle:
                candles.append(candle)

        with state_lock:
            local_candles[timeframe] = candles[-300:]


# ============================================================
# TWELVE DATA HISTORICAL DATA
# ============================================================

def twelve_data_candles(interval, outputsize=100):

    if not API_KEY:

        print("TWELVE DATA API KEY MISSING")

        return []

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": GOLD_SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "format": "JSON",
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=15,
        )

        if response.status_code == 429:

            print(
                f"TWELVE DATA {interval}: 429 RATE LIMIT"
            )

            return []

        if response.status_code != 200:

            print(
                f"TWELVE DATA {interval}: "
                f"HTTP {response.status_code}"
            )

            return []

        data = response.json()

        if data.get("status") == "error":

            print(
                f"TWELVE DATA {interval}: "
                f"{data.get('message')}"
            )

            return []

        values = data.get("values", [])

        values = list(reversed(values))

        candles = []

        for item in values:

            candle = candle_from_dict(item)

            if candle is None:
                continue

            timestamp = None

            timestamp_text = item.get("datetime")

            if timestamp_text:

                try:

                    dt = datetime.fromisoformat(
                        timestamp_text.replace(
                            "Z",
                            "+00:00"
                        )
                    )

                    timestamp = dt.timestamp()

                except Exception:
                    timestamp = None

            candles.append(
                {
                    "timestamp": timestamp,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                }
            )

        return candles

    except Exception as e:

        print(
            f"TWELVE DATA {interval} ERROR: {e}"
        )

        return []


def bootstrap_one(interval):

    info = bootstrap_state[interval]

    now = time.time()

    if now < info["next_attempt"]:
        return False

    info["last_attempt"] = now
    info["status"] = "LOADING"

    print(
        f"BOOTSTRAP {interval}: "
        f"requesting historical data..."
    )

    values = twelve_data_candles(
        interval,
        outputsize=100
    )

    if not values:

        info["failures"] += 1
        info["status"] = "RATE_LIMITED"

        backoff = min(
            3600,
            300 * (
                2 ** min(
                    info["failures"] - 1,
                    3
                )
            )
        )

        info["next_attempt"] = now + backoff

        print(
            f"BOOTSTRAP {interval}: "
            f"failed. Next attempt in "
            f"{backoff}s."
        )

        return False

    info["status"] = "READY"
    info["failures"] = 0
    info["next_attempt"] = now + 86400

    candles = []

    for item in values:

        candle = candle_from_dict(item)

        if candle:
            candles.append(candle)

    with state_lock:
        local_candles[interval] = candles[-300:]

    print(
        f"BOOTSTRAP {interval}: "
        f"{len(candles)} candles loaded."
    )

    return True


def bootstrap_worker():

    time.sleep(15)

    bootstrap_one("5min")

    time.sleep(45)

    bootstrap_one("1h")

    print("BOOTSTRAP WORKER READY")


# ============================================================
# GOLD WEBSOCKET
# ============================================================

def gold_websocket_loop():

    while True:

        try:

            connection_state["gold_ws"] = "CONNECTING"

            print("CONNECTING GOLD WEBSOCKET...")

            ws = websocket.create_connection(
                WS_URL,
                timeout=20
            )

            connection_state["gold_ws"] = "CONNECTED"

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(
                json.dumps(subscribe_message)
            )

            print(
                "TWELVE DATA GOLD SUBSCRIBE SENT"
            )

            while True:

                message = ws.recv()

                if not message:
                    continue

                try:
                    data = json.loads(message)
                except Exception:
                    continue

                if data.get("status") == "error":

                    print(
                        "GOLD WS ERROR:",
                        data
                    )

                    break

                price = data.get("price")

                if price is None:

                    nested = data.get("data")

                    if isinstance(nested, dict):
                        price = nested.get("price")

                if price is None:
                    continue

                price = safe_float(price)

                if price <= 0:
                    continue

                timestamp = data.get("timestamp")

                if timestamp is None:
                    timestamp = time.time()

                timestamp = safe_float(
                    timestamp,
                    time.time()
                )

                with state_lock:

                    live_price["gold"] = price
                    live_price["gold_timestamp"] = timestamp

                update_live_candle(
                    price,
                    timestamp
                )

                build_local_timeframes()

        except Exception as e:

            print(
                "GOLD WEBSOCKET ERROR:",
                e
            )

        connection_state["gold_ws"] = "RECONNECTING"

        print(
            "GOLD WEBSOCKET "
            "RECONNECTING IN 5 SECONDS..."
        )

        time.sleep(5)


# ============================================================
# AI ENGINE
# ============================================================

def get_candles_for_engine(timeframe):

    with state_lock:

        return list(
            local_candles.get(
                timeframe,
                []
            )
        )


def build_ai_analysis():

    five_min = get_candles_for_engine("5min")
    fifteen_min = get_candles_for_engine("15min")
    one_hour = get_candles_for_engine("1h")

    if len(five_min) < 60:

        return {
            "decision": "WAIT",
            "confidence": 0,
            "score": 0,
            "trend": "UNKNOWN",
            "momentum": "UNKNOWN",
            "structure": "UNKNOWN",
            "liquidity": "UNKNOWN",
            "breakout": "UNKNOWN",
            "reasons": [
                "Waiting for sufficient 5-minute market data."
            ],
            "warnings": [
                f"5m candles available: "
                f"{len(five_min)} / 60"
            ],
            "data_status": "WARMING_UP",
            "updated_at": time.time(),
        }

    higher = fifteen_min

    if len(higher) < 60:
        higher = five_min

    result = generate_signal(
        five_min,
        higher_timeframe_candles=higher
    )

    output = signal_to_dict(result)

    if len(one_hour) >= 50:

        try:

            hour_result = generate_signal(
                one_hour
            )

            hour_decision = hour_result.decision

            output["hour_trend"] = hour_result.trend
            output["hour_decision"] = hour_decision

            if (
                output["decision"] == "BUY"
                and hour_decision == "SELL"
            ):

                output["decision"] = "WAIT"

                output["confidence"] = min(
                    output["confidence"],
                    60
                )

                output["warnings"].append(
                    "5m signal conflicts with 1h direction."
                )

            elif (
                output["decision"] == "SELL"
                and hour_decision == "BUY"
            ):

                output["decision"] = "WAIT"

                output["confidence"] = min(
                    output["confidence"],
                    60
                )

                output["warnings"].append(
                    "5m signal conflicts with 1h direction."
                )

        except Exception as e:

            output.setdefault(
                "warnings",
                []
            ).append(
                f"1h confirmation unavailable: {e}"
            )

    output["data_status"] = "LIVE"
    output["updated_at"] = time.time()

    return output


def ai_worker():

    global ai_state

    while True:

        try:

            result = build_ai_analysis()

            with state_lock:
                ai_state = result

            record_signal_history(result)

        except Exception as e:

            print(
                "AI ENGINE ERROR:",
                e
            )

        time.sleep(5)


# ============================================================
# SIGNAL HISTORY
# ============================================================

def record_signal_history(result):

    global last_recorded_decision
    global last_recorded_time

    decision = result.get(
        "decision",
        "WAIT"
    )

    now = time.time()

    if (
        decision == last_recorded_decision
        and now - last_recorded_time < 300
    ):
        return

    if decision == "WAIT":
        return

    item = {
        "time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "decision": decision,
        "confidence": result.get(
            "confidence",
            0
        ),
        "score": result.get(
            "score",
            0
        ),
    }

    signal_history.insert(
        0,
        item
    )

    del signal_history[30:]

    last_recorded_decision = decision
    last_recorded_time = now


# ============================================================
# TRADE PLAN
# ============================================================

def build_trade_plan():

    candles = get_candles_for_engine("5min")

    with state_lock:

        price = live_price["gold"]

        decision = ai_state.get(
            "decision",
            "WAIT"
        )

    if not candles or price is None:

        return {
            "status": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
        }

    recent = candles[-50:]

    support = min(
        c.low
        for c in recent
    )

    resistance = max(
        c.high
        for c in recent
    )

    risk_range = max(
        resistance - support,
        price * 0.002
    )

    if decision == "BUY":

        entry = price
        stop = entry - risk_range * 0.25
        target_1 = entry + risk_range * 0.50
        target_2 = entry + risk_range * 0.90

    elif decision == "SELL":

        entry = price
        stop = entry + risk_range * 0.25
        target_1 = entry - risk_range * 0.50
        target_2 = entry - risk_range * 0.90

    else:

        return {
            "status": "WAIT",
            "entry": price,
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
            "support": round(
                support,
                3
            ),
            "resistance": round(
                resistance,
                3
            ),
        }

    return {
        "status": decision,
        "entry": round(entry, 3),
        "stop_loss": round(stop, 3),
        "target_1": round(target_1, 3),
        "target_2": round(target_2, 3),
        "support": round(support, 3),
        "resistance": round(resistance, 3),
    }


# ============================================================
# SERIALIZE CANDLES
# ============================================================

def serialize_candles(timeframe):

    with state_lock:

        candles = list(
            local_candles.get(
                timeframe,
                []
            )
        )

    output = []

    for index, candle in enumerate(candles):

        output.append(
            {
                "index": index,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
        )

    return output[-200:]


# ============================================================
# MARKET PAYLOAD
# ============================================================

def build_market_payload():

    with state_lock:

        price = live_price["gold"]

        ws = connection_state["gold_ws"]

        ai = dict(ai_state)

        counts = {
            timeframe: len(values)
            for timeframe, values
            in local_candles.items()
        }

    return {
        "symbol": GOLD_SYMBOL,

        "price": price,

        "timestamp": time.time(),

        "connection": {
            "websocket": ws,
        },

        "ai": ai,

        "trade_plan": build_trade_plan(),

        "history": list(signal_history),

        "candles": {
            "1min": serialize_candles("1min"),
            "5min": serialize_candles("5min"),
            "15min": serialize_candles("15min"),
            "30min": serialize_candles("30min"),
            "1h": serialize_candles("1h"),
        },

        "candle_counts": counts,

        "api_mode": "WEBSOCKET + LOCAL CANDLES",

        "http_policy": "MINIMAL BOOTSTRAP ONLY",

        "oil": {
            "enabled": OIL_ENABLED,
            "symbol": OIL_SYMBOL,
        },
    }


# ============================================================
# DASHBOARD
# ============================================================

@app.route("/")
def home():

    return """
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
    font-family: Arial, Helvetica, sans-serif;
    background:
        radial-gradient(
            circle at top left,
            #162447,
            #07111f 55%,
            #030812
        );
    color: #ffffff;
}

.container {
    max-width: 1250px;
    margin: auto;
    padding: 24px;
}

.header {
    margin-bottom: 22px;
}

.title {
    font-size: 34px;
    font-weight: 800;
}

.subtitle {
    color: #8fa6c5;
    margin-top: 6px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(
            auto-fit,
            minmax(280px, 1fr)
        );
    gap: 18px;
}

.card {
    background: rgba(11, 25, 45, 0.88);
    border: 1px solid #203653;
    border-radius: 18px;
    padding: 20px;
    box-shadow:
        0 12px 35px rgba(0,0,0,.25);
}

.label {
    color: #8299b8;
    font-size: 13px;
    margin-bottom: 8px;
}

.price {
    font-size: 38px;
    font-weight: 800;
}

.live {
    color: #55e39a;
    font-weight: 700;
}

.value {
    font-size: 22px;
    font-weight: 700;
}

.decision {
    font-size: 34px;
    font-weight: 900;
    margin: 8px 0;
}

.reason {
    margin-top: 8px;
    color: #b8c8dc;
    font-size: 14px;
}

.warning {
    margin-top: 8px;
    color: #f0bd69;
    font-size: 14px;
}

.badge {
    display: inline-block;
    padding: 7px 12px;
    border-radius: 20px;
    background: #132842;
    color: #9fc4ee;
    font-size: 12px;
    margin-top: 8px;
}

.tf {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 15px;
}

button {
    background: #10243c;
    border: 1px solid #284563;
    color: #d9e8fb;
    padding: 9px 14px;
    border-radius: 9px;
    cursor: pointer;
}

button:hover {
    background: #183553;
}

.small {
    color: #8097b4;
    font-size: 12px;
}

table {
    width: 100%;
    border-collapse: collapse;
}

td {
    padding: 8px 4px;
    border-bottom: 1px solid #1c324d;
}

</style>

</head>

<body>

<div class="container">

<div class="header">

<div class="title">
Trading AI
</div>

<div class="subtitle">
Real-Time Market Intelligence •
Multi-Factor Market Analysis
</div>

</div>


<div class="grid">


<div class="card">

<div class="label">
🥇 Gold — XAU/USD
</div>

<div
id="price"
class="price"
>
Loading...
</div>

<div
id="live"
class="live"
>
● CONNECTING
</div>

<div
id="mode"
class="badge"
>
WebSocket + Local Candles
</div>

</div>


<div class="card">

<div class="label">
AI MARKET CONCLUSION
</div>

<div
id="decision"
class="decision"
>
WAIT
</div>

<div>
Confidence:
<b id="confidence">
0%
</b>
</div>

<div>
AI Score:
<b id="score">
0
</b>
</div>

</div>


<div class="card">

<div class="label">
MARKET TREND
</div>

<div
id="trend"
class="value"
>
UNKNOWN
</div>

<div
class="label"
style="margin-top:16px"
>
MOMENTUM
</div>

<div
id="momentum"
class="value"
>
UNKNOWN
</div>

</div>


<div class="card">

<div class="label">
MARKET STRUCTURE
</div>

<div
id="structure"
class="value"
>
UNKNOWN
</div>

<div
class="label"
style="margin-top:16px"
>
LIQUIDITY
</div>

<div
id="liquidity"
class="value"
>
UNKNOWN
</div>

</div>

</div>


<div
class="card"
style="margin-top:18px"
>

<div class="label">
TIMEFRAME MARKET DATA
</div>

<div class="tf">

<button onclick="showTF('1min')">
1m
</button>

<button onclick="showTF('5min')">
5m
</button>

<button onclick="showTF('15min')">
15m
</button>

<button onclick="showTF('30min')">
30m
</button>

<button onclick="showTF('1h')">
1h
</button>

</div>

<div
id="tfinfo"
style="margin-top:15px"
>
5m is the primary AI timeframe.
</div>

</div>


<div
class="grid"
style="margin-top:18px"
>


<div class="card">

<div class="label">
AI REASONS
</div>

<div id="reasons">
Waiting for data...
</div>

</div>


<div class="card">

<div class="label">
WARNINGS
</div>

<div id="warnings">
Waiting for data...
</div>

</div>


<div class="card">

<div class="label">
AI TRADE SETUP
</div>

<table>

<tr>
<td>Status</td>
<td id="tp_status">WAIT</td>
</tr>

<tr>
<td>Entry</td>
<td id="tp_entry">—</td>
</tr>

<tr>
<td>Stop Loss</td>
<td id="tp_sl">—</td>
</tr>

<tr>
<td>Target 1</td>
<td id="tp_t1">—</td>
</tr>

<tr>
<td>Target 2</td>
<td id="tp_t2">—</td>
</tr>

<tr>
<td>Support</td>
<td id="tp_support">—</td>
</tr>

<tr>
<td>Resistance</td>
<td id="tp_resistance">—</td>
</tr>

</table>

</div>


<div class="card">

<div class="label">
DATA ENGINE
</div>

<div>
WebSocket:
<b id="ws">
DISCONNECTED
</b>
</div>

<br>

<div>
5m candles:
<b id="c5">
0
</b>
</div>

<div>
15m candles:
<b id="c15">
0
</b>
</div>

<div>
1h candles:
<b id="c1h">
0
</b>
</div>

<br>

<div class="small">
HTTP candle requests are limited
to historical bootstrap only.
</div>

</div>

</div>


<div
class="card"
style="margin-top:18px"
>

<div class="label">
SIGNAL HISTORY
</div>

<div id="history">
No confirmed signals yet.
</div>

</div>


<div
class="card"
style="margin-top:18px"
>

<div class="label">
CRUDE OIL
</div>

<div class="small">
WTI is disabled on the current
data plan. We will add the crude
feed when the required market
data becomes available.
</div>

</div>


</div>


<script>

let latestData = null;


function el(id) {
    return document.getElementById(id);
}


function setText(id, value) {

    const node = el(id);

    if (node) {

        node.textContent =
            value === null ||
            value === undefined
                ? "—"
                : value;

    }
}


function renderReasons(items) {

    if (!items || !items.length) {

        setText(
            "reasons",
            "No confirmed reasons yet."
        );

        return;
    }

    el("reasons").innerHTML =
        items.map(
            x =>
                `<div class="reason">
                    • ${x}
                </div>`
        ).join("");
}


function renderWarnings(items) {

    if (!items || !items.length) {

        setText(
            "warnings",
            "No active warnings."
        );

        return;
    }

    el("warnings").innerHTML =
        items.map(
            x =>
                `<div class="warning">
                    • ${x}
                </div>`
        ).join("");
}


function renderHistory(items) {

    if (!items || !items.length) {

        setText(
            "history",
            "No confirmed signals yet."
        );

        return;
    }

    el("history").innerHTML =
        items.map(
            x =>
                `<div class="reason">
                    ${x.time}
                    — ${x.decision}
                    — ${x.confidence}%
                    — Score ${x.score}
                </div>`
        ).join("");
}


function render(data) {

    latestData = data;

    const ai = data.ai || {};
    const tp = data.trade_plan || {};

    if (
        data.price !== null &&
        data.price !== undefined
    ) {

        setText(
            "price",
            Number(data.price).toFixed(3)
        );

    }

    setText(
        "live",
        "● " +
        (
            data.connection?.websocket
            || "DISCONNECTED"
        )
    );

    setText(
        "decision",
        ai.decision || "WAIT"
    );

    setText(
        "confidence",
        (ai.confidence || 0) + "%"
    );

    setText(
        "score",
        ai.score ?? 0
    );

    setText(
        "trend",
        ai.trend || "UNKNOWN"
    );

    setText(
        "momentum",
        ai.momentum || "UNKNOWN"
    );

    setText(
        "structure",
        ai.structure || "UNKNOWN"
    );

    setText(
        "liquidity",
        ai.liquidity || "UNKNOWN"
    );

    setText(
        "ws",
        data.connection?.websocket
        || "DISCONNECTED"
    );

    setText(
        "c5",
        data.candle_counts?.["5min"] || 0
    );

    setText(
        "c15",
        data.candle_counts?.["15min"] || 0
    );

    setText(
        "c1h",
        data.candle_counts?.["1h"] || 0
    );

    renderReasons(ai.reasons);
    renderWarnings(ai.warnings);
    renderHistory(data.history);

    setText(
        "tp_status",
        tp.status || "WAIT"
    );

    setText(
        "tp_entry",
        tp.entry ?? "—"
    );

    setText(
        "tp_sl",
        tp.stop_loss ?? "—"
    );

    setText(
        "tp_t1",
        tp.target_1 ?? "—"
    );

    setText(
        "tp_t2",
        tp.target_2 ?? "—"
    );

    setText(
        "tp_support",
        tp.support ?? "—"
    );

    setText(
        "tp_resistance",
        tp.resistance ?? "—"
    );
}


function showTF(tf) {

    if (!latestData) {
        return;
    }

    const candles =
        latestData.candles?.[tf] || [];

    setText(
        "tfinfo",
        `${tf} candles available: ${candles.length}`
    );
}


async function loadMarket() {

    try {

        const response =
            await fetch("/api/market");

        const data =
            await response.json();

        render(data);

    } catch (error) {

        console.log(
            "Market update error:",
            error
        );

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
# ROUTES
# ============================================================

@app.route("/api/market")
def api_market():

    return jsonify(
        build_market_payload()
    )


@app.route("/health")
def health():

    with state_lock:

        return jsonify(
            {
                "status": "ok",
                "websocket":
                    connection_state["gold_ws"],
                "gold_price":
                    live_price["gold"],
                "api_key":
                    bool(API_KEY),
                "mode":
                    "WEBSOCKET + LOCAL CANDLES",
                "candles":
                    {
                        k: len(v)
                        for k, v
                        in local_candles.items()
                    },
            }
        )


@app.route("/stream")
def stream():

    def generate():

        while True:

            payload = build_market_payload()

            yield (
                "data: "
                + json.dumps(payload)
                + "\n\n"
            )

            time.sleep(1)

    return Response(
        generate(),
        mimetype="text/event-stream"
    )


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    global _started

    with _start_lock:

        if _started:
            return

        _started = True

        print(
            "STARTING TRADING AI WORKERS..."
        )

        threading.Thread(
            target=gold_websocket_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=bootstrap_worker,
            daemon=True
        ).start()

        threading.Thread(
            target=ai_worker,
            daemon=True
        ).start()

        print(
            "TRADING AI WORKERS STARTED"
        )


start_workers()


# ============================================================
# LOCAL RUN
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
        debug=False
    )
