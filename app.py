from flask import Flask, jsonify, render_template_string
import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")
GOLD_SYMBOL = "XAU/USD"

if not API_KEY:
    print("WARNING: TWELVE_DATA_API_KEY is missing", flush=True)

# ============================================================
# GLOBAL STATE
# ============================================================

lock = threading.Lock()

gold_prices = deque(maxlen=500)

candles = {
    "5min": [],
    "15min": [],
    "1h": [],
    "1day": []
}

latest = {
    "gold": {
        "symbol": GOLD_SYMBOL,
        "price": None,
        "status": "waiting",
        "source": "Twelve Data WebSocket",
        "updated": None,

        "decision": "WAIT",
        "confidence": 0,
        "score": 0,

        "entry": "--",
        "invalidation": "--",
        "target": "--",
        "why": "Waiting for live market data...",

        "trend": "WAIT",
        "momentum": "WAIT",
        "structure": "WAIT",

        "trend_5m": "WAIT",
        "trend_15m": "WAIT",
        "trend_1h": "WAIT",
        "trend_1d": "WAIT",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "buy_liquidity": None,
        "sell_liquidity": None,

        "candles_5m": 0,
        "candles_15m": 0,
        "candles_1h": 0,
        "candles_1d": 0
    }
}

# ============================================================
# HELPERS
# ============================================================

def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def avg(values):
    values = [v for v in values if v is not None]

    if not values:
        return None

    return sum(values) / len(values)


def closes(interval):
    with lock:
        data = list(candles.get(interval, []))

    result = []

    for candle in data:
        value = to_float(candle.get("close"))

        if value is not None:
            result.append(value)

    return result


def ema(values, period):
    if not values:
        return None

    if len(values) < period:
        period = len(values)

    if period <= 0:
        return None

    multiplier = 2 / (period + 1)

    result = values[0]

    for price in values[1:]:
        result = (price - result) * multiplier + result

    return result


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    recent_gains = gains[-period:]
    recent_losses = losses[-period:]

    avg_gain = sum(recent_gains) / period
    avg_loss = sum(recent_losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def trend_from_values(values):
    if len(values) < 20:
        return "WAIT"

    short = avg(values[-5:])
    medium = avg(values[-20:])

    if short is None or medium is None:
        return "WAIT"

    difference = short - medium

    threshold = max(abs(medium) * 0.00025, 0.01)

    if difference > threshold:
        return "BULLISH"

    if difference < -threshold:
        return "BEARISH"

    return "NEUTRAL"


def momentum_from_values(values):
    if len(values) < 10:
        return "WAIT"

    current = values[-1]
    previous = values[-10]

    if previous == 0:
        return "WAIT"

    change_percent = ((current - previous) / previous) * 100

    if change_percent > 0.08:
        return "BULLISH"

    if change_percent < -0.08:
        return "BEARISH"

    return "NEUTRAL"


def structure_from_values(values):
    if len(values) < 20:
        return "WAIT"

    recent = values[-10:]
    previous = values[-20:-10]

    recent_high = max(recent)
    recent_low = min(recent)

    previous_high = max(previous)
    previous_low = min(previous)

    if recent_high > previous_high and recent_low > previous_low:
        return "BULLISH"

    if recent_high < previous_high and recent_low < previous_low:
        return "BEARISH"

    return "NEUTRAL"


def liquidity_levels(values):
    if len(values) < 10:
        return None, None

    recent = values[-30:]

    buy_liquidity = min(recent)
    sell_liquidity = max(recent)

    return buy_liquidity, sell_liquidity


# ============================================================
# AI MARKET ANALYSIS
# ============================================================

def calculate_analysis(live_price=None):

    data_5m = closes("5min")
    data_15m = closes("15min")
    data_1h = closes("1h")
    data_1d = closes("1day")

    with lock:
        live_ticks = list(gold_prices)

    # Use candle price if websocket price isn't available yet
    price = live_price

    if price is None and live_ticks:
        price = live_ticks[-1]

    if price is None:
        fallback = data_5m or data_15m or data_1h

        if fallback:
            price = fallback[-1]

    if price is None:
        return

    trend_5m = trend_from_values(data_5m)
    trend_15m = trend_from_values(data_15m)
    trend_1h = trend_from_values(data_1h)
    trend_1d = trend_from_values(data_1d)

    momentum_5m = momentum_from_values(data_5m)
    structure_5m = structure_from_values(data_5m)

    analysis_values = data_5m

    if len(analysis_values) < 20:
        analysis_values = data_15m

    rsi_value = calculate_rsi(analysis_values)

    ema20_value = ema(analysis_values, 20)
    ema50_value = ema(analysis_values, 50)

    buy_liquidity, sell_liquidity = liquidity_levels(analysis_values)

    # ========================================================
    # SCORING ENGINE
    # ========================================================

    score = 0
    reasons = []

    # 5 MIN
    if trend_5m == "BULLISH":
        score += 2
        reasons.append("5m trend bullish")
    elif trend_5m == "BEARISH":
        score -= 2
        reasons.append("5m trend bearish")

    # 15 MIN
    if trend_15m == "BULLISH":
        score += 2
        reasons.append("15m trend bullish")
    elif trend_15m == "BEARISH":
        score -= 2
        reasons.append("15m trend bearish")

    # 1 HOUR
    if trend_1h == "BULLISH":
        score += 3
        reasons.append("1H trend bullish")
    elif trend_1h == "BEARISH":
        score -= 3
        reasons.append("1H trend bearish")

    # DAILY
    if trend_1d == "BULLISH":
        score += 3
        reasons.append("1D trend bullish")
    elif trend_1d == "BEARISH":
        score -= 3
        reasons.append("1D trend bearish")

    # MOMENTUM
    if momentum_5m == "BULLISH":
        score += 1
        reasons.append("short-term momentum positive")
    elif momentum_5m == "BEARISH":
        score -= 1
        reasons.append("short-term momentum negative")

    # STRUCTURE
    if structure_5m == "BULLISH":
        score += 2
        reasons.append("market structure bullish")
    elif structure_5m == "BEARISH":
        score -= 2
        reasons.append("market structure bearish")

    # EMA 20
    if ema20_value is not None:

        if price > ema20_value:
            score += 1
            reasons.append("price above EMA20")

        elif price < ema20_value:
            score -= 1
            reasons.append("price below EMA20")

    # EMA 50
    if ema50_value is not None:

        if price > ema50_value:
            score += 1
            reasons.append("price above EMA50")

        elif price < ema50_value:
            score -= 1
            reasons.append("price below EMA50")

    # RSI
    if rsi_value is not None:

        if 55 <= rsi_value <= 70:
            score += 1
            reasons.append("RSI supports bullish momentum")

        elif 30 <= rsi_value <= 45:
            score -= 1
            reasons.append("RSI supports bearish momentum")

        elif rsi_value > 75:
            reasons.append("RSI is overbought")

        elif rsi_value < 25:
            reasons.append("RSI is oversold")

    # ========================================================
    # DECISION
    # ========================================================

    decision = "WAIT"

    if score >= 7:
        decision = "BUY SIDE"

    elif score <= -7:
        decision = "SELL SIDE"

    # Strong higher-timeframe conflict = WAIT
    if (
        trend_1d != "WAIT"
        and trend_1h != "WAIT"
        and trend_1d != trend_1h
    ):
        decision = "WAIT"
        reasons.append("1D and 1H trend conflict")

    # ========================================================
    # CONFIDENCE
    # ========================================================

    confidence = min(95, 50 + abs(score) * 4)

    if decision == "WAIT":
        confidence = min(confidence, 60)

    # ========================================================
    # TRADE LEVELS
    # ========================================================

    entry = price
    invalidation = None
    target = None

    if decision == "BUY SIDE":

        # Long invalidation below market
        invalidation = buy_liquidity

        # Long target above market
        target = sell_liquidity

    elif decision == "SELL SIDE":

        # Short invalidation above market
        invalidation = sell_liquidity

        # Short target below market
        target = buy_liquidity

    # Prevent nonsensical levels
    if decision == "BUY SIDE":

        if invalidation is not None and invalidation >= price:
            invalidation = None

        if target is not None and target <= price:
            target = None

    elif decision == "SELL SIDE":

        if invalidation is not None and invalidation <= price:
            invalidation = None

        if target is not None and target >= price:
            target = None

    # ========================================================
    # HUMAN-READABLE WHY
    # ========================================================

    if reasons:
        why = " • ".join(reasons[:6])
    else:
        why = "Waiting for stronger market alignment"

    # ========================================================
    # UPDATE STATE
    # ========================================================

    with lock:

        latest["gold"].update({
            "price": round(price, 5),
            "status": "live",
            "updated": datetime.now(timezone.utc).isoformat(),

            "decision": decision,
            "confidence": confidence,
            "score": score,

            "entry": round(entry, 5) if entry is not None else "--",
            "invalidation": (
                round(invalidation, 5)
                if invalidation is not None
                else "--"
            ),
            "target": (
                round(target, 5)
                if target is not None
                else "--"
            ),

            "why": why,

            "trend": trend_1h,
            "momentum": momentum_5m,
            "structure": structure_5m,

            "trend_5m": trend_5m,
            "trend_15m": trend_15m,
            "trend_1h": trend_1h,
            "trend_1d": trend_1d,

            "rsi": (
                round(rsi_value, 2)
                if rsi_value is not None
                else None
            ),

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

            "buy_liquidity": (
                round(buy_liquidity, 5)
                if buy_liquidity is not None
                else None
            ),

            "sell_liquidity": (
                round(sell_liquidity, 5)
                if sell_liquidity is not None
                else None
            ),

            "candles_5m": len(data_5m),
            "candles_15m": len(data_15m),
            "candles_1h": len(data_1h),
            "candles_1d": len(data_1d)
        })


# ============================================================
# TWELVE DATA CANDLES
# ============================================================

def fetch_candles(interval):

    if not API_KEY:
        return

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": GOLD_SYMBOL,
        "interval": interval,
        "outputsize": 100,
        "apikey": API_KEY
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=20
        )

        if response.status_code != 200:

            print(
                f"CANDLE {interval}: HTTP {response.status_code} "
                f"{response.text[:250]}",
                flush=True
            )

            return

        data = response.json()

        if "values" not in data:

            print(
                f"CANDLE {interval}: no values {data}",
                flush=True
            )

            return

        values = data["values"]

        values = list(reversed(values))

        with lock:
            candles[interval] = values

        print(
            f"CANDLES {interval}: {len(values)}",
            flush=True
        )

        calculate_analysis()

    except Exception as e:

        print(
            f"CANDLE ERROR {interval}: {e}",
            flush=True
        )


# ============================================================
# CANDLE ENGINE
# ============================================================

def candle_worker():

    print("CANDLE ENGINE STARTING", flush=True)

    last_run = {
        "5min": 0,
        "15min": 0,
        "1h": 0,
        "1day": 0
    }

    intervals = {
        "5min": 60,
        "15min": 180,
        "1h": 300,
        "1day": 900
    }

    while True:

        now = time.time()

        for interval, seconds in intervals.items():

            if now - last_run[interval] >= seconds:

                fetch_candles(interval)

                last_run[interval] = now

        time.sleep(5)


# ============================================================
# GOLD WEBSOCKET ENGINE
# ============================================================

def gold_worker():

    print("GOLD WEBSOCKET ENGINE STARTING", flush=True)

    if not API_KEY:

        print(
            "GOLD WEBSOCKET STOPPED: API KEY MISSING",
            flush=True
        )

        return

    while True:

        ws = None

        try:

            ws_url = (
                "wss://ws.twelvedata.com/v1/quotes/price"
                f"?apikey={API_KEY}"
            )

            print("GOLD WS CONNECTING", flush=True)

            ws = websocket.create_connection(
                ws_url,
                timeout=30
            )

            print("GOLD WS OPEN", flush=True)

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(json.dumps(subscribe_message))

            print(
                "GOLD WS SUBSCRIBED:",
                GOLD_SYMBOL,
                flush=True
            )

            while True:

                raw = ws.recv()

                if not raw:
                    print(
                        "GOLD WS EMPTY MESSAGE",
                        flush=True
                    )
                    break

                try:
                    data = json.loads(raw)
                except Exception:
                    continue

                print(
                    "GOLD WS MESSAGE:",
                    data,
                    flush=True
                )

                price = None

                if isinstance(data, dict):

                    # Normal Twelve Data price message
                    price = to_float(
                        data.get("price")
                    )

                    # Some responses can contain values differently
                    if price is None:

                        price = to_float(
                            data.get("close")
                        )

                if price is not None:

                    with lock:
                        gold_prices.append(price)

                    print(
                        "GOLD PRICE:",
                        price,
                        flush=True
                    )

                    # Analyze immediately from live price
                    calculate_analysis(price)

        except Exception as e:

            print(
                "GOLD WS ERROR:",
                repr(e),
                flush=True
            )

        finally:

            try:

                if ws:
                    ws.close()

            except Exception:
                pass

        print(
            "GOLD WS RECONNECTING IN 5 SECONDS",
            flush=True
        )

        time.sleep(5)


# ============================================================
# API
# ============================================================

@app.route("/")
def home():

    return render_template_string(HTML)


@app.route("/api/state")
def api_state():

    with lock:
        result = {
            "gold": dict(latest["gold"])
        }

    return jsonify(result)


# ============================================================
# FRONTEND
# ============================================================

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
    padding: 20px;
    background: #080b12;
    color: #ffffff;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 900px;
    margin: auto;
}

.card {
    background: #111722;
    border: 1px solid #252d3a;
    border-radius: 18px;
    padding: 22px;
    margin-bottom: 20px;
}

.title {
    font-size: 28px;
    font-weight: bold;
    margin-bottom: 8px;
}

.subtitle {
    color: #9da8b8;
    margin-bottom: 20px;
}

.price {
    font-size: 38px;
    font-weight: bold;
    margin: 15px 0;
}

.live {
    color: #45e58c;
    font-weight: bold;
}

.waiting {
    color: #f2c94c;
    font-weight: bold;
}

.decision {
    font-size: 30px;
    font-weight: bold;
    margin: 12px 0;
}

.grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 12px;
}

.box {
    background: #0b1019;
    border-radius: 12px;
    padding: 14px;
}

.label {
    color: #8994a5;
    font-size: 13px;
    margin-bottom: 6px;
}

.value {
    font-size: 18px;
    font-weight: bold;
}

.reason {
    line-height: 1.7;
    color: #d6dce6;
}

@media(max-width:600px) {

    body {
        padding: 12px;
    }

    .grid {
        grid-template-columns: 1fr;
    }

    .price {
        font-size: 32px;
    }

}

</style>

</head>

<body>

<div class="container">

<div class="card">

<div class="title">
Trading-AI
</div>

<div class="subtitle">
Live Market Intelligence
</div>

<div class="title">
🥇 Gold — XAU/USD
</div>

<div id="connection" class="waiting">
CONNECTING...
</div>

<div id="price" class="price">
--
</div>

<div class="label">
AI MARKET CONCLUSION
</div>

<div id="decision" class="decision">
WAIT
</div>

<div class="grid">

<div class="box">

<div class="label">
CONFIDENCE
</div>

<div id="confidence" class="value">
0%
</div>

</div>

<div class="box">

<div class="label">
AI SCORE
</div>

<div id="score" class="value">
0
</div>

</div>

<div class="box">

<div class="label">
ENTRY
</div>

<div id="entry" class="value">
--
</div>

</div>

<div class="box">

<div class="label">
INVALIDATION
</div>

<div id="invalidation" class="value">
--
</div>

</div>

<div class="box">

<div class="label">
TARGET LIQUIDITY
</div>

<div id="target" class="value">
--
</div>

</div>

<div class="box">

<div class="label">
RSI
</div>

<div id="rsi" class="value">
--
</div>

</div>

</div>

</div>


<div class="card">

<div class="label">
MARKET STRUCTURE
</div>

<div class="grid">

<div class="box">
<div class="label">5 MIN</div>
<div id="trend5" class="value">WAIT</div>
</div>

<div class="box">
<div class="label">15 MIN</div>
<div id="trend15" class="value">WAIT</div>
</div>

<div class="box">
<div class="label">1 HOUR</div>
<div id="trend1h" class="value">WAIT</div>
</div>

<div class="box">
<div class="label">1 DAY</div>
<div id="trend1d" class="value">WAIT</div>
</div>

</div>

</div>


<div class="card">

<div class="label">
WHY?
</div>

<div id="why" class="reason">
Waiting for live market data...
</div>

</div>

</div>


<script>

async function updateState() {

    try {

        const response = await fetch(
            "/api/state?t=" + Date.now()
        );

        const data = await response.json();

        const gold = data.gold;

        document.getElementById("price").innerText =
            gold.price ?? "--";

        document.getElementById("decision").innerText =
            gold.decision ?? "WAIT";

        document.getElementById("confidence").innerText =
            (gold.confidence ?? 0) + "%";

        document.getElementById("score").innerText =
            gold.score ?? 0;

        document.getElementById("entry").innerText =
            gold.entry ?? "--";

        document.getElementById("invalidation").innerText =
            gold.invalidation ?? "--";

        document.getElementById("target").innerText =
            gold.target ?? "--";

        document.getElementById("rsi").innerText =
            gold.rsi ?? "--";

        document.getElementById("trend5").innerText =
            gold.trend_5m ?? "WAIT";

        document.getElementById("trend15").innerText =
            gold.trend_15m ?? "WAIT";

        document.getElementById("trend1h").innerText =
            gold.trend_1h ?? "WAIT";

        document.getElementById("trend1d").innerText =
            gold.trend_1d ?? "WAIT";

        document.getElementById("why").innerText =
            gold.why ?? "Waiting...";


        const connection =
            document.getElementById("connection");

        if (gold.status === "live") {

            connection.innerText =
                "● LIVE STREAM";

            connection.className = "live";

        } else {

            connection.innerText =
                "● WAITING FOR DATA";

            connection.className = "waiting";
        }


        const decision =
            document.getElementById("decision");

        if (gold.decision === "BUY SIDE") {

            decision.style.color = "#45e58c";

        } else if (gold.decision === "SELL SIDE") {

            decision.style.color = "#ff6b6b";

        } else {

            decision.style.color = "#f2c94c";
        }

    } catch (error) {

        document.getElementById("connection").innerText =
            "CONNECTION ERROR";

    }

}


setInterval(updateState, 1000);

updateState();

</script>

</body>

</html>
"""


# ============================================================
# START ENGINES
# ============================================================

print("LIVE ENGINE STARTING", flush=True)

threading.Thread(
    target=gold_worker,
    daemon=True
).start()

threading.Thread(
    target=candle_worker,
    daemon=True
).start()

print("LIVE ENGINE STARTED", flush=True)


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False
    )
