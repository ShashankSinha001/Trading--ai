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

lock = threading.RLock()

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
# BASIC HELPERS
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


def get_closes(interval):
    with lock:
        data = list(candles.get(interval, []))

    result = []

    for candle in data:
        value = to_float(candle.get("close"))

        if value is not None:
            result.append(value)

    return result


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def ema(values, period):
    if not values:
        return None

    period = min(period, len(values))

    if period <= 0:
        return None

    multiplier = 2 / (period + 1)

    result = values[0]

    for price in values[1:]:
        result = ((price - result) * multiplier) + result

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

    gains = gains[-period:]
    losses = losses[-period:]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

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

    return min(recent), max(recent)


# ============================================================
# LIVE PRICE STATE
# ============================================================

def update_live_price(price):
    """
    IMPORTANT:
    Live WebSocket price is written directly into latest state.
    Analysis failure cannot prevent the price from appearing.
    """

    if price is None:
        return

    price = float(price)

    with lock:
        gold_prices.append(price)

        latest["gold"]["price"] = round(price, 5)
        latest["gold"]["status"] = "live"
        latest["gold"]["source"] = "Twelve Data WebSocket"
        latest["gold"]["updated"] = datetime.now(
            timezone.utc
        ).isoformat()

    print(
        f"STATE UPDATED: GOLD = {price}",
        flush=True
    )


# ============================================================
# AI MARKET ANALYSIS
# ============================================================

def calculate_analysis(live_price=None):

    try:

        data_5m = get_closes("5min")
        data_15m = get_closes("15min")
        data_1h = get_closes("1h")
        data_1d = get_closes("1day")

        with lock:
            live_ticks = list(gold_prices)

        price = live_price

        if price is None and live_ticks:
            price = live_ticks[-1]

        if price is None:
            for data in [data_5m, data_15m, data_1h, data_1d]:
                if data:
                    price = data[-1]
                    break

        if price is None:
            return

        trend_5m = trend_from_values(data_5m)
        trend_15m = trend_from_values(data_15m)
        trend_1h = trend_from_values(data_1h)
        trend_1d = trend_from_values(data_1d)

        momentum = momentum_from_values(data_5m)
        structure = structure_from_values(data_5m)

        analysis_values = data_5m

        if len(analysis_values) < 20:
            analysis_values = data_15m

        if len(analysis_values) < 20:
            analysis_values = data_1h

        rsi_value = calculate_rsi(analysis_values)

        ema20_value = ema(analysis_values, 20)
        ema50_value = ema(analysis_values, 50)

        buy_liquidity, sell_liquidity = liquidity_levels(
            analysis_values
        )

        # ====================================================
        # SCORE
        # ====================================================

        score = 0
        reasons = []

        if trend_5m == "BULLISH":
            score += 2
            reasons.append("5m bullish")

        elif trend_5m == "BEARISH":
            score -= 2
            reasons.append("5m bearish")

        if trend_15m == "BULLISH":
            score += 2
            reasons.append("15m bullish")

        elif trend_15m == "BEARISH":
            score -= 2
            reasons.append("15m bearish")

        if trend_1h == "BULLISH":
            score += 3
            reasons.append("1H bullish")

        elif trend_1h == "BEARISH":
            score -= 3
            reasons.append("1H bearish")

        if trend_1d == "BULLISH":
            score += 3
            reasons.append("1D bullish")

        elif trend_1d == "BEARISH":
            score -= 3
            reasons.append("1D bearish")

        if momentum == "BULLISH":
            score += 1
            reasons.append("momentum positive")

        elif momentum == "BEARISH":
            score -= 1
            reasons.append("momentum negative")

        if structure == "BULLISH":
            score += 2
            reasons.append("structure bullish")

        elif structure == "BEARISH":
            score -= 2
            reasons.append("structure bearish")

        if ema20_value is not None:

            if price > ema20_value:
                score += 1
                reasons.append("above EMA20")

            elif price < ema20_value:
                score -= 1
                reasons.append("below EMA20")

        if ema50_value is not None:

            if price > ema50_value:
                score += 1
                reasons.append("above EMA50")

            elif price < ema50_value:
                score -= 1
                reasons.append("below EMA50")

        if rsi_value is not None:

            if 55 <= rsi_value <= 70:
                score += 1
                reasons.append("RSI bullish zone")

            elif 30 <= rsi_value <= 45:
                score -= 1
                reasons.append("RSI bearish zone")

            elif rsi_value > 75:
                reasons.append("RSI overbought")

            elif rsi_value < 25:
                reasons.append("RSI oversold")

        # ====================================================
        # DECISION
        # ====================================================

        decision = "WAIT"

        if score >= 7:
            decision = "BUY SIDE"

        elif score <= -7:
            decision = "SELL SIDE"

        if (
            trend_1d != "WAIT"
            and trend_1h != "WAIT"
            and trend_1d != trend_1h
        ):
            decision = "WAIT"
            reasons.append("1D/1H conflict")

        # ====================================================
        # CONFIDENCE
        # ====================================================

        confidence = min(
            95,
            50 + abs(score) * 4
        )

        if decision == "WAIT":
            confidence = min(confidence, 60)

        # ====================================================
        # LEVELS
        # ====================================================

        entry = price
        invalidation = None
        target = None

        if decision == "BUY SIDE":

            invalidation = buy_liquidity
            target = sell_liquidity

            if invalidation is not None and invalidation >= price:
                invalidation = None

            if target is not None and target <= price:
                target = None

        elif decision == "SELL SIDE":

            invalidation = sell_liquidity
            target = buy_liquidity

            if invalidation is not None and invalidation <= price:
                invalidation = None

            if target is not None and target >= price:
                target = None

        if reasons:
            why = " • ".join(reasons[:7])
        else:
            why = "Waiting for stronger market alignment"

        # ====================================================
        # UPDATE ANALYSIS
        # ====================================================

        with lock:

            latest["gold"].update({

                "price": round(price, 5),

                "status": "live",

                "updated": datetime.now(
                    timezone.utc
                ).isoformat(),

                "decision": decision,

                "confidence": confidence,

                "score": score,

                "entry": (
                    round(entry, 5)
                    if entry is not None
                    else "--"
                ),

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

                "momentum": momentum,

                "structure": structure,

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

        print(
            f"AI UPDATE: price={price} "
            f"score={score} "
            f"decision={decision}",
            flush=True
        )

    except Exception as e:

        print(
            "ANALYSIS ERROR:",
            repr(e),
            flush=True
        )

        # Even if analysis fails,
        # keep live price visible.

        if live_price is not None:

            with lock:
                latest["gold"]["price"] = round(
                    float(live_price),
                    5
                )

                latest["gold"]["status"] = "live"

                latest["gold"]["updated"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )


# ============================================================
# CANDLE FETCH
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

        print(
            f"CANDLE REQUEST {interval}: HTTP "
            f"{response.status_code}",
            flush=True
        )

        if response.status_code != 200:
            print(
                response.text[:250],
                flush=True
            )
            return

        data = response.json()

        if "values" not in data:

            print(
                f"CANDLE {interval}: no values",
                data,
                flush=True
            )

            return

        values = list(
            reversed(data["values"])
        )

        with lock:
            candles[interval] = values

        print(
            f"CANDLES {interval}: {len(values)}",
            flush=True
        )

        # Recalculate using current live price
        with lock:
            current_price = latest["gold"]["price"]

        calculate_analysis(current_price)

    except Exception as e:

        print(
            f"CANDLE ERROR {interval}:",
            repr(e),
            flush=True
        )


# ============================================================
# CANDLE ENGINE
# ============================================================

def candle_worker():

    print(
        "CANDLE ENGINE STARTING",
        flush=True
    )

    last_run = {
        "5min": 0,
        "15min": 0,
        "1h": 0,
        "1day": 0
    }

    # Deliberately spaced out to avoid
    # Twelve Data Basic credit bursts.

    intervals = {
        "5min": 120,
        "15min": 300,
        "1h": 600,
        "1day": 1800
    }

    while True:

        now = time.time()

        for interval, seconds in intervals.items():

            if now - last_run[interval] >= seconds:

                fetch_candles(interval)

                last_run[interval] = now

                # Small spacing between API requests
                time.sleep(2)

        time.sleep(5)


# ============================================================
# GOLD WEBSOCKET
# ============================================================

def gold_worker():

    print(
        "GOLD WEBSOCKET ENGINE STARTING",
        flush=True
    )

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

            print(
                "GOLD WS CONNECTING",
                flush=True
            )

            ws = websocket.create_connection(
                ws_url,
                timeout=30
            )

            print(
                "GOLD WS OPEN",
                flush=True
            )

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(
                json.dumps(
                    subscribe_message
                )
            )

            print(
                "GOLD WS SUBSCRIBED:",
                GOLD_SYMBOL,
                flush=True
            )

            while True:

                raw = ws.recv()

                if not raw:
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

                    price = to_float(
                        data.get("price")
                    )

                    if price is None:
                        price = to_float(
                            data.get("close")
                        )

                if price is None:
                    continue

                print(
                    "GOLD PRICE:",
                    price,
                    flush=True
                )

                # ==================================================
                # CRITICAL:
                # Update API state FIRST.
                # ==================================================

                update_live_price(price)

                # Then calculate analysis.
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
# API ROUTES
# ============================================================

@app.route("/")
def home():

    return render_template_string(HTML)


@app.route("/api/state")
def api_state():

    with lock:

        result = {
            "gold": dict(
                latest["gold"]
            )
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

.error {
    color: #ff6b6b;
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
<div class="label">CONFIDENCE</div>
<div id="confidence" class="value">0%</div>
</div>

<div class="box">
<div class="label">AI SCORE</div>
<div id="score" class="value">0</div>
</div>

<div class="box">
<div class="label">ENTRY</div>
<div id="entry" class="value">--</div>
</div>

<div class="box">
<div class="label">INVALIDATION</div>
<div id="invalidation" class="value">--</div>
</div>

<div class="box">
<div class="label">TARGET LIQUIDITY</div>
<div id="target" class="value">--</div>
</div>

<div class="box">
<div class="label">RSI</div>
<div id="rsi" class="value">--</div>
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
            "/api/state?t=" + Date.now(),
            {
                cache: "no-store"
            }
        );

        if (!response.ok) {
            throw new Error(
                "HTTP " + response.status
            );
        }

        const data = await response.json();

        const gold = data.gold || {};

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

        const decisionElement =
            document.getElementById("decision");

        if (gold.decision === "BUY SIDE") {

            decisionElement.style.color =
                "#45e58c";

        } else if (
            gold.decision === "SELL SIDE"
        ) {

            decisionElement.style.color =
                "#ff6b6b";

        } else {

            decisionElement.style.color =
                "#f2c94c";
        }

    } catch (error) {

        const connection =
            document.getElementById("connection");

        connection.innerText =
            "CONNECTION ERROR";

        connection.className = "error";

        console.error(error);
    }
}


updateState();

setInterval(
    updateState,
    1000
);

</script>

</body>

</html>
"""


# ============================================================
# START ENGINES
# ============================================================

print(
    "LIVE ENGINE STARTING",
    flush=True
)

threading.Thread(
    target=gold_worker,
    daemon=True
).start()

threading.Thread(
    target=candle_worker,
    daemon=True
).start()

print(
    "LIVE ENGINE STARTED",
    flush=True
)


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                5000
            )
        ),
        debug=False
    )
