from flask import Flask, jsonify, render_template_string
import os
import time
import threading
import requests
from collections import deque

app = Flask(__name__)

# =========================================================
# CONFIG
# =========================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")
GOLD_SYMBOL = "XAU/USD"

PRICE_URL = "https://api.twelvedata.com/price"
CANDLE_URL = "https://api.twelvedata.com/time_series"

latest = {
    "gold": {
        "symbol": GOLD_SYMBOL,
        "price": None,
        "status": "Connecting...",
        "source": "Twelve Data",
        "updated": None,

        "decision": "WAIT",
        "confidence": 0,
        "score": 0,

        "entry": None,
        "invalidation": None,
        "target": None,

        "why": "",
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
        "candles_1d": 0,
    }
}

gold_prices = deque(maxlen=500)

candles = {
    "5min": [],
    "15min": [],
    "1h": [],
    "1day": []
}

lock = threading.Lock()


# =========================================================
# HELPERS
# =========================================================

def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def avg(values):
    values = [x for x in values if x is not None]

    if not values:
        return None

    return sum(values) / len(values)


def closes(interval):
    data = candles.get(interval, [])

    result = []

    for item in data:
        value = to_float(item.get("close"))

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
        result = ((price - result) * multiplier) + result

    return result


def rsi(values, period=14):
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

    average_gain = avg(recent_gains)
    average_loss = avg(recent_losses)

    if average_loss == 0:
        return 100.0

    if average_gain is None or average_loss is None:
        return None

    rs = average_gain / average_loss

    return 100 - (100 / (1 + rs))


def trend_from_values(values):
    if len(values) < 8:
        return "WAIT"

    recent = values[-1]
    old = values[-8]

    if recent > old:
        return "BULLISH"

    if recent < old:
        return "BEARISH"

    return "FLAT"


def momentum_from_values(values):
    if len(values) < 6:
        return "WAIT"

    recent = values[-1]
    old = values[-6]

    change = recent - old

    if change > 0:
        return "POSITIVE"

    if change < 0:
        return "NEGATIVE"

    return "NEUTRAL"


def structure_from_values(values):
    if len(values) < 10:
        return "WAIT"

    recent = values[-5:]
    previous = values[-10:-5]

    recent_high = max(recent)
    recent_low = min(recent)

    previous_high = max(previous)
    previous_low = min(previous)

    if recent_high > previous_high and recent_low > previous_low:
        return "BULLISH"

    if recent_high < previous_high and recent_low < previous_low:
        return "BEARISH"

    return "MIXED"


def liquidity_levels(values):
    if len(values) < 10:
        return None, None

    recent = values[-20:]

    buy_liquidity = min(recent)
    sell_liquidity = max(recent)

    return buy_liquidity, sell_liquidity


# =========================================================
# GOLD PRICE
# =========================================================

def fetch_gold_price():
    if not API_KEY:
        print("ERROR: TWELVE_DATA_API_KEY missing", flush=True)
        return None

    try:
        response = requests.get(
            PRICE_URL,
            params={
                "symbol": GOLD_SYMBOL,
                "apikey": API_KEY
            },
            timeout=15
        )

        print(
            "GOLD PRICE REQUEST:",
            response.status_code,
            response.text[:300],
            flush=True
        )

        if response.status_code != 200:
            return None

        data = response.json()

        price = to_float(data.get("price"))

        if price is None:
            print("GOLD PRICE INVALID:", data, flush=True)
            return None

        return price

    except Exception as e:
        print("GOLD PRICE ERROR:", e, flush=True)
        return None


# =========================================================
# CANDLES
# =========================================================

def fetch_candles(interval):

    if not API_KEY:
        return []

    try:
        response = requests.get(
            CANDLE_URL,
            params={
                "symbol": GOLD_SYMBOL,
                "interval": interval,
                "outputsize": 100,
                "apikey": API_KEY
            },
            timeout=20
        )

        print(
            f"CANDLE REQUEST {interval}:",
            response.status_code,
            flush=True
        )

        if response.status_code != 200:
            print(
                f"CANDLE ERROR {interval}:",
                response.text[:300],
                flush=True
            )
            return []

        data = response.json()

        values = data.get("values", [])

        if not values:
            print(
                f"CANDLES {interval}: 0",
                flush=True
            )
            return []

        # API generally returns newest first.
        # Reverse so calculations run oldest -> newest.
        values = list(reversed(values))

        print(
            f"CANDLES {interval}: {len(values)}",
            flush=True
        )

        return values

    except Exception as e:
        print(
            f"CANDLE ERROR {interval}:",
            e,
            flush=True
        )
        return []


# =========================================================
# AI ANALYSIS ENGINE
# =========================================================

def calculate_analysis(price=None):

    with lock:

        if price is None:
            price = latest["gold"]["price"]

        c5 = closes("5min")
        c15 = closes("15min")
        c1h = closes("1h")
        c1d = closes("1day")

        if not c5:
            c5 = list(gold_prices)

        trend5 = trend_from_values(c5)
        trend15 = trend_from_values(c15)
        trend1h = trend_from_values(c1h)
        trend1d = trend_from_values(c1d)

        momentum = momentum_from_values(c5)
        structure = structure_from_values(c5)

        ema20 = ema(c5, 20)
        ema50 = ema(c5, 50)

        rsi_value = rsi(c5)

        buy_liquidity, sell_liquidity = liquidity_levels(c5)

        score = 0
        reasons = []

        # -------------------------------------------------
        # TIMEFRAME TREND
        # -------------------------------------------------

        trend_weights = {
            "5m": 2,
            "15m": 2,
            "1h": 3,
            "1d": 3
        }

        trends = [
            ("5m", trend5),
            ("15m", trend15),
            ("1h", trend1h),
            ("1d", trend1d)
        ]

        for name, value in trends:

            weight = trend_weights[name]

            if value == "BULLISH":
                score += weight
                reasons.append(f"{name} bullish")

            elif value == "BEARISH":
                score -= weight
                reasons.append(f"{name} bearish")

        # -------------------------------------------------
        # MOMENTUM
        # -------------------------------------------------

        if momentum == "POSITIVE":
            score += 1
            reasons.append("short-term momentum positive")

        elif momentum == "NEGATIVE":
            score -= 1
            reasons.append("short-term momentum negative")

        # -------------------------------------------------
        # STRUCTURE
        # -------------------------------------------------

        if structure == "BULLISH":
            score += 2
            reasons.append("market structure bullish")

        elif structure == "BEARISH":
            score -= 2
            reasons.append("market structure bearish")

        # -------------------------------------------------
        # EMA
        # -------------------------------------------------

        if price is not None and ema20 is not None:

            if price > ema20:
                score += 1
                reasons.append("price above EMA20")

            elif price < ema20:
                score -= 1
                reasons.append("price below EMA20")

        if price is not None and ema50 is not None:

            if price > ema50:
                score += 1
                reasons.append("price above EMA50")

            elif price < ema50:
                score -= 1
                reasons.append("price below EMA50")

        # -------------------------------------------------
        # RSI
        # -------------------------------------------------

        if rsi_value is not None:

            if 55 <= rsi_value <= 70:
                score += 1
                reasons.append("RSI supports bullish momentum")

            elif 30 <= rsi_value <= 45:
                score -= 1
                reasons.append("RSI supports bearish momentum")

        # -------------------------------------------------
        # DECISION
        # -------------------------------------------------

        decision = "WAIT"

        if score >= 7:
            decision = "BUY SIDE"

        elif score <= -7:
            decision = "SELL SIDE"

        # Higher timeframe conflict = WAIT
        if trend1d != "WAIT" and trend1h != "WAIT":

            if (
                (trend1d == "BULLISH" and trend1h == "BEARISH")
                or
                (trend1d == "BEARISH" and trend1h == "BULLISH")
            ):
                decision = "WAIT"
                reasons.append("1D and 1H trend conflict")

        # -------------------------------------------------
        # CONFIDENCE
        # -------------------------------------------------

        confidence = min(
            95,
            50 + abs(score) * 4
        )

        if decision == "WAIT":
            confidence = min(confidence, 60)

        # -------------------------------------------------
        # TRADE LEVELS
        # -------------------------------------------------

        entry = None
        invalidation = None
        target = None

        if price is not None:

            if decision == "BUY SIDE":

                entry = price

                invalidation = (
                    sell_liquidity
                    if sell_liquidity is not None
                    else price * 0.995
                )

                target = (
                    sell_liquidity
                    if sell_liquidity is not None and sell_liquidity > price
                    else price * 1.01
                )

            elif decision == "SELL SIDE":

                entry = price

                invalidation = (
                    buy_liquidity
                    if buy_liquidity is not None
                    else price * 1.005
                )

                target = (
                    buy_liquidity
                    if buy_liquidity is not None and buy_liquidity < price
                    else price * 0.99
                )

        # -------------------------------------------------
        # TREND LABEL
        # -------------------------------------------------

        if score >= 3:
            overall_trend = "BULLISH"

        elif score <= -3:
            overall_trend = "BEARISH"

        else:
            overall_trend = "MIXED"

        # -------------------------------------------------
        # UPDATE STATE
        # -------------------------------------------------

        latest["gold"].update({
            "price": price,
            "status": "LIVE" if price is not None else "WAITING",
            "updated": time.strftime("%Y-%m-%d %H:%M:%S UTC"),

            "decision": decision,
            "confidence": confidence,
            "score": score,

            "entry": entry,
            "invalidation": invalidation,
            "target": target,

            "why": "; ".join(reasons[-8:]),

            "trend": overall_trend,
            "momentum": momentum,
            "structure": structure,

            "trend_5m": trend5,
            "trend_15m": trend15,
            "trend_1h": trend1h,
            "trend_1d": trend1d,

            "rsi": rsi_value,
            "ema20": ema20,
            "ema50": ema50,

            "buy_liquidity": buy_liquidity,
            "sell_liquidity": sell_liquidity,

            "candles_5m": len(c5),
            "candles_15m": len(c15),
            "candles_1h": len(c1h),
            "candles_1d": len(c1d),
        })


# =========================================================
# GOLD LIVE WORKER
# =========================================================

def gold_worker():

    print("GOLD REST ENGINE STARTING", flush=True)

    while True:

        try:

            price = fetch_gold_price()

            if price is not None:

                with lock:
                    gold_prices.append(price)

                print(
                    "GOLD PRICE:",
                    price,
                    flush=True
                )

                calculate_analysis(price)

            else:

                print(
                    "GOLD PRICE: waiting...",
                    flush=True
                )

        except Exception as e:

            print(
                "GOLD ENGINE ERROR:",
                e,
                flush=True
            )

        # REST polling interval
        time.sleep(10)


# =========================================================
# CANDLE WORKER
# =========================================================

def candle_worker():

    print("CANDLE ENGINE STARTING", flush=True)

    while True:

        try:

            for interval in [
                "5min",
                "15min",
                "1h",
                "1day"
            ]:

                values = fetch_candles(interval)

                if values:

                    with lock:
                        candles[interval] = values

            with lock:
                current_price = latest["gold"]["price"]

            calculate_analysis(current_price)

        except Exception as e:

            print(
                "CANDLE ENGINE ERROR:",
                e,
                flush=True
            )

        time.sleep(60)


# =========================================================
# API
# =========================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/state")
def api_state():

    with lock:
        data = {
            "gold": dict(latest["gold"])
        }

    return jsonify(data)


# =========================================================
# FRONTEND
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
    background: #080b12;
    color: #f5f7fb;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 900px;
    margin: auto;
    padding: 20px;
}

.header {
    margin-bottom: 20px;
}

.logo {
    font-size: 30px;
    font-weight: 800;
}

.subtitle {
    color: #8e98aa;
    margin-top: 5px;
}

.card {
    background: #111722;
    border: 1px solid #202938;
    border-radius: 18px;
    padding: 22px;
    margin-bottom: 18px;
}

.title {
    font-size: 22px;
    font-weight: 700;
}

.symbol {
    color: #8e98aa;
    margin-top: 5px;
}

.status {
    margin-top: 14px;
    color: #62e6a8;
    font-weight: 700;
}

.price {
    font-size: 42px;
    font-weight: 800;
    margin-top: 12px;
}

.section {
    margin-top: 25px;
}

.section-title {
    color: #8e98aa;
    font-size: 13px;
    letter-spacing: 1px;
    text-transform: uppercase;
}

.decision {
    font-size: 30px;
    font-weight: 900;
    margin-top: 8px;
}

.confidence {
    margin-top: 5px;
    color: #aab3c2;
}

.grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
    margin-top: 15px;
}

.box {
    background: #0b1019;
    border-radius: 12px;
    padding: 14px;
}

.box-label {
    color: #7f8999;
    font-size: 12px;
}

.box-value {
    margin-top: 6px;
    font-size: 18px;
    font-weight: 700;
}

.analysis {
    line-height: 1.6;
    color: #cbd2dd;
}

.row {
    display: flex;
    justify-content: space-between;
    padding: 9px 0;
    border-bottom: 1px solid #202938;
}

.muted {
    color: #7f8999;
}

@media (max-width: 650px) {

    .grid {
        grid-template-columns: 1fr 1fr;
    }

    .price {
        font-size: 34px;
    }

}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <div class="logo">
            Trading-AI
        </div>

        <div class="subtitle">
            Live Market Intelligence
        </div>

    </div>


    <div class="card">

        <div class="title">
            🥇 Gold
        </div>

        <div class="symbol">
            XAU/USD
        </div>

        <div id="status" class="status">
            Connecting...
        </div>

        <div id="price" class="price">
            --
        </div>


        <div class="section">

            <div class="section-title">
                AI Market Conclusion
            </div>

            <div id="decision" class="decision">
                WAIT
            </div>

            <div id="confidence"
                 class="confidence">
                Confidence: 0%
            </div>

        </div>


        <div class="grid">

            <div class="box">

                <div class="box-label">
                    AI Score
                </div>

                <div id="score"
                     class="box-value">
                    0
                </div>

            </div>


            <div class="box">

                <div class="box-label">
                    Entry
                </div>

                <div id="entry"
                     class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-label">
                    Invalidation
                </div>

                <div id="invalidation"
                     class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-label">
                    Target
                </div>

                <div id="target"
                     class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-label">
                    RSI
                </div>

                <div id="rsi"
                     class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-label">
                    EMA20
                </div>

                <div id="ema20"
                     class="box-value">
                    --
                </div>

            </div>

        </div>

    </div>


    <div class="card">

        <div class="section-title">
            Market Analysis
        </div>

        <div class="analysis">

            <div class="row">
                <span>Overall Trend</span>
                <strong id="trend">--</strong>
            </div>

            <div class="row">
                <span>Momentum</span>
                <strong id="momentum">--</strong>
            </div>

            <div class="row">
                <span>Structure</span>
                <strong id="structure">--</strong>
            </div>

            <div class="row">
                <span>5 Minute</span>
                <strong id="trend5">--</strong>
            </div>

            <div class="row">
                <span>15 Minute</span>
                <strong id="trend15">--</strong>
            </div>

            <div class="row">
                <span>1 Hour</span>
                <strong id="trend1h">--</strong>
            </div>

            <div class="row">
                <span>1 Day</span>
                <strong id="trend1d">--</strong>
            </div>

        </div>

    </div>


    <div class="card">

        <div class="section-title">
            AI Reasoning
        </div>

        <p id="why">
            Waiting for market data...
        </p>

    </div>


    <div class="card">

        <div class="section-title">
            Liquidity Map
        </div>

        <div class="row">
            <span>Buy-side Liquidity</span>
            <strong id="buyLiquidity">--</strong>
        </div>

        <div class="row">
            <span>Sell-side Liquidity</span>
            <strong id="sellLiquidity">--</strong>
        </div>

        <div class="row">
            <span>Last Update</span>
            <strong id="updated">--</strong>
        </div>

    </div>

</div>


<script>

function value(v) {

    if (
        v === null ||
        v === undefined ||
        v === ""
    ) {
        return "--";
    }

    if (
        typeof v === "number"
    ) {
        return v.toFixed(2);
    }

    return v;
}


async function updateState() {

    try {

        const response =
            await fetch(
                "/api/state?t=" +
                Date.now()
            );

        const data =
            await response.json();

        const g = data.gold;


        document.getElementById("status")
            .innerText =
            g.status || "WAITING";


        document.getElementById("price")
            .innerText =
            value(g.price);


        document.getElementById("decision")
            .innerText =
            g.decision || "WAIT";


        document.getElementById("confidence")
            .innerText =
            "Confidence: " +
            value(g.confidence) +
            "%";


        document.getElementById("score")
            .innerText =
            value(g.score);


        document.getElementById("entry")
            .innerText =
            value(g.entry);


        document.getElementById("invalidation")
            .innerText =
            value(g.invalidation);


        document.getElementById("target")
            .innerText =
            value(g.target);


        document.getElementById("rsi")
            .innerText =
            value(g.rsi);


        document.getElementById("ema20")
            .innerText =
            value(g.ema20);


        document.getElementById("trend")
            .innerText =
            value(g.trend);


        document.getElementById("momentum")
            .innerText =
            value(g.momentum);


        document.getElementById("structure")
            .innerText =
            value(g.structure);


        document.getElementById("trend5")
            .innerText =
            value(g.trend_5m);


        document.getElementById("trend15")
            .innerText =
            value(g.trend_15m);


        document.getElementById("trend1h")
            .innerText =
            value(g.trend_1h);


        document.getElementById("trend1d")
            .innerText =
            value(g.trend_1d);


        document.getElementById("why")
            .innerText =
            g.why ||
            "Waiting for enough market information...";


        document.getElementById("buyLiquidity")
            .innerText =
            value(g.buy_liquidity);


        document.getElementById("sellLiquidity")
            .innerText =
            value(g.sell_liquidity);


        document.getElementById("updated")
            .innerText =
            g.updated || "--";

    }

    catch (error) {

        document.getElementById("status")
            .innerText =
            "Connection Error";

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


# =========================================================
# START BACKGROUND ENGINES
# =========================================================

def start_engines():

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


start_engines()


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000)),
        debug=False
    )
