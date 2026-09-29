from flask import Flask, jsonify, render_template_string
import os
import json
import time
import threading
import websocket
import requests
from collections import deque

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

GOLD_SYMBOL = "XAU/USD"

# ============================================================
# GLOBAL STATE
# ============================================================

latest = {
    "gold": {
        "symbol": GOLD_SYMBOL,
        "price": None,
        "status": "Connecting...",
        "source": "Twelve Data",

        "decision": "WAIT",
        "confidence": 0,
        "score": 0,

        "entry": None,
        "invalidation": None,
        "target": None,

        "why": "Waiting for market data...",

        "trend": "Waiting",
        "momentum": "Waiting",
        "structure": "Waiting",

        "trend_5m": "Waiting",
        "trend_15m": "Waiting",
        "trend_1h": "Waiting",
        "trend_1d": "Waiting",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "buy_liquidity": None,
        "sell_liquidity": None,

        "candles_5m": 0,
        "candles_15m": 0,
        "candles_1h": 0,
        "candles_1d": 0,

        "updated": None
    }
}

gold_prices = deque(maxlen=500)

candles = {
    "5min": [],
    "15min": [],
    "1h": [],
    "1day": []
}

lock = threading.RLock()


# ============================================================
# HELPERS
# ============================================================

def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except:
        return None


def average(values):
    values = [safe_float(x) for x in values]
    values = [x for x in values if x is not None]

    if not values:
        return None

    return sum(values) / len(values)


def candle_closes(interval):
    with lock:
        data = list(candles.get(interval, []))

    closes = []

    for candle in data:
        value = safe_float(candle.get("close"))

        if value is not None:
            closes.append(value)

    return closes


def ema(values, period):
    values = [safe_float(x) for x in values]
    values = [x for x in values if x is not None]

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


def calculate_rsi(values, period=14):
    values = [safe_float(x) for x in values]
    values = [x for x in values if x is not None]

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
    values = [safe_float(x) for x in values]
    values = [x for x in values if x is not None]

    if len(values) < 10:
        return "Waiting"

    recent = values[-10:]
    first = recent[0]
    last = recent[-1]

    if first == 0:
        return "Waiting"

    change_pct = ((last - first) / first) * 100

    if change_pct > 0.08:
        return "Bullish"

    if change_pct < -0.08:
        return "Bearish"

    return "Neutral"


def momentum_from_values(values):
    values = [safe_float(x) for x in values]
    values = [x for x in values if x is not None]

    if len(values) < 5:
        return "Waiting"

    first = values[-5]
    last = values[-1]

    if first == 0:
        return "Waiting"

    change_pct = ((last - first) / first) * 100

    if change_pct > 0.04:
        return "Positive"

    if change_pct < -0.04:
        return "Negative"

    return "Neutral"


def structure_from_candles(data):
    if len(data) < 5:
        return "Waiting"

    highs = []
    lows = []

    for candle in data[-10:]:
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    if len(highs) < 4 or len(lows) < 4:
        return "Waiting"

    recent_high = max(highs[-5:])
    previous_high = max(highs[:-5]) if len(highs) > 5 else max(highs[:2])

    recent_low = min(lows[-5:])
    previous_low = min(lows[:-5]) if len(lows) > 5 else min(lows[:2])

    if recent_high > previous_high and recent_low > previous_low:
        return "Higher Highs / Higher Lows"

    if recent_high < previous_high and recent_low < previous_low:
        return "Lower Highs / Lower Lows"

    return "Range / Mixed"


def liquidity_levels(data):
    if not data:
        return None, None

    highs = []
    lows = []

    for candle in data[-40:]:
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    buy_liquidity = max(highs) if highs else None
    sell_liquidity = min(lows) if lows else None

    return buy_liquidity, sell_liquidity


def format_price(value):
    if value is None:
        return "--"

    try:
        return f"{float(value):.2f}"
    except:
        return "--"


# ============================================================
# AI MARKET ANALYSIS
# ============================================================

def calculate_analysis(price=None):

    with lock:

        if price is None:
            price = latest["gold"].get("price")

        price = safe_float(price)

        if price is None:
            return

        closes_5m = candle_closes("5min")
        closes_15m = candle_closes("15min")
        closes_1h = candle_closes("1h")
        closes_1d = candle_closes("1day")

        # Need candle data for proper analysis.
        if len(closes_5m) < 10:
            latest["gold"]["status"] = "Live — collecting analysis data"
            latest["gold"]["updated"] = int(time.time())
            return

        # --------------------------------------------------------
        # TIMEFRAME TRENDS
        # --------------------------------------------------------

        trend_5m = trend_from_values(closes_5m)
        trend_15m = trend_from_values(closes_15m)
        trend_1h = trend_from_values(closes_1h)
        trend_1d = trend_from_values(closes_1d)

        # --------------------------------------------------------
        # MOMENTUM
        # --------------------------------------------------------

        live_prices = list(gold_prices)

        # If live ticks are not moving enough, use 5m candles.
        if len(live_prices) >= 5:
            momentum_source = live_prices
        else:
            momentum_source = closes_5m

        momentum = momentum_from_values(momentum_source)

        # --------------------------------------------------------
        # STRUCTURE
        # --------------------------------------------------------

        with lock:
            candle_data_5m = list(candles["5min"])

        structure = structure_from_candles(candle_data_5m)

        # --------------------------------------------------------
        # INDICATORS
        # --------------------------------------------------------

        ema20 = ema(closes_5m, 20)
        ema50 = ema(closes_5m, 50)
        rsi = calculate_rsi(closes_5m, 14)

        # --------------------------------------------------------
        # LIQUIDITY
        # --------------------------------------------------------

        buy_liquidity, sell_liquidity = liquidity_levels(candle_data_5m)

        # --------------------------------------------------------
        # SCORING ENGINE
        # --------------------------------------------------------

        score = 0
        reasons = []

        # 5 MIN
        if trend_5m == "Bullish":
            score += 2
            reasons.append("5m trend bullish")
        elif trend_5m == "Bearish":
            score -= 2
            reasons.append("5m trend bearish")

        # 15 MIN
        if trend_15m == "Bullish":
            score += 2
            reasons.append("15m trend bullish")
        elif trend_15m == "Bearish":
            score -= 2
            reasons.append("15m trend bearish")

        # 1 HOUR
        if trend_1h == "Bullish":
            score += 3
            reasons.append("1h trend bullish")
        elif trend_1h == "Bearish":
            score -= 3
            reasons.append("1h trend bearish")

        # 1 DAY
        if trend_1d == "Bullish":
            score += 3
            reasons.append("1D trend bullish")
        elif trend_1d == "Bearish":
            score -= 3
            reasons.append("1D trend bearish")

        # MOMENTUM
        if momentum == "Positive":
            score += 1
            reasons.append("short-term momentum positive")

        elif momentum == "Negative":
            score -= 1
            reasons.append("short-term momentum negative")

        # STRUCTURE
        if structure == "Higher Highs / Higher Lows":
            score += 2
            reasons.append("market structure making higher highs/lows")

        elif structure == "Lower Highs / Lower Lows":
            score -= 2
            reasons.append("market structure making lower highs/lows")

        # EMA
        if ema20 is not None:

            if price > ema20:
                score += 1
                reasons.append("price above EMA20")

            elif price < ema20:
                score -= 1
                reasons.append("price below EMA20")

        if ema50 is not None:

            if price > ema50:
                score += 1
                reasons.append("price above EMA50")

            elif price < ema50:
                score -= 1
                reasons.append("price below EMA50")

        # RSI
        if rsi is not None:

            if 55 <= rsi <= 70:
                score += 1
                reasons.append("RSI supports bullish momentum")

            elif 30 <= rsi <= 45:
                score -= 1
                reasons.append("RSI supports bearish momentum")

            elif rsi > 70:
                reasons.append("RSI is elevated")

            elif rsi < 30:
                reasons.append("RSI is deeply oversold")

        # --------------------------------------------------------
        # DECISION
        # --------------------------------------------------------

        decision = "WAIT"

        # Strong alignment
        if score >= 7:
            decision = "BUY SIDE"

        elif score <= -7:
            decision = "SELL SIDE"

        else:
            decision = "WAIT"

        # Higher timeframe conflict protection
        if (
            trend_1d in ["Bullish", "Bearish"]
            and trend_1h in ["Bullish", "Bearish"]
            and trend_1d != trend_1h
        ):
            decision = "WAIT"
            reasons.append("1D and 1H trends are conflicting")

        # --------------------------------------------------------
        # LEVELS
        # --------------------------------------------------------

        entry = price
        invalidation = None
        target = None

        if decision == "BUY SIDE":

            invalidation = sell_liquidity

            if invalidation is None or invalidation >= price:
                invalidation = price * 0.995

            target = buy_liquidity

            if target is None or target <= price:
                target = price * 1.01

        elif decision == "SELL SIDE":

            invalidation = buy_liquidity

            if invalidation is None or invalidation <= price:
                invalidation = price * 1.005

            target = sell_liquidity

            if target is None or target >= price:
                target = price * 0.99

        else:

            entry = price

            # For WAIT, show nearby liquidity without calling it
            # a trade entry/target.
            invalidation = None
            target = None

        # --------------------------------------------------------
        # CONFIDENCE
        # --------------------------------------------------------

        if decision == "WAIT":

            confidence = min(65, 45 + abs(score) * 3)

        else:

            confidence = min(95, 50 + abs(score) * 4)

        # --------------------------------------------------------
        # WHY
        # --------------------------------------------------------

        if not reasons:
            reasons.append("Market signals are still developing")

        why = " • ".join(reasons[-8:])

        # --------------------------------------------------------
        # UPDATE STATE
        # --------------------------------------------------------

        latest["gold"].update({

            "price": price,
            "status": "LIVE",
            "source": "Twelve Data",

            "decision": decision,
            "confidence": confidence,
            "score": score,

            "entry": entry,
            "invalidation": invalidation,
            "target": target,

            "why": why,

            "trend": (
                "Bullish" if score > 0
                else "Bearish" if score < 0
                else "Neutral"
            ),

            "momentum": momentum,
            "structure": structure,

            "trend_5m": trend_5m,
            "trend_15m": trend_15m,
            "trend_1h": trend_1h,
            "trend_1d": trend_1d,

            "rsi": round(rsi, 2) if rsi is not None else None,
            "ema20": ema20,
            "ema50": ema50,

            "buy_liquidity": buy_liquidity,
            "sell_liquidity": sell_liquidity,

            "candles_5m": len(closes_5m),
            "candles_15m": len(closes_15m),
            "candles_1h": len(closes_1h),
            "candles_1d": len(closes_1d),

            "updated": int(time.time())
        })


# ============================================================
# GOLD WEBSOCKET
# ============================================================

def gold_worker():

    print("GOLD ENGINE STARTING")

    if not API_KEY:
        print("ERROR: TWELVE_DATA_API_KEY NOT FOUND")
        return

    while True:

        try:

            websocket_url = (
                "wss://ws.twelvedata.com/v1/quotes/price?apikey="
                + API_KEY
            )

            def on_open(ws):

                print("GOLD WS OPEN")

                subscribe_message = {
                    "action": "subscribe",
                    "params": {
                        "symbols": GOLD_SYMBOL
                    }
                }

                ws.send(json.dumps(subscribe_message))

                print("GOLD WS SUBSCRIBED")

            def on_message(ws, message):

                try:

                    data = json.loads(message)

                    print("GOLD WS MESSAGE:", data)

                    price = None

                    # Current Twelve Data format
                    if isinstance(data, dict):
                        price = safe_float(data.get("price"))

                    # Backup format
                    if price is None and isinstance(data, dict):

                        nested = data.get("data")

                        if isinstance(nested, dict):
                            price = safe_float(nested.get("price"))

                    if price is None:
                        return

                    print("GOLD PRICE:", price)

                    with lock:

                        gold_prices.append(price)

                        latest["gold"]["price"] = price
                        latest["gold"]["status"] = "LIVE"
                        latest["gold"]["updated"] = int(time.time())

                    # Analyze immediately whenever price arrives.
                    calculate_analysis(price)

                except Exception as e:

                    print("GOLD MESSAGE ERROR:", repr(e))

            def on_error(ws, error):

                print("GOLD WS ERROR:", error)

            def on_close(ws, close_status_code, close_msg):

                print(
                    "GOLD WS CLOSED:",
                    close_status_code,
                    close_msg
                )

            ws = websocket.WebSocketApp(
                websocket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            print("GOLD WS CONNECTING")

            ws.run_forever(
                ping_interval=25,
                ping_timeout=10
            )

        except Exception as e:

            print("GOLD WORKER ERROR:", repr(e))

        print("GOLD WS RECONNECTING IN 5 SECONDS")

        time.sleep(5)


# ============================================================
# TWELVE DATA CANDLE ENGINE
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
            f"CANDLE REQUEST {interval}: HTTP",
            response.status_code
        )

        data = response.json()

        values = data.get("values")

        if not isinstance(values, list):

            print(
                f"CANDLE ERROR {interval}:",
                data
            )

            return

        # Twelve Data returns newest first.
        # Reverse so oldest -> newest.
        values = list(reversed(values))

        with lock:
            candles[interval] = values

        print(
            f"CANDLES {interval}:",
            len(values)
        )

    except Exception as e:

        print(
            f"CANDLE REQUEST ERROR {interval}:",
            repr(e)
        )


def candle_worker():

    print("CANDLE ENGINE STARTING")

    while True:

        try:

            fetch_candles("5min")
            fetch_candles("15min")
            fetch_candles("1h")
            fetch_candles("1day")

            with lock:
                current_price = latest["gold"].get("price")

            if current_price is not None:
                calculate_analysis(current_price)

        except Exception as e:

            print(
                "CANDLE WORKER ERROR:",
                repr(e)
            )

        # Twelve Data Basic plan friendly refresh.
        time.sleep(60)


# ============================================================
# HTML
# ============================================================

HTML = """
<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>Trading-AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background:
        radial-gradient(circle at top, #18243d 0%, #070b14 45%, #03050a 100%);
    color: #f4f7fb;
    font-family: Arial, Helvetica, sans-serif;
    min-height: 100vh;
}

.container {
    width: 94%;
    max-width: 1100px;
    margin: auto;
    padding: 28px 0 60px;
}

.header {
    text-align: center;
    margin-bottom: 24px;
}

.header h1 {
    margin: 0;
    font-size: 34px;
    letter-spacing: 1px;
}

.header p {
    margin-top: 8px;
    color: #9ca9bc;
}

.card {
    background: rgba(13, 20, 34, 0.94);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 20px;
    padding: 24px;
    box-shadow: 0 18px 60px rgba(0,0,0,0.35);
    margin-bottom: 20px;
}

.asset-title {
    font-size: 25px;
    font-weight: bold;
}

.live {
    margin-top: 8px;
    color: #63e6a5;
    font-weight: bold;
}

.price {
    font-size: 42px;
    font-weight: bold;
    margin: 15px 0;
}

.conclusion {
    margin-top: 20px;
    padding: 22px;
    border-radius: 16px;
    background: rgba(255,255,255,0.04);
    text-align: center;
}

.conclusion-label {
    color: #9ca9bc;
    font-size: 13px;
    letter-spacing: 1.5px;
}

.decision {
    font-size: 32px;
    font-weight: bold;
    margin: 12px 0;
}

.confidence {
    color: #cbd5e1;
}

.grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 14px;
    margin-top: 18px;
}

.box {
    background: rgba(255,255,255,0.035);
    border-radius: 14px;
    padding: 16px;
}

.box-title {
    color: #8f9db1;
    font-size: 12px;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.box-value {
    font-size: 19px;
    font-weight: bold;
}

.section-title {
    font-size: 18px;
    font-weight: bold;
    margin-bottom: 14px;
}

.why {
    line-height: 1.7;
    color: #d8e0eb;
}

.timeframes {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
}

.tf {
    padding: 15px;
    border-radius: 14px;
    background: rgba(255,255,255,0.035);
    text-align: center;
}

.tf-name {
    color: #8997aa;
    font-size: 12px;
}

.tf-value {
    margin-top: 7px;
    font-weight: bold;
}

.footer {
    text-align: center;
    color: #68758a;
    font-size: 12px;
    margin-top: 25px;
}

@media (max-width: 700px) {

    .grid {
        grid-template-columns: 1fr;
    }

    .timeframes {
        grid-template-columns: repeat(2, 1fr);
    }

    .price {
        font-size: 34px;
    }

    .decision {
        font-size: 26px;
    }
}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <h1>Trading-AI</h1>

        <p>
            Live Market Intelligence
        </p>

    </div>


    <div class="card">

        <div class="asset-title">
            🥇 Gold — XAU/USD
        </div>

        <div id="status" class="live">
            Connecting...
        </div>

        <div id="price" class="price">
            --
        </div>


        <div class="conclusion">

            <div class="conclusion-label">
                AI MARKET CONCLUSION
            </div>

            <div id="decision" class="decision">
                WAIT
            </div>

            <div class="confidence">
                CONFIDENCE:
                <strong id="confidence">0</strong>%
            </div>

        </div>


        <div class="grid">

            <div class="box">

                <div class="box-title">
                    AI Score
                </div>

                <div id="score" class="box-value">
                    0
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Entry Zone
                </div>

                <div id="entry" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Invalidation
                </div>

                <div id="invalidation" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Target Liquidity
                </div>

                <div id="target" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    RSI
                </div>

                <div id="rsi" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Structure
                </div>

                <div id="structure" class="box-value">
                    --
                </div>

            </div>

        </div>

    </div>


    <div class="card">

        <div class="section-title">
            WHY?
        </div>

        <div id="why" class="why">
            Waiting for market data...
        </div>

    </div>


    <div class="card">

        <div class="section-title">
            Market Analysis
        </div>

        <div class="grid">

            <div class="box">

                <div class="box-title">
                    Overall Trend
                </div>

                <div id="trend" class="box-value">
                    Waiting
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Momentum
                </div>

                <div id="momentum" class="box-value">
                    Waiting
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    EMA20
                </div>

                <div id="ema20" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    EMA50
                </div>

                <div id="ema50" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Buy-side Liquidity
                </div>

                <div id="buyLiquidity" class="box-value">
                    --
                </div>

            </div>


            <div class="box">

                <div class="box-title">
                    Sell-side Liquidity
                </div>

                <div id="sellLiquidity" class="box-value">
                    --
                </div>

            </div>

        </div>

    </div>


    <div class="card">

        <div class="section-title">
            Timeframe Alignment
        </div>

        <div class="timeframes">

            <div class="tf">

                <div class="tf-name">
                    5 MIN
                </div>

                <div id="tf5" class="tf-value">
                    Waiting
                </div>

            </div>


            <div class="tf">

                <div class="tf-name">
                    15 MIN
                </div>

                <div id="tf15" class="tf-value">
                    Waiting
                </div>

            </div>


            <div class="tf">

                <div class="tf-name">
                    1 HOUR
                </div>

                <div id="tf1h" class="tf-value">
                    Waiting
                </div>

            </div>


            <div class="tf">

                <div class="tf-name">
                    1 DAY
                </div>

                <div id="tf1d" class="tf-value">
                    Waiting
                </div>

            </div>

        </div>

    </div>


    <div class="footer">

        Twelve Data · Live market data · Trading-AI

        <br>

        Last update:
        <span id="updated">--</span>

    </div>

</div>


<script>

function valueOrDash(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "--";
    }

    return value;
}


function priceValue(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "--";
    }

    let number = Number(value);

    if (Number.isNaN(number)) {
        return "--";
    }

    return number.toFixed(2);
}


function updatePage(data) {

    if (!data || !data.gold) {
        return;
    }

    const g = data.gold;


    document.getElementById("status").textContent =
        valueOrDash(g.status);


    document.getElementById("price").textContent =
        priceValue(g.price);


    document.getElementById("decision").textContent =
        valueOrDash(g.decision);


    document.getElementById("confidence").textContent =
        valueOrDash(g.confidence);


    document.getElementById("score").textContent =
        valueOrDash(g.score);


    document.getElementById("entry").textContent =
        priceValue(g.entry);


    document.getElementById("invalidation").textContent =
        priceValue(g.invalidation);


    document.getElementById("target").textContent =
        priceValue(g.target);


    document.getElementById("why").textContent =
        valueOrDash(g.why);


    document.getElementById("trend").textContent =
        valueOrDash(g.trend);


    document.getElementById("momentum").textContent =
        valueOrDash(g.momentum);


    document.getElementById("structure").textContent =
        valueOrDash(g.structure);


    document.getElementById("rsi").textContent =
        priceValue(g.rsi);


    document.getElementById("ema20").textContent =
        priceValue(g.ema20);


    document.getElementById("ema50").textContent =
        priceValue(g.ema50);


    document.getElementById("buyLiquidity").textContent =
        priceValue(g.buy_liquidity);


    document.getElementById("sellLiquidity").textContent =
        priceValue(g.sell_liquidity);


    document.getElementById("tf5").textContent =
        valueOrDash(g.trend_5m);


    document.getElementById("tf15").textContent =
        valueOrDash(g.trend_15m);


    document.getElementById("tf1h").textContent =
        valueOrDash(g.trend_1h);


    document.getElementById("tf1d").textContent =
        valueOrDash(g.trend_1d);


    if (g.updated) {

        const date =
            new Date(g.updated * 1000);

        document.getElementById("updated").textContent =
            date.toLocaleTimeString();

    }

}


async function loadState() {

    try {

        const response =
            await fetch("/api/state?t=" + Date.now());

        const data =
            await response.json();

        updatePage(data);

    } catch (error) {

        console.log(
            "STATE ERROR:",
            error
        );

    }

}


loadState();

setInterval(
    loadState,
    1000
);

</script>

</body>

</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/state")
def api_state():

    with lock:
        return jsonify(latest)


@app.route("/health")
def health():

    with lock:

        return jsonify({
            "status": "ok",
            "gold_price": latest["gold"]["price"],
            "decision": latest["gold"]["decision"],
            "confidence": latest["gold"]["confidence"]
        })


# ============================================================
# START ENGINES
# ============================================================

print("LIVE ENGINE STARTING")

threading.Thread(
    target=gold_worker,
    daemon=True
).start()

threading.Thread(
    target=candle_worker,
    daemon=True
).start()

print("LIVE ENGINE STARTED")


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
