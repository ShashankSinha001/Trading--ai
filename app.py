from flask import Flask, jsonify, render_template_string, make_response
import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket


app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

GOLD_SYMBOL = "XAU/USD"
STATE_FILE = "/tmp/trading_ai_state.json"

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


# =========================================================
# STATE STORAGE
# =========================================================

def save_state():
    try:
        with lock:
            data = json.dumps(latest)

        tmp_file = STATE_FILE + ".tmp"

        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(data)

        os.replace(tmp_file, STATE_FILE)

    except Exception as e:
        print("STATE SAVE ERROR:", e)


def load_state():
    try:
        if not os.path.exists(STATE_FILE):
            return

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "gold" not in data:
            return

        with lock:
            latest["gold"].update(data["gold"])

    except Exception as e:
        print("STATE LOAD ERROR:", e)


# =========================================================
# BASIC HELPERS
# =========================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def fmt_price(value):
    if value is None:
        return "--"

    try:
        return f"{float(value):.2f}"
    except Exception:
        return "--"


def calculate_ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(values[:period]) / period

    for price in values[period:]:
        ema = ((price - ema) * multiplier) + ema

    return ema


def calculate_rsi(values, period=14):
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

    if len(gains) < period:
        return None

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def trend_from_candles(data):
    if len(data) < 20:
        return "WAIT"

    closes = [
        safe_float(x.get("close"))
        for x in data
        if safe_float(x.get("close")) is not None
    ]

    if len(closes) < 20:
        return "WAIT"

    recent = closes[-10:]

    first_half = sum(recent[:5]) / 5
    second_half = sum(recent[5:]) / 5

    if second_half > first_half:
        return "BULLISH"

    if second_half < first_half:
        return "BEARISH"

    return "SIDEWAYS"


def momentum_from_prices(values):
    if len(values) < 10:
        return "WAIT"

    recent = values[-10:]

    old_avg = sum(recent[:5]) / 5
    new_avg = sum(recent[5:]) / 5

    difference = new_avg - old_avg

    if difference > 0:
        return "BUYING"

    if difference < 0:
        return "SELLING"

    return "NEUTRAL"


def structure_from_candles(data):
    if len(data) < 10:
        return "WAIT"

    highs = [
        safe_float(x.get("high"))
        for x in data[-10:]
        if safe_float(x.get("high")) is not None
    ]

    lows = [
        safe_float(x.get("low"))
        for x in data[-10:]
        if safe_float(x.get("low")) is not None
    ]

    if len(highs) < 10 or len(lows) < 10:
        return "WAIT"

    first_high = max(highs[:5])
    second_high = max(highs[5:])

    first_low = min(lows[:5])
    second_low = min(lows[5:])

    if second_high > first_high and second_low > first_low:
        return "BULLISH"

    if second_high < first_high and second_low < first_low:
        return "BEARISH"

    return "RANGE"


def liquidity_levels(data):
    if len(data) < 10:
        return None, None

    highs = [
        safe_float(x.get("high"))
        for x in data[-20:]
        if safe_float(x.get("high")) is not None
    ]

    lows = [
        safe_float(x.get("low"))
        for x in data[-20:]
        if safe_float(x.get("low")) is not None
    ]

    if not highs or not lows:
        return None, None

    buy_liquidity = max(highs)
    sell_liquidity = min(lows)

    return buy_liquidity, sell_liquidity


# =========================================================
# LIVE PRICE
# =========================================================

def update_live_price(price):
    price = safe_float(price)

    if price is None:
        return

    with lock:
        gold_prices.append(price)

        latest["gold"]["price"] = price
        latest["gold"]["status"] = "live"
        latest["gold"]["source"] = "Twelve Data WebSocket"
        latest["gold"]["updated"] = now_iso()

    save_state()

    print("STATE UPDATED: GOLD =", price)


# =========================================================
# AI ANALYSIS
# =========================================================

def calculate_analysis(current_price=None):
    with lock:
        price = (
            safe_float(current_price)
            or latest["gold"].get("price")
        )

        data_5m = list(candles["5min"])
        data_15m = list(candles["15min"])
        data_1h = list(candles["1h"])
        data_1d = list(candles["1day"])

        prices = list(gold_prices)

    if price is None:
        return

    score = 0
    reasons = []

    trend_5m = trend_from_candles(data_5m)
    trend_15m = trend_from_candles(data_15m)
    trend_1h = trend_from_candles(data_1h)
    trend_1d = trend_from_candles(data_1d)

    momentum = momentum_from_prices(prices)
    structure = structure_from_candles(data_5m)

    closes_5m = [
        safe_float(x.get("close"))
        for x in data_5m
        if safe_float(x.get("close")) is not None
    ]

    rsi = calculate_rsi(closes_5m)

    ema20 = calculate_ema(closes_5m, 20)
    ema50 = calculate_ema(closes_5m, 50)

    buy_liquidity, sell_liquidity = liquidity_levels(data_5m)

    # -----------------------------------------------------
    # MULTI-TIMEFRAME SCORE
    # -----------------------------------------------------

    if trend_5m == "BULLISH":
        score += 2
        reasons.append("5M trend is bullish")
    elif trend_5m == "BEARISH":
        score -= 2
        reasons.append("5M trend is bearish")

    if trend_15m == "BULLISH":
        score += 2
        reasons.append("15M trend is bullish")
    elif trend_15m == "BEARISH":
        score -= 2
        reasons.append("15M trend is bearish")

    if trend_1h == "BULLISH":
        score += 3
        reasons.append("1H trend is bullish")
    elif trend_1h == "BEARISH":
        score -= 3
        reasons.append("1H trend is bearish")

    if trend_1d == "BULLISH":
        score += 3
        reasons.append("1D trend is bullish")
    elif trend_1d == "BEARISH":
        score -= 3
        reasons.append("1D trend is bearish")

    # -----------------------------------------------------
    # MOMENTUM
    # -----------------------------------------------------

    if momentum == "BUYING":
        score += 1
        reasons.append("Short-term momentum shows buying pressure")

    elif momentum == "SELLING":
        score -= 1
        reasons.append("Short-term momentum shows selling pressure")

    # -----------------------------------------------------
    # STRUCTURE
    # -----------------------------------------------------

    if structure == "BULLISH":
        score += 2
        reasons.append("Market structure is bullish")

    elif structure == "BEARISH":
        score -= 2
        reasons.append("Market structure is bearish")

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    if ema20 is not None:

        if price > ema20:
            score += 1
            reasons.append("Price is above EMA20")

        elif price < ema20:
            score -= 1
            reasons.append("Price is below EMA20")

    if ema50 is not None:

        if price > ema50:
            score += 1
            reasons.append("Price is above EMA50")

        elif price < ema50:
            score -= 1
            reasons.append("Price is below EMA50")

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if rsi is not None:

        if 55 <= rsi <= 70:
            score += 1
            reasons.append("RSI supports bullish momentum")

        elif 30 <= rsi <= 45:
            score -= 1
            reasons.append("RSI supports bearish momentum")

        elif rsi > 70:
            reasons.append("RSI is overbought; upside may be extended")

        elif rsi < 30:
            reasons.append("RSI is oversold; downside may be extended")

    # -----------------------------------------------------
    # DECISION
    # -----------------------------------------------------

    decision = "WAIT"

    if score >= 7:
        decision = "BUY SIDE"

    elif score <= -7:
        decision = "SELL SIDE"

    # Higher timeframe conflict protection
    if (
        trend_1d in ("BULLISH", "BEARISH")
        and trend_1h in ("BULLISH", "BEARISH")
        and trend_1d != trend_1h
    ):
        decision = "WAIT"
        reasons.insert(
            0,
            "Higher-timeframe conflict: 1D and 1H are pointing in different directions"
        )

    # -----------------------------------------------------
    # CONFIDENCE
    # -----------------------------------------------------

    confidence = min(95, 50 + abs(score) * 4)

    if decision == "WAIT":
        confidence = min(confidence, 60)

    # -----------------------------------------------------
    # TRADE LEVELS
    # -----------------------------------------------------

    entry = price
    invalidation = "--"
    target = "--"

    if decision == "BUY SIDE":

        if sell_liquidity is not None:
            invalidation = sell_liquidity

        if buy_liquidity is not None and buy_liquidity > price:
            target = buy_liquidity

    elif decision == "SELL SIDE":

        if buy_liquidity is not None:
            invalidation = buy_liquidity

        if sell_liquidity is not None and sell_liquidity < price:
            target = sell_liquidity

    # -----------------------------------------------------
    # OVERALL TREND
    # -----------------------------------------------------

    if score > 0:
        overall_trend = "BULLISH"

    elif score < 0:
        overall_trend = "BEARISH"

    else:
        overall_trend = "WAIT"

    # -----------------------------------------------------
    # WHY
    # -----------------------------------------------------

    if not reasons:
        reasons.append("Not enough market data for a strong conclusion")

    why = " • ".join(reasons[:10])

    with lock:

        latest["gold"]["price"] = price

        latest["gold"]["decision"] = decision
        latest["gold"]["confidence"] = confidence
        latest["gold"]["score"] = score

        latest["gold"]["entry"] = fmt_price(entry)
        latest["gold"]["invalidation"] = fmt_price(invalidation)
        latest["gold"]["target"] = fmt_price(target)

        latest["gold"]["why"] = why

        latest["gold"]["trend"] = overall_trend
        latest["gold"]["momentum"] = momentum
        latest["gold"]["structure"] = structure

        latest["gold"]["trend_5m"] = trend_5m
        latest["gold"]["trend_15m"] = trend_15m
        latest["gold"]["trend_1h"] = trend_1h
        latest["gold"]["trend_1d"] = trend_1d

        latest["gold"]["rsi"] = round(rsi, 2) if rsi is not None else None

        latest["gold"]["ema20"] = (
            round(ema20, 2)
            if ema20 is not None
            else None
        )

        latest["gold"]["ema50"] = (
            round(ema50, 2)
            if ema50 is not None
            else None
        )

        latest["gold"]["buy_liquidity"] = (
            round(buy_liquidity, 2)
            if buy_liquidity is not None
            else None
        )

        latest["gold"]["sell_liquidity"] = (
            round(sell_liquidity, 2)
            if sell_liquidity is not None
            else None
        )

        latest["gold"]["candles_5m"] = len(data_5m)
        latest["gold"]["candles_15m"] = len(data_15m)
        latest["gold"]["candles_1h"] = len(data_1h)
        latest["gold"]["candles_1d"] = len(data_1d)

    save_state()

    print(
        "AI UPDATE:",
        "price=", price,
        "score=", score,
        "decision=", decision
    )


# =========================================================
# TWELVE DATA CANDLES
# =========================================================

def fetch_candles(interval):

    if not API_KEY:
        print("TWELVE DATA API KEY NOT FOUND")
        return

    try:

        url = "https://api.twelvedata.com/time_series"

        params = {
            "symbol": GOLD_SYMBOL,
            "interval": interval,
            "outputsize": 100,
            "apikey": API_KEY
        }

        response = requests.get(
            url,
            params=params,
            timeout=20
        )

        print(
            f"CANDLE REQUEST {interval}: HTTP {response.status_code}"
        )

        if response.status_code != 200:
            print(
                "CANDLE ERROR:",
                response.text[:500]
            )
            return

        data = response.json()

        values = data.get("values", [])

        if not values:
            print("NO CANDLES:", interval)
            return

        values = list(reversed(values))

        with lock:
            candles[interval] = values

        print(
            f"CANDLES {interval}: {len(values)}"
        )

        with lock:
            current_price = latest["gold"]["price"]

        if current_price is not None:
            calculate_analysis(current_price)

    except Exception as e:
        print(
            f"CANDLE ENGINE ERROR {interval}:",
            e
        )


# =========================================================
# CANDLE WORKER
# =========================================================

def candle_worker():

    print("CANDLE ENGINE STARTING")

    last_5m = 0
    last_15m = 0
    last_1h = 0
    last_1d = 0

    while True:

        try:

            now = time.time()

            if now - last_5m >= 120:
                fetch_candles("5min")
                last_5m = now
                time.sleep(2)

            if now - last_15m >= 300:
                fetch_candles("15min")
                last_15m = now
                time.sleep(2)

            if now - last_1h >= 600:
                fetch_candles("1h")
                last_1h = now
                time.sleep(2)

            if now - last_1d >= 1800:
                fetch_candles("1day")
                last_1d = now

            time.sleep(2)

        except Exception as e:
            print("CANDLE LOOP ERROR:", e)
            time.sleep(5)


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_worker():

    print("GOLD ENGINE STARTING")

    if not API_KEY:
        print("GOLD ENGINE: API KEY MISSING")
        return

    last_analysis = 0

    while True:

        try:

            ws_url = (
                "wss://ws.twelvedata.com/v1/quotes/price"
                f"?apikey={API_KEY}"
            )

            print("GOLD WS CONNECTING")

            ws = websocket.create_connection(
                ws_url,
                timeout=30
            )

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(json.dumps(subscribe_message))

            print("GOLD WS SUBSCRIBED")

            while True:

                raw = ws.recv()

                if not raw:
                    continue

                try:
                    message = json.loads(raw)
                except Exception:
                    continue

                print("GOLD WS MESSAGE:", message)

                price = safe_float(
                    message.get("price")
                )

                if price is None:

                    data = message.get("data")

                    if isinstance(data, dict):
                        price = safe_float(
                            data.get("price")
                        )

                if price is None:
                    continue

                update_live_price(price)

                now = time.time()

                if now - last_analysis >= 5:

                    calculate_analysis(price)

                    last_analysis = now

        except Exception as e:

            print(
                "GOLD WS ERROR:",
                e
            )

            time.sleep(5)


# =========================================================
# API
# =========================================================

@app.route("/api/state")
def api_state():

    load_state()

    with lock:
        data = json.loads(
            json.dumps(latest)
        )

    response = make_response(
        jsonify(data)
    )

    response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, max-age=0"
    )

    response.headers["Pragma"] = "no-cache"

    return response


# =========================================================
# DETAILED DASHBOARD
# =========================================================

HTML = """
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>Trading-AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background:
        radial-gradient(
            circle at top,
            #17213a 0,
            #090d18 42%,
            #05070d 100%
        );
    color: #ffffff;
    font-family:
        Arial,
        Helvetica,
        sans-serif;
    min-height: 100vh;
}

.container {
    width: min(1200px, 94%);
    margin: auto;
    padding: 22px 0 40px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 15px;
    margin-bottom: 20px;
}

.logo {
    font-size: 28px;
    font-weight: 800;
    letter-spacing: .5px;
}

.subtitle {
    color: #8e9bb8;
    margin-top: 5px;
    font-size: 13px;
}

.live-pill {
    padding: 9px 14px;
    border-radius: 30px;
    background: rgba(0, 220, 140, .12);
    border: 1px solid rgba(0, 220, 140, .35);
    color: #52e7a9;
    font-size: 12px;
    font-weight: 700;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(12, 1fr);
    gap: 14px;
}

.card {
    background:
        rgba(15, 21, 36, .88);
    border: 1px solid
        rgba(255,255,255,.08);
    border-radius: 18px;
    padding: 18px;
    box-shadow:
        0 15px 40px
        rgba(0,0,0,.22);
    backdrop-filter: blur(10px);
}

.market-card {
    grid-column: span 12;
}

.decision-card {
    grid-column: span 6;
}

.setup-card {
    grid-column: span 6;
}

.technical-card {
    grid-column: span 6;
}

.timeframe-card {
    grid-column: span 6;
}

.liquidity-card {
    grid-column: span 6;
}

.data-card {
    grid-column: span 6;
}

.why-card {
    grid-column: span 12;
}

.card-title {
    color: #8490aa;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
    margin-bottom: 15px;
}

.symbol {
    color: #dce4f7;
    font-size: 15px;
    font-weight: 700;
}

.price {
    font-size: 43px;
    font-weight: 800;
    margin-top: 8px;
    letter-spacing: -1px;
}

.price-status {
    color: #53e4aa;
    font-size: 12px;
    font-weight: 700;
    margin-top: 5px;
}

.decision {
    font-size: 32px;
    font-weight: 900;
    margin: 8px 0;
}

.wait {
    color: #ffd166;
}

.buy {
    color: #4ee6a0;
}

.sell {
    color: #ff6675;
}

.confidence {
    color: #9da8c0;
    font-size: 13px;
}

.score {
    margin-top: 13px;
    font-size: 14px;
    color: #aab5ca;
}

.score strong {
    color: #ffffff;
    font-size: 20px;
}

.metrics {
    display: grid;
    grid-template-columns:
        repeat(2, 1fr);
    gap: 10px;
}

.metric {
    background: rgba(255,255,255,.035);
    border-radius: 12px;
    padding: 12px;
    border: 1px solid
        rgba(255,255,255,.05);
}

.metric-label {
    color: #7f8ba5;
    font-size: 11px;
    margin-bottom: 6px;
}

.metric-value {
    color: #f2f5fb;
    font-size: 17px;
    font-weight: 700;
}

.tf-grid {
    display: grid;
    grid-template-columns:
        repeat(2, 1fr);
    gap: 10px;
}

.tf {
    padding: 13px;
    border-radius: 13px;
    background: rgba(255,255,255,.035);
    border: 1px solid
        rgba(255,255,255,.05);
}

.tf-label {
    color: #7f8ba5;
    font-size: 11px;
    margin-bottom: 7px;
}

.tf-value {
    font-weight: 800;
    font-size: 14px;
}

.bullish {
    color: #4ee6a0;
}

.bearish {
    color: #ff6675;
}

.neutral {
    color: #ffd166;
}

.liquidity-value {
    font-size: 21px;
    font-weight: 800;
}

.buy-liq {
    color: #4ee6a0;
}

.sell-liq {
    color: #ff6675;
}

.data-row {
    display: flex;
    justify-content: space-between;
    gap: 15px;
    padding: 9px 0;
    border-bottom: 1px solid
        rgba(255,255,255,.06);
}

.data-row:last-child {
    border-bottom: 0;
}

.data-label {
    color: #7f8ba5;
    font-size: 12px;
}

.data-value {
    color: #ffffff;
    font-size: 12px;
    font-weight: 700;
    text-align: right;
}

.why {
    color: #c2cadb;
    font-size: 14px;
    line-height: 1.8;
    background: rgba(255,255,255,.035);
    padding: 16px;
    border-radius: 13px;
}

.footer {
    text-align: center;
    color: #5f6b83;
    font-size: 11px;
    margin-top: 20px;
}

@media (max-width: 800px) {

    .decision-card,
    .setup-card,
    .technical-card,
    .timeframe-card,
    .liquidity-card,
    .data-card {
        grid-column: span 12;
    }

    .price {
        font-size: 36px;
    }

    .header {
        align-items: flex-start;
        flex-direction: column;
    }

}

</style>

</head>


<body>

<div class="container">

    <div class="header">

        <div>

            <div class="logo">
                Trading-AI
            </div>

            <div class="subtitle">
                Live Market Intelligence
            </div>

        </div>

        <div
            id="connection"
            class="live-pill"
        >
            ● CONNECTING
        </div>

    </div>


    <div class="grid">


        <!-- MARKET -->

        <div class="card market-card">

            <div class="card-title">
                LIVE MARKET
            </div>

            <div class="symbol">
                🥇 Gold — XAU/USD
            </div>

            <div
                id="price"
                class="price"
            >
                --
            </div>

            <div
                id="status"
                class="price-status"
            >
                Waiting for live data...
            </div>

        </div>


        <!-- DECISION -->

        <div class="card decision-card">

            <div class="card-title">
                AI MARKET CONCLUSION
            </div>

            <div
                id="decision"
                class="decision wait"
            >
                WAIT
            </div>

            <div class="confidence">
                Confidence:
                <strong id="confidence">
                    0%
                </strong>
            </div>

            <div class="score">
                AI SCORE:
                <strong id="score">
                    0
                </strong>
            </div>

        </div>


        <!-- SETUP -->

        <div class="card setup-card">

            <div class="card-title">
                TRADE SETUP
            </div>

            <div class="metrics">

                <div class="metric">
                    <div class="metric-label">
                        ENTRY
                    </div>

                    <div
                        id="entry"
                        class="metric-value"
                    >
                        --
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        INVALIDATION
                    </div>

                    <div
                        id="invalidation"
                        class="metric-value"
                    >
                        --
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        TARGET LIQUIDITY
                    </div>

                    <div
                        id="target"
                        class="metric-value"
                    >
                        --
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        OVERALL TREND
                    </div>

                    <div
                        id="overallTrend"
                        class="metric-value"
                    >
                        WAIT
                    </div>
                </div>

            </div>

        </div>


        <!-- TECHNICAL -->

        <div class="card technical-card">

            <div class="card-title">
                TECHNICAL INTELLIGENCE
            </div>

            <div class="metrics">

                <div class="metric">
                    <div class="metric-label">
                        RSI
                    </div>

                    <div
                        id="rsi"
                        class="metric-value"
                    >
                        --
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        EMA 20
                    </div>

                    <div
                        id="ema20"
                        class="metric-value"
                    >
                        --
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        EMA 50
                    </div>

                    <div
                        id="ema50"
                        class="metric-value"
                    >
                        --
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        MOMENTUM
                    </div>

                    <div
                        id="momentum"
                        class="metric-value"
                    >
                        WAIT
                    </div>
                </div>


                <div class="metric">
                    <div class="metric-label">
                        MARKET STRUCTURE
                    </div>

                    <div
                        id="structure"
                        class="metric-value"
                    >
                        WAIT
                    </div>
                </div>

            </div>

        </div>


        <!-- TIMEFRAMES -->

        <div class="card timeframe-card">

            <div class="card-title">
                MULTI-TIMEFRAME ANALYSIS
            </div>

            <div class="tf-grid">

                <div class="tf">

                    <div class="tf-label">
                        5 MIN
                    </div>

                    <div
                        id="tf5"
                        class="tf-value neutral"
                    >
                        WAIT
                    </div>

                </div>


                <div class="tf">

                    <div class="tf-label">
                        15 MIN
                    </div>

                    <div
                        id="tf15"
                        class="tf-value neutral"
                    >
                        WAIT
                    </div>

                </div>


                <div class="tf">

                    <div class="tf-label">
                        1 HOUR
                    </div>

                    <div
                        id="tf1h"
                        class="tf-value neutral"
                    >
                        WAIT
                    </div>

                </div>


                <div class="tf">

                    <div class="tf-label">
                        1 DAY
                    </div>

                    <div
                        id="tf1d"
                        class="tf-value neutral"
                    >
                        WAIT
                    </div>

                </div>

            </div>

        </div>


        <!-- LIQUIDITY -->

        <div class="card liquidity-card">

            <div class="card-title">
                LIQUIDITY MAP
            </div>

            <div class="metrics">

                <div class="metric">

                    <div class="metric-label">
                        BUY-SIDE LIQUIDITY
                    </div>

                    <div
                        id="buyLiquidity"
                        class="liquidity-value buy-liq"
                    >
                        --
                    </div>

                </div>


                <div class="metric">

                    <div class="metric-label">
                        SELL-SIDE LIQUIDITY
                    </div>

                    <div
                        id="sellLiquidity"
                        class="liquidity-value sell-liq"
                    >
                        --
                    </div>

                </div>

            </div>

        </div>


        <!-- DATA HEALTH -->

        <div class="card data-card">

            <div class="card-title">
                DATA & ENGINE STATUS
            </div>

            <div class="data-row">

                <div class="data-label">
                    Source
                </div>

                <div
                    id="source"
                    class="data-value"
                >
                    --
                </div>

            </div>


            <div class="data-row">

                <div class="data-label">
                    5M Candles
                </div>

                <div
                    id="candles5"
                    class="data-value"
                >
                    0
                </div>

            </div>


            <div class="data-row">

                <div class="data-label">
                    15M Candles
                </div>

                <div
                    id="candles15"
                    class="data-value"
                >
                    0
                </div>

            </div>


            <div class="data-row">

                <div class="data-label">
                    1H Candles
                </div>

                <div
                    id="candles1h"
                    class="data-value"
                >
                    0
                </div>

            </div>


            <div class="data-row">

                <div class="data-label">
                    1D Candles
                </div>

                <div
                    id="candles1d"
                    class="data-value"
                >
                    0
                </div>

            </div>


            <div class="data-row">

                <div class="data-label">
                    Last Updated
                </div>

                <div
                    id="updated"
                    class="data-value"
                >
                    --
                </div>

            </div>

        </div>


        <!-- WHY -->

        <div class="card why-card">

            <div class="card-title">
                WHY? — AI MARKET REASONING
            </div>

            <div
                id="why"
                class="why"
            >
                Waiting for live market data...
            </div>

        </div>


    </div>


    <div class="footer">
        Trading-AI • Live analysis engine • Data is informational
    </div>

</div>


<script>

function setText(id, value) {

    const el = document.getElementById(id);

    if (!el) return;

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        el.textContent = "--";
    } else {
        el.textContent = value;
    }
}


function styleDirection(id, value) {

    const el = document.getElementById(id);

    if (!el) return;

    el.classList.remove(
        "bullish",
        "bearish",
        "neutral",
        "buy",
        "sell",
        "wait"
    );

    const text = String(value || "").toUpperCase();

    if (
        text.includes("BULLISH") ||
        text.includes("BUYING") ||
        text.includes("BUY SIDE")
    ) {

        el.classList.add("bullish");

    } else if (
        text.includes("BEARISH") ||
        text.includes("SELLING") ||
        text.includes("SELL SIDE")
    ) {

        el.classList.add("bearish");

    } else {

        el.classList.add("neutral");

    }

    el.textContent = value || "WAIT";
}


function updateDashboard(data) {

    if (!data || !data.gold) {
        return;
    }

    const g = data.gold;


    // -----------------------------------------
    // CONNECTION
    // -----------------------------------------

    const connection =
        document.getElementById("connection");

    if (g.status === "live") {

        connection.textContent =
            "● LIVE STREAM";

        connection.style.color =
            "#52e7a9";

    } else {

        connection.textContent =
            "● CONNECTING";

        connection.style.color =
            "#ffd166";
    }


    // -----------------------------------------
    // MARKET
    // -----------------------------------------

    setText(
        "price",
        g.price !== null
            ? Number(g.price).toFixed(2)
            : "--"
    );


    setText(
        "status",
        g.status === "live"
            ? "LIVE • Twelve Data WebSocket"
            : "Waiting for live data..."
    );


    // -----------------------------------------
    // DECISION
    // -----------------------------------------

    setText(
        "decision",
        g.decision || "WAIT"
    );

    const decision =
        document.getElementById("decision");

    decision.classList.remove(
        "buy",
        "sell",
        "wait"
    );

    if (g.decision === "BUY SIDE") {

        decision.classList.add("buy");

    } else if (g.decision === "SELL SIDE") {

        decision.classList.add("sell");

    } else {

        decision.classList.add("wait");
    }


    setText(
        "confidence",
        (g.confidence || 0) + "%"
    );

    setText(
        "score",
        g.score !== undefined
            ? g.score
            : 0
    );


    // -----------------------------------------
    // SETUP
    // -----------------------------------------

    setText("entry", g.entry);
    setText("invalidation", g.invalidation);
    setText("target", g.target);

    styleDirection(
        "overallTrend",
        g.trend || "WAIT"
    );


    // -----------------------------------------
    // TECHNICAL
    // -----------------------------------------

    setText(
        "rsi",
        g.rsi !== null
            ? Number(g.rsi).toFixed(2)
            : "--"
    );

    setText(
        "ema20",
        g.ema20 !== null
            ? Number(g.ema20).toFixed(2)
            : "--"
    );

    setText(
        "ema50",
        g.ema50 !== null
            ? Number(g.ema50).toFixed(2)
            : "--"
    );

    styleDirection(
        "momentum",
        g.momentum || "WAIT"
    );

    styleDirection(
        "structure",
        g.structure || "WAIT"
    );


    // -----------------------------------------
    // TIMEFRAMES
    // -----------------------------------------

    styleDirection(
        "tf5",
        g.trend_5m || "WAIT"
    );

    styleDirection(
        "tf15",
        g.trend_15m || "WAIT"
    );

    styleDirection(
        "tf1h",
        g.trend_1h || "WAIT"
    );

    styleDirection(
        "tf1d",
        g.trend_1d || "WAIT"
    );


    // -----------------------------------------
    // LIQUIDITY
    // -----------------------------------------

    setText(
        "buyLiquidity",
        g.buy_liquidity !== null
            ? Number(g.buy_liquidity).toFixed(2)
            : "--"
    );

    setText(
        "sellLiquidity",
        g.sell_liquidity !== null
            ? Number(g.sell_liquidity).toFixed(2)
            : "--"
    );


    // -----------------------------------------
    // DATA
    // -----------------------------------------

    setText(
        "source",
        g.source || "--"
    );

    setText(
        "candles5",
        g.candles_5m || 0
    );

    setText(
        "candles15",
        g.candles_15m || 0
    );

    setText(
        "candles1h",
        g.candles_1h || 0
    );

    setText(
        "candles1d",
        g.candles_1d || 0
    );


    if (g.updated) {

        try {

            const d =
                new Date(g.updated);

            setText(
                "updated",
                d.toLocaleString()
            );

        } catch {

            setText(
                "updated",
                g.updated
            );
        }

    } else {

        setText(
            "updated",
            "--"
        );
    }


    // -----------------------------------------
    // WHY
    // -----------------------------------------

    setText(
        "why",
        g.why ||
        "Waiting for live market data..."
    );
}


async function loadState() {

    try {

        const response =
            await fetch(
                "/api/state?t=" +
                Date.now(),
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            throw new Error(
                "HTTP " + response.status
            );
        }

        const data =
            await response.json();

        updateDashboard(data);

    } catch (error) {

        console.log(
            "STATE ERROR:",
            error
        );

        const connection =
            document.getElementById(
                "connection"
            );

        connection.textContent =
            "● RECONNECTING";

        connection.style.color =
            "#ffd166";
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


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    response = make_response(
        render_template_string(HTML)
    )

    response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, max-age=0"
    )

    response.headers["Pragma"] = "no-cache"

    return response


# =========================================================
# START ENGINES
# =========================================================

print("LIVE ENGINE STARTING")

load_state()

threading.Thread(
    target=gold_worker,
    daemon=True
).start()

threading.Thread(
    target=candle_worker,
    daemon=True
).start()

print("LIVE ENGINE STARTED")


# =========================================================
# RUN
# =========================================================

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
