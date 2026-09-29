from flask import Flask, jsonify, render_template_string
import os
import json
import time
import threading
import websocket
import requests
from collections import deque

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")
GOLD_SYMBOL = "XAU/USD"

latest = {
    "gold": {
        "price": None,
        "status": "Connecting...",
        "source": "Twelve Data",
        "updated": None,
        "decision": "WAIT",
        "confidence": 0,
        "score": 0,
        "entry": "--",
        "invalidation": "--",
        "target": "--",
        "why": "Waiting for market data...",
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

gold_prices = deque(maxlen=500)

candles = {
    "5min": [],
    "15min": [],
    "1h": [],
    "1day": []
}

lock = threading.RLock()


# =========================================================
# HELPERS
# =========================================================

def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def average(values):
    clean = []

    for value in values:
        number = safe_float(value)

        if number is not None:
            clean.append(number)

    if not clean:
        return None

    return sum(clean) / len(clean)


def candle_closes(data):
    closes = []

    for candle in data:
        close = safe_float(candle.get("close"))

        if close is not None:
            closes.append(close)

    return closes


# =========================================================
# EMA
# =========================================================

def ema(values, period):
    clean = []

    for value in values:
        number = safe_float(value)

        if number is not None:
            clean.append(number)

    if len(clean) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(clean[:period]) / period

    for price in clean[period:]:
        result = ((price - result) * multiplier) + result

    return result


# =========================================================
# RSI
# =========================================================

def calculate_rsi(values, period=14):
    clean = []

    for value in values:
        number = safe_float(value)

        if number is not None:
            clean.append(number)

    if len(clean) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(clean)):
        change = clean[i] - clean[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            ((avg_gain * (period - 1)) + gains[i])
            / period
        )

        avg_loss = (
            ((avg_loss * (period - 1)) + losses[i])
            / period
        )

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# =========================================================
# TREND
# =========================================================

def trend_from_candles(data):
    closes = candle_closes(data)

    if len(closes) < 10:
        return "WAIT"

    recent = average(closes[-5:])
    previous = average(closes[-10:-5])

    if recent is None or previous is None:
        return "WAIT"

    if recent > previous:
        return "BULLISH"

    if recent < previous:
        return "BEARISH"

    return "WAIT"


# =========================================================
# MOMENTUM
# =========================================================

def momentum_from_prices(values):
    if len(values) < 10:
        return "WAIT"

    recent = average(values[-5:])
    previous = average(values[-10:-5])

    if recent is None or previous is None:
        return "WAIT"

    if recent > previous:
        return "BULLISH"

    if recent < previous:
        return "BEARISH"

    return "WAIT"


# =========================================================
# MARKET STRUCTURE
# =========================================================

def structure_from_candles(data):
    if len(data) < 10:
        return "WAIT"

    highs = []
    lows = []

    for candle in data[-10:]:
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    if len(highs) < 10 or len(lows) < 10:
        return "WAIT"

    recent_high = max(highs[-5:])
    previous_high = max(highs[:5])

    recent_low = min(lows[-5:])
    previous_low = min(lows[:5])

    if (
        recent_high > previous_high
        and recent_low > previous_low
    ):
        return "BULLISH"

    if (
        recent_high < previous_high
        and recent_low < previous_low
    ):
        return "BEARISH"

    return "MIXED"


# =========================================================
# LIQUIDITY
# =========================================================

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

    if not highs or not lows:
        return None, None

    return max(highs), min(lows)


# =========================================================
# AI ANALYSIS
# =========================================================

def calculate_analysis(price=None):

    with lock:

        if price is None:
            price = latest["gold"]["price"]

        if price is None:
            latest["gold"]["decision"] = "WAIT"
            latest["gold"]["confidence"] = 0
            latest["gold"]["score"] = 0
            latest["gold"]["why"] = (
                "Waiting for live gold price..."
            )
            return

        live_history = list(gold_prices)

        local_candles = {
            key: list(value)
            for key, value in candles.items()
        }

    if len(live_history) < 5:

        with lock:
            latest["gold"]["price"] = price
            latest["gold"]["decision"] = "WAIT"
            latest["gold"]["confidence"] = 0
            latest["gold"]["score"] = 0
            latest["gold"]["why"] = (
                "Collecting live market data..."
            )

        return

    trend_5m = trend_from_candles(
        local_candles["5min"]
    )

    trend_15m = trend_from_candles(
        local_candles["15min"]
    )

    trend_1h = trend_from_candles(
        local_candles["1h"]
    )

    trend_1d = trend_from_candles(
        local_candles["1day"]
    )

    momentum = momentum_from_prices(
        live_history
    )

    structure = structure_from_candles(
        local_candles["5min"]
    )

    five_min_closes = candle_closes(
        local_candles["5min"]
    )

    combined_prices = (
        five_min_closes[-150:]
        + live_history[-150:]
    )

    ema20 = ema(
        combined_prices,
        20
    )

    ema50 = ema(
        combined_prices,
        50
    )

    rsi = calculate_rsi(
        combined_prices,
        14
    )

    liquidity_high, liquidity_low = (
        liquidity_levels(
            local_candles["5min"]
        )
    )

    score = 0
    reasons = []

    weights = {
        "5m": 2,
        "15m": 2,
        "1h": 3,
        "1d": 3
    }

    trends = {
        "5m": trend_5m,
        "15m": trend_15m,
        "1h": trend_1h,
        "1d": trend_1d
    }

    for timeframe, trend in trends.items():

        weight = weights[timeframe]

        if trend == "BULLISH":
            score += weight
            reasons.append(
                f"{timeframe} trend bullish"
            )

        elif trend == "BEARISH":
            score -= weight
            reasons.append(
                f"{timeframe} trend bearish"
            )

    if momentum == "BULLISH":
        score += 1
        reasons.append(
            "Live momentum bullish"
        )

    elif momentum == "BEARISH":
        score -= 1
        reasons.append(
            "Live momentum bearish"
        )

    if structure == "BULLISH":
        score += 2
        reasons.append(
            "5m market structure bullish"
        )

    elif structure == "BEARISH":
        score -= 2
        reasons.append(
            "5m market structure bearish"
        )

    if ema20 is not None:

        if price > ema20:
            score += 1
            reasons.append(
                "Price above EMA20"
            )

        elif price < ema20:
            score -= 1
            reasons.append(
                "Price below EMA20"
            )

    if ema50 is not None:

        if price > ema50:
            score += 1
            reasons.append(
                "Price above EMA50"
            )

        elif price < ema50:
            score -= 1
            reasons.append(
                "Price below EMA50"
            )

    if rsi is not None:

        if 55 <= rsi <= 70:
            score += 1
            reasons.append(
                f"RSI supportive ({rsi:.1f})"
            )

        elif 30 <= rsi <= 45:
            score -= 1
            reasons.append(
                f"RSI bearish zone ({rsi:.1f})"
            )

        elif rsi > 70:
            reasons.append(
                f"RSI elevated ({rsi:.1f})"
            )

        elif rsi < 30:
            reasons.append(
                f"RSI weak/oversold ({rsi:.1f})"
            )

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

        reasons.append(
            "1D and 1H trend conflict"
        )

    if decision == "BUY SIDE":

        entry = price

        invalidation = (
            liquidity_low
            if liquidity_low is not None
            else price * 0.995
        )

        target = (
            liquidity_high
            if liquidity_high is not None
            else price * 1.01
        )

    elif decision == "SELL SIDE":

        entry = price

        invalidation = (
            liquidity_high
            if liquidity_high is not None
            else price * 1.005
        )

        target = (
            liquidity_low
            if liquidity_low is not None
            else price * 0.99
        )

    else:

        entry = "--"
        invalidation = "--"
        target = "--"

    confidence = min(
        95,
        50 + abs(score) * 4
    )

    if decision == "WAIT":
        confidence = min(
            confidence,
            60
        )

    if reasons:
        why = " | ".join(
            reasons[-8:]
        )
    else:
        why = (
            "Waiting for stronger "
            "market confirmation."
        )

    with lock:

        latest["gold"]["price"] = price
        latest["gold"]["decision"] = decision
        latest["gold"]["confidence"] = confidence
        latest["gold"]["score"] = score

        latest["gold"]["entry"] = (
            round(entry, 4)
            if isinstance(entry, (int, float))
            else entry
        )

        latest["gold"]["invalidation"] = (
            round(invalidation, 4)
            if isinstance(invalidation, (int, float))
            else invalidation
        )

        latest["gold"]["target"] = (
            round(target, 4)
            if isinstance(target, (int, float))
            else target
        )

        latest["gold"]["trend"] = trend_1h
        latest["gold"]["momentum"] = momentum
        latest["gold"]["structure"] = structure

        latest["gold"]["trend_5m"] = trend_5m
        latest["gold"]["trend_15m"] = trend_15m
        latest["gold"]["trend_1h"] = trend_1h
        latest["gold"]["trend_1d"] = trend_1d

        latest["gold"]["rsi"] = (
            round(rsi, 2)
            if rsi is not None
            else None
        )

        latest["gold"]["ema20"] = (
            round(ema20, 4)
            if ema20 is not None
            else None
        )

        latest["gold"]["ema50"] = (
            round(ema50, 4)
            if ema50 is not None
            else None
        )

        latest["gold"]["buy_liquidity"] = (
            round(liquidity_low, 4)
            if liquidity_low is not None
            else None
        )

        latest["gold"]["sell_liquidity"] = (
            round(liquidity_high, 4)
            if liquidity_high is not None
            else None
        )

        latest["gold"]["candles_5m"] = len(
            local_candles["5min"]
        )

        latest["gold"]["candles_15m"] = len(
            local_candles["15min"]
        )

        latest["gold"]["candles_1h"] = len(
            local_candles["1h"]
        )

        latest["gold"]["candles_1d"] = len(
            local_candles["1day"]
        )

        latest["gold"]["why"] = why


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_worker():

    print(
        "GOLD ENGINE STARTING",
        flush=True
    )

    while True:

        try:

            if not API_KEY:

                with lock:
                    latest["gold"]["status"] = (
                        "API KEY MISSING"
                    )

                print(
                    "TWELVE_DATA_API_KEY MISSING",
                    flush=True
                )

                time.sleep(10)
                continue

            print(
                "GOLD WS CONNECT ATTEMPT",
                flush=True
            )

            websocket_url = (
                "wss://ws.twelvedata.com/"
                "v1/quotes/price?apikey="
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
                        "symbols": GOLD_SYMBOL
                    }
                }

                ws.send(
                    json.dumps(
                        subscribe_message
                    )
                )

                print(
                    "GOLD SUBSCRIBE SENT:",
                    GOLD_SYMBOL,
                    flush=True
                )

                with lock:
                    latest["gold"]["status"] = "LIVE"

            def on_message(ws, message):

                try:

                    data = json.loads(message)

                    print(
                        "GOLD WS MESSAGE:",
                        data,
                        flush=True
                    )

                    price = None

                    if isinstance(data, dict):

                        if "price" in data:

                            price = safe_float(
                                data.get("price")
                            )

                        elif (
                            "data" in data
                            and isinstance(
                                data["data"],
                                dict
                            )
                        ):

                            price = safe_float(
                                data["data"].get(
                                    "price"
                                )
                            )

                    if price is None:
                        return

                    with lock:

                        gold_prices.append(
                            price
                        )

                        latest["gold"]["price"] = (
                            price
                        )

                        latest["gold"]["status"] = (
                            "LIVE"
                        )

                        latest["gold"]["updated"] = (
                            time.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        )

                    print(
                        "GOLD PRICE:",
                        price,
                        flush=True
                    )

                    calculate_analysis(
                        price
                    )

                except Exception as error:

                    print(
                        "GOLD MESSAGE ERROR:",
                        str(error),
                        flush=True
                    )

            def on_error(ws, error):

                print(
                    "GOLD WEBSOCKET ERROR:",
                    str(error),
                    flush=True
                )

                with lock:
                    latest["gold"]["status"] = (
                        "RECONNECTING"
                    )

            def on_close(
                ws,
                close_status_code,
                close_msg
            ):

                print(
                    "GOLD WEBSOCKET CLOSED:",
                    close_status_code,
                    close_msg,
                    flush=True
                )

                with lock:
                    latest["gold"]["status"] = (
                        "RECONNECTING"
                    )

            ws = websocket.WebSocketApp(
                websocket_url,
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
                "GOLD WORKER ERROR:",
                str(error),
                flush=True
            )

        time.sleep(5)


# =========================================================
# CANDLE ENGINE
# =========================================================

def fetch_candles(interval):

    try:

        if not API_KEY:
            return []

        url = (
            "https://api.twelvedata.com/"
            "time_series"
        )

        params = {
            "symbol": GOLD_SYMBOL,
            "interval": interval,
            "outputsize": 100,
            "apikey": API_KEY
        }

        print(
            f"CANDLE REQUEST {interval}: START",
            flush=True
        )

        response = requests.get(
            url,
            params=params,
            timeout=15
        )

        print(
            f"CANDLE REQUEST {interval}: "
            f"HTTP {response.status_code}",
            flush=True
        )

        data = response.json()

        if "values" not in data:

            print(
                f"CANDLE ERROR {interval}:",
                data,
                flush=True
            )

            return []

        values = data["values"]

        values.reverse()

        print(
            f"CANDLES {interval}: "
            f"{len(values)}",
            flush=True
        )

        return values

    except Exception as error:

        print(
            f"CANDLE EXCEPTION {interval}:",
            str(error),
            flush=True
        )

        return []


def candle_worker():

    print(
        "CANDLE ENGINE STARTING",
        flush=True
    )

    while True:

        try:

            for interval in [
                "5min",
                "15min",
                "1h",
                "1day"
            ]:

                data = fetch_candles(
                    interval
                )

                if data:

                    with lock:
                        candles[interval] = data

            calculate_analysis()

        except Exception as error:

            print(
                "CANDLE WORKER ERROR:",
                str(error),
                flush=True
            )

        time.sleep(60)


# =========================================================
# FRONTEND
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
            #142b44 0%,
            #07111f 45%,
            #030912 100%
        );
    color: #ffffff;
    font-family:
        Arial,
        Helvetica,
        sans-serif;
    min-height: 100vh;
}

.container {
    max-width: 1100px;
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
    color: #94a3b8;
    margin-top: 6px;
}

.card {
    background:
        rgba(
            13,
            27,
            42,
            0.96
        );
    border:
        1px solid
        #1d344c;
    border-radius: 20px;
    padding: 22px;
    margin-bottom: 18px;
    box-shadow:
        0 15px 45px
        rgba(
            0,
            0,
            0,
            0.28
        );
}

.asset-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 15px;
    flex-wrap: wrap;
}

.asset {
    font-size: 23px;
    font-weight: 800;
}

.live {
    color: #4ade80;
    font-weight: 700;
    font-size: 14px;
}

.price {
    font-size: 44px;
    font-weight: 800;
    margin-top: 12px;
}

.source {
    color: #64748b;
    font-size: 13px;
    margin-top: 5px;
}

.conclusion {
    margin-top: 22px;
    padding: 25px;
    border-radius: 18px;
    background:
        linear-gradient(
            135deg,
            #102338,
            #091727
        );
    text-align: center;
    border:
        1px solid
        #24415c;
}

.conclusion-title {
    color: #94a3b8;
    font-size: 13px;
    letter-spacing: 1.5px;
}

.decision {
    font-size: 34px;
    font-weight: 900;
    margin: 10px 0;
}

.confidence {
    color: #cbd5e1;
    font-size: 17px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(
            4,
            1fr
        );
    gap: 12px;
    margin-top: 18px;
}

.metric {
    background: #091625;
    border:
        1px solid
        #182d43;
    border-radius: 14px;
    padding: 15px;
}

.label {
    color: #718096;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
}

.value {
    font-size: 19px;
    font-weight: 800;
    margin-top: 7px;
}

.section-title {
    margin-top: 25px;
    margin-bottom: 12px;
    font-size: 18px;
    font-weight: 800;
}

.analysis-grid {
    display: grid;
    grid-template-columns:
        repeat(
            3,
            1fr
        );
    gap: 12px;
}

.analysis {
    background: #091625;
    border:
        1px solid
        #182d43;
    padding: 15px;
    border-radius: 14px;
}

.analysis-name {
    color: #718096;
    font-size: 12px;
}

.analysis-value {
    font-size: 18px;
    font-weight: 800;
    margin-top: 6px;
}

.timeframes {
    display: grid;
    grid-template-columns:
        repeat(
            4,
            1fr
        );
    gap: 10px;
}

.timeframe {
    background: #091625;
    border-radius: 13px;
    padding: 14px;
    text-align: center;
}

.tf-name {
    color: #718096;
    font-size: 12px;
}

.tf-value {
    font-weight: 800;
    margin-top: 6px;
}

.reason {
    color: #cbd5e1;
    line-height: 1.7;
    background: #091625;
    border-radius: 14px;
    padding: 16px;
    border:
        1px solid
        #182d43;
}

.liquidity-grid {
    display: grid;
    grid-template-columns:
        repeat(
            2,
            1fr
        );
    gap: 12px;
}

.liquidity {
    padding: 18px;
    border-radius: 14px;
    background: #091625;
    border:
        1px solid
        #182d43;
}

.liquidity-name {
    color: #94a3b8;
    font-size: 13px;
}

.liquidity-value {
    font-size: 22px;
    font-weight: 800;
    margin-top: 7px;
}

.footer {
    text-align: center;
    color: #475569;
    font-size: 12px;
    padding: 15px 0 30px;
}

@media (max-width: 750px) {

    .grid {
        grid-template-columns:
            repeat(
                2,
                1fr
            );
    }

    .analysis-grid {
        grid-template-columns:
            repeat(
                2,
                1fr
            );
    }

    .timeframes {
        grid-template-columns:
            repeat(
                2,
                1fr
            );
    }

    .price {
        font-size: 36px;
    }
}

@media (max-width: 450px) {

    .grid {
        grid-template-columns: 1fr;
    }

    .analysis-grid {
        grid-template-columns: 1fr;
    }

    .liquidity-grid {
        grid-template-columns: 1fr;
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
Live Market Intelligence Engine
</div>

</div>

<div class="card">

<div class="asset-header">

<div class="asset">
🥇 Gold — XAU/USD
</div>

<div
class="live"
id="status"
>
Connecting...
</div>

</div>

<div
class="price"
id="price"
>
--
</div>

<div class="source">
XAU/USD · Twelve Data
</div>

<div class="conclusion">

<div class="conclusion-title">
AI MARKET CONCLUSION
</div>

<div
class="decision"
id="decision"
>
WAIT
</div>

<div class="confidence">
CONFIDENCE:
<strong id="confidence">
--
</strong>%
</div>

</div>

<div class="grid">

<div class="metric">

<div class="label">
AI Score
</div>

<div
class="value"
id="score"
>
--
</div>

</div>

<div class="metric">

<div class="label">
Entry Zone
</div>

<div
class="value"
id="entry"
>
--
</div>

</div>

<div class="metric">

<div class="label">
Invalidation
</div>

<div
class="value"
id="invalidation"
>
--
</div>

</div>

<div class="metric">

<div class="label">
Target Liquidity
</div>

<div
class="value"
id="target"
>
--
</div>

</div>

</div>

<div class="section-title">
WHY?
</div>

<div
class="reason"
id="why"
>
Waiting for market data...
</div>

<div class="section-title">
Market Analysis
</div>

<div class="analysis-grid">

<div class="analysis">

<div class="analysis-name">
TREND
</div>

<div
class="analysis-value"
id="trend"
>
--
</div>

</div>

<div class="analysis">

<div class="analysis-name">
MOMENTUM
</div>

<div
class="analysis-value"
id="momentum"
>
--
</div>

</div>

<div class="analysis">

<div class="analysis-name">
STRUCTURE
</div>

<div
class="analysis-value"
id="structure"
>
--
</div>

</div>

<div class="analysis">

<div class="analysis-name">
RSI
</div>

<div
class="analysis-value"
id="rsi"
>
--
</div>

</div>

<div class="analysis">

<div class="analysis-name">
EMA 20
</div>

<div
class="analysis-value"
id="ema20"
>
--
</div>

</div>

<div class="analysis">

<div class="analysis-name">
EMA 50
</div>

<div
class="analysis-value"
id="ema50"
>
--
</div>

</div>

</div>

<div class="section-title">
Timeframe Alignment
</div>

<div class="timeframes">

<div class="timeframe">

<div class="tf-name">
5 MIN
</div>

<div
class="tf-value"
id="trend5"
>
--
</div>

</div>

<div class="timeframe">

<div class="tf-name">
15 MIN
</div>

<div
class="tf-value"
id="trend15"
>
--
</div>

</div>

<div class="timeframe">

<div class="tf-name">
1 HOUR
</div>

<div
class="tf-value"
id="trend1h"
>
--
</div>

</div>

<div class="timeframe">

<div class="tf-name">
1 DAY
</div>

<div
class="tf-value"
id="trend1d"
>
--
</div>

</div>

</div>

<div class="section-title">
Liquidity Map
</div>

<div class="liquidity-grid">

<div class="liquidity">

<div class="liquidity-name">
BUY-SIDE
</div>

<div
class="liquidity-value"
id="buyLiquidity"
>
Waiting...
</div>

</div>

<div class="liquidity">

<div class="liquidity-name">
SELL-SIDE
</div>

<div
class="liquidity-value"
id="sellLiquidity"
>
Waiting...
</div>

</div>

</div>

<div class="section-title">
System
</div>

<div class="source">
Last update:
<span id="updated">
--
</span>
</div>

</div>

<div class="footer">
Trading-AI analysis is informational.
Market outcomes are not guaranteed.
</div>

</div>

<script>

async function updateState() {

    try {

        const response =
            await fetch(
                "/api/state?t="
                + Date.now(),
                {
                    cache:
                        "no-store"
                }
            );

        if (!response.ok) {
            throw new Error(
                "API request failed"
            );
        }

        const data =
            await response.json();

        const gold =
            data.gold;

        if (!gold) {
            return;
        }

        document.getElementById(
            "status"
        ).innerText =
            gold.status
            || "Connecting...";

        document.getElementById(
            "price"
        ).innerText =
            gold.price !== null
            && gold.price !== undefined
                ? Number(
                    gold.price
                  ).toFixed(4)
                : "--";

        document.getElementById(
            "decision"
        ).innerText =
            gold.decision
            || "WAIT";

        document.getElementById(
            "confidence"
        ).innerText =
            gold.confidence
            ?? "--";

        document.getElementById(
            "score"
        ).innerText =
            gold.score
            ?? "--";

        document.getElementById(
            "entry"
        ).innerText =
            gold.entry
            ?? "--";

        document.getElementById(
            "invalidation"
        ).innerText =
            gold.invalidation
            ?? "--";

        document.getElementById(
            "target"
        ).innerText =
            gold.target
            ?? "--";

        document.getElementById(
            "why"
        ).innerText =
            gold.why
            || "Waiting...";

        document.getElementById(
            "trend"
        ).innerText =
            gold.trend
            || "--";

        document.getElementById(
            "momentum"
        ).innerText =
            gold.momentum
            || "--";

        document.getElementById(
            "structure"
        ).innerText =
            gold.structure
            || "--";

        document.getElementById(
            "rsi"
        ).innerText =
            gold.rsi !== null
            && gold.rsi !== undefined
                ? Number(
                    gold.rsi
                  ).toFixed(2)
                : "--";

        document.getElementById(
            "ema20"
        ).innerText =
            gold.ema20 !== null
            && gold.ema20 !== undefined
                ? Number(
                    gold.ema20
                  ).toFixed(4)
                : "--";

        document.getElementById(
            "ema50"
        ).innerText =
            gold.ema50 !== null
            && gold.ema50 !== undefined
                ? Number(
                    gold.ema50
                  ).toFixed(4)
                : "--";

        document.getElementById(
            "trend5"
        ).innerText =
            gold.trend_5m
            || "--";

        document.getElementById(
            "trend15"
        ).innerText =
            gold.trend_15m
            || "--";

        document.getElementById(
            "trend1h"
        ).innerText =
            gold.trend_1h
            || "--";

        document.getElementById(
            "trend1d"
        ).innerText =
            gold.trend_1d
            || "--";

        document.getElementById(
            "buyLiquidity"
        ).innerText =
            gold.buy_liquidity !== null
            && gold.buy_liquidity !== undefined
                ? Number(
                    gold.buy_liquidity
                  ).toFixed(4)
                : "Waiting...";

        document.getElementById(
            "sellLiquidity"
        ).innerText =
            gold.sell_liquidity !== null
            && gold.sell_liquidity !== undefined
                ? Number(
                    gold.sell_liquidity
                  ).toFixed(4)
                : "Waiting...";

        document.getElementById(
            "updated"
        ).innerText =
            gold.updated
            || "--";

    }

    catch (error) {

        document.getElementById(
            "status"
        ).innerText =
            "Connection retrying...";

        console.log(
            "STATE ERROR:",
            error
        );
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
# ROUTES
# =========================================================

@app.route("/")
def home():
    return render_template_string(
        HTML
    )


@app.route("/api/state")
def api_state():

    with lock:

        data = {
            "gold": dict(
                latest["gold"]
            )
        }

    return jsonify(data)


@app.route("/health")
def health():

    with lock:

        return jsonify({
            "status": "ok",
            "gold_status":
                latest["gold"]["status"],
            "gold_price":
                latest["gold"]["price"]
        })


# =========================================================
# START ENGINES
# =========================================================

def start_engines():

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


start_engines()


# =========================================================
# LOCAL SERVER
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
