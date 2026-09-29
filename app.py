```python
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

latest = {
    "gold": {
        "symbol": GOLD_SYMBOL,
        "price": None,
        "previous_price": None,
        "status": "CONNECTING",
        "source": "Twelve Data",
        "updated": None,

        "decision": "WAIT",
        "confidence": None,
        "score": None,
        "entry": None,
        "invalidation": None,
        "target": None,
        "why": "Waiting for market data...",

        "trend_5m": "WAIT",
        "trend_15m": "WAIT",
        "trend_1h": "WAIT",
        "trend_1d": "WAIT",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "candles_5m": 0,
        "candles_15m": 0,
        "candles_1h": 0,
        "candles_1d": 0
    }
}

# Recent live prices
gold_prices = deque(maxlen=300)

# Candle storage
candles = {
    "5min": [],
    "15min": [],
    "1h": [],
    "1day": []
}

lock = threading.Lock()


# ============================================================
# HELPERS
# ============================================================

def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for price in values[period:]:
        result = (price - result) * multiplier + result

    return result


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
    if not data or len(data) < 5:
        return "WAIT"

    closes = []

    for candle in data:
        close = safe_float(candle.get("close"))
        if close is not None:
            closes.append(close)

    if len(closes) < 5:
        return "WAIT"

    recent = closes[-5:]
    first = recent[0]
    last = recent[-1]

    change_pct = ((last - first) / first) * 100

    if change_pct > 0.08:
        return "BULLISH"

    if change_pct < -0.08:
        return "BEARISH"

    return "NEUTRAL"


def liquidity_levels(data):
    if not data:
        return None, None

    highs = []
    lows = []

    for candle in data:
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    if not highs or not lows:
        return None, None

    return max(highs[-30:]), min(lows[-30:])


# ============================================================
# AI ANALYSIS
# ============================================================

def calculate_analysis(price):
    with lock:
        c5 = list(candles["5min"])
        c15 = list(candles["15min"])
        c1h = list(candles["1h"])
        c1d = list(candles["1day"])

        history = list(gold_prices)

    if price is None:
        return

    if len(history) < 5:
        with lock:
            latest["gold"]["decision"] = "WAIT"
            latest["gold"]["confidence"] = 0
            latest["gold"]["score"] = 0
            latest["gold"]["why"] = "Collecting live market data..."
        return

    trend5 = trend_from_candles(c5)
    trend15 = trend_from_candles(c15)
    trend1h = trend_from_candles(c1h)
    trend1d = trend_from_candles(c1d)

    closes = []

    for candle in c5:
        close = safe_float(candle.get("close"))
        if close is not None:
            closes.append(close)

    closes.extend(history)

    # Keep unique-ish chronological values
    closes = closes[-150:]

    ema20_value = ema(closes, 20)
    ema50_value = ema(closes, 50)
    rsi_value = calculate_rsi(closes)

    score = 0
    reasons = []

    # --------------------------------------------------------
    # MULTI TIMEFRAME TREND
    # --------------------------------------------------------

    trend_weights = {
        "1d": 3,
        "1h": 3,
        "15m": 2,
        "5m": 2
    }

    trends = {
        "1d": trend1d,
        "1h": trend1h,
        "15m": trend15,
        "5m": trend5
    }

    for tf, weight in trend_weights.items():

        if trends[tf] == "BULLISH":
            score += weight
            reasons.append(f"{tf.upper()} bullish")

        elif trends[tf] == "BEARISH":
            score -= weight
            reasons.append(f"{tf.upper()} bearish")

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if ema20_value is not None:
        if price > ema20_value:
            score += 1
            reasons.append("Price above EMA20")
        else:
            score -= 1
            reasons.append("Price below EMA20")

    if ema50_value is not None:
        if price > ema50_value:
            score += 1
            reasons.append("Price above EMA50")
        else:
            score -= 1
            reasons.append("Price below EMA50")

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if rsi_value is not None:

        if rsi_value > 55 and rsi_value < 75:
            score += 1
            reasons.append("RSI bullish")

        elif rsi_value < 45 and rsi_value > 25:
            score -= 1
            reasons.append("RSI bearish")

        elif rsi_value >= 75:
            reasons.append("RSI overbought")

        elif rsi_value <= 25:
            reasons.append("RSI oversold")

    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    liquidity_high, liquidity_low = liquidity_levels(c5)

    target = None
    invalidation = None
    entry = None

    if liquidity_high is not None and liquidity_low is not None:

        distance_high = abs(liquidity_high - price)
        distance_low = abs(price - liquidity_low)

        if score > 0:
            target = liquidity_high
            invalidation = liquidity_low
            entry = price

        elif score < 0:
            target = liquidity_low
            invalidation = liquidity_high
            entry = price

        else:
            target = liquidity_high if distance_high < distance_low else liquidity_low
            invalidation = liquidity_low if target == liquidity_high else liquidity_high
            entry = price

    # --------------------------------------------------------
    # FINAL DECISION
    # --------------------------------------------------------

    decision = "WAIT"

    if score >= 6:
        decision = "BUY SIDE"

    elif score <= -6:
        decision = "SELL SIDE"

    # Strong higher timeframe conflict = WAIT
    if (
        trend1d != "WAIT"
        and trend1h != "WAIT"
        and trend1d != trend1h
    ):
        decision = "WAIT"
        reasons.append("Higher timeframe conflict")

    confidence = min(95, 50 + abs(score) * 4)

    if decision == "WAIT":
        confidence = min(confidence, 60)

    if not reasons:
        reasons.append("Insufficient confirmation")

    why = " • ".join(reasons[-6:])

    with lock:
        latest["gold"].update({
            "price": price,
            "decision": decision,
            "confidence": confidence,
            "score": score,
            "entry": entry,
            "invalidation": invalidation,
            "target": target,
            "why": why,

            "trend_5m": trend5,
            "trend_15m": trend15,
            "trend_1h": trend1h,
            "trend_1d": trend1d,

            "rsi": rsi_value,
            "ema20": ema20_value,
            "ema50": ema50_value
        })


# ============================================================
# TWELVE DATA GOLD WEBSOCKET
# ============================================================

def gold_worker():

    print("GOLD ENGINE STARTING", flush=True)

    if not API_KEY:
        print("TWELVE_DATA_API_KEY MISSING", flush=True)

        with lock:
            latest["gold"]["status"] = "API KEY MISSING"

        return

    while True:

        try:

            print("GOLD WS CONNECT ATTEMPT", flush=True)

            ws = websocket.create_connection(
                "wss://ws.twelvedata.com/v1/quotes/price",
                timeout=30
            )

            print("TWELVE DATA GOLD CONNECTED", flush=True)

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                },
                "apikey": API_KEY
            }

            ws.send(json.dumps(subscribe_message))

            print(
                f"GOLD SUBSCRIBE SENT: {GOLD_SYMBOL}",
                flush=True
            )

            with lock:
                latest["gold"]["status"] = "LIVE"

            while True:

                message = ws.recv()

                if not message:
                    continue

                try:
                    data = json.loads(message)
                except Exception:
                    continue

                event = data.get("event")

                if event == "subscribe-status":

                    print(
                        f"GOLD WS: {data}",
                        flush=True
                    )

                    continue

                price = safe_float(data.get("price"))

                if price is None:
                    continue

                print(
                    f"GOLD PRICE: {price}",
                    flush=True
                )

                with lock:

                    previous = latest["gold"]["price"]

                    latest["gold"]["previous_price"] = previous
                    latest["gold"]["price"] = price
                    latest["gold"]["status"] = "LIVE"
                    latest["gold"]["updated"] = time.time()

                    gold_prices.append(price)

                calculate_analysis(price)

        except Exception as e:

            print(
                f"GOLD WEBSOCKET ERROR: {e}",
                flush=True
            )

            with lock:
                latest["gold"]["status"] = "RECONNECTING"

            time.sleep(3)


# ============================================================
# CANDLE DATA
# ============================================================

def fetch_candles(interval):

    if not API_KEY:
        return []

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
            timeout=15
        )

        print(
            f"CANDLE REQUEST {interval}: HTTP {response.status_code}",
            flush=True
        )

        data = response.jso
```
