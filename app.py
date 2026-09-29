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

# ============================================================
# GLOBAL DATA
# ============================================================

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

gold_prices = deque(maxlen=300)

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
        result = ((price - result) * multiplier) + result

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

    if len(gains) < period:
        return None

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

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

    if first == 0:
        return "WAIT"

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

    recent_highs = highs[-30:]
    recent_lows = lows[-30:]

    return max(recent_highs), min(recent_lows)


# ============================================================
# AI ANALYSIS
# ============================================================

def calculate_analysis(price):
    if price is None:
        return

    with lock:
        c5 = list(candles["5min"])
        c15 = list(candles["15min"])
        c1h = list(candles["1h"])
        c1d = list(candles["1day"])
        history = list(gold_prices)

    if len(history) < 5:
        with lock:
            latest["gold"]["decision"] = "WAIT"
            latest["gold"]["confidence"] = 0
            latest["gold"]["score"] = 0
            latest["gold"]["why"] = (
                "Collecting live market data..."
            )

        return

    # --------------------------------------------------------
    # MULTI-TIMEFRAME TREND
    # --------------------------------------------------------

    trend5 = trend_from_candles(c5)
    trend15 = trend_from_candles(c15)
    trend1h = trend_from_candles(c1h)
    trend1d = trend_from_candles(c1d)

    # --------------------------------------------------------
    # PRICE SERIES
    # --------------------------------------------------------

    closes = []

    for candle in c5:
        close = safe_float(candle.get("close"))

        if close is not None:
            closes.append(close)

    closes.extend(history)

    closes = closes[-200:]

    ema20_value = ema(closes, 20)
    ema50_value = ema(closes, 50)
    rsi_value = calculate_rsi(closes)

    # --------------------------------------------------------
    # SCORE
    # --------------------------------------------------------

    score = 0
    reasons = []

    trends = {
        "1D": trend1d,
        "1H": trend1h,
        "15M": trend15,
        "5M": trend5
    }

    weights = {
        "1D": 3,
        "1H": 3,
        "15M": 2,
        "5M": 2
    }

    for timeframe, weight in weights.items():

        trend = trends[timeframe]

        if trend == "BULLISH":
            score += weight
            reasons.append(
                f"{timeframe} bullish"
            )

        elif trend == "BEARISH":
            score -= weight
            reasons.append(
                f"{timeframe} bearish"
            )

    # --------------------------------------------------------
    # EMA20
    # --------------------------------------------------------

    if ema20_value is not None:

        if price > ema20_value:
            score += 1
            reasons.append("Price above EMA20")

        else:
            score -= 1
            reasons.append("Price below EMA20")

    # --------------------------------------------------------
    # EMA50
    # --------------------------------------------------------

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

        if 55 < rsi_value < 75:
            score += 1
            reasons.append("RSI bullish")

        elif 25 < rsi_value < 45:
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

    entry = price
    target = None
    invalidation = None

    if (
        liquidity_high is not None
        and liquidity_low is not None
    ):

        if score > 0:

            target = liquidity_high
            invalidation = liquidity_low

        elif score < 0:

            target = liquidity_low
            invalidation = liquidity_high

        else:

            high_distance = abs(
                liquidity_high - price
            )

            low_distance = abs(
                price - liquidity_low
            )

            if high_distance <= low_distance:

                target = liquidity_high
                invalidation = liquidity_low

            else:

                target = liquidity_low
                invalidation = liquidity_high

    # --------------------------------------------------------
    # FINAL DECISION
    # --------------------------------------------------------

    decision = "WAIT"

    if score >= 6:
        decision = "BUY SIDE"

    elif score <= -6:
        decision = "SELL SIDE"

    # Higher timeframe conflict
    if (
        trend1d != "WAIT"
        and trend1h != "WAIT"
        and trend1d != trend1h
    ):
        decision = "WAIT"
        reasons.append(
            "1D / 1H higher-timeframe conflict"
        )

    confidence = min(
        95,
        50 + (abs(score) * 4)
    )

    if decision == "WAIT":
        confidence = min(
            confidence,
            60
        )

    if not reasons:
        reasons.append(
            "Insufficient confirmation"
        )

    why = " • ".join(
        reasons[-6:]
    )

    # --------------------------------------------------------
    # SAVE ANALYSIS
    # --------------------------------------------------------

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
# GOLD WEBSOCKET
# ============================================================

def gold_worker():

    print(
        "GOLD ENGINE STARTING",
        flush=True
    )

    if not API_KEY:

        print(
            "TWELVE_DATA_API_KEY MISSING",
            flush=True
        )

        with lock:
            latest["gold"]["status"] = (
                "API KEY MISSING"
            )

        return

    while True:

        try:

            print(
                "GOLD WS CONNECT ATTEMPT",
                flush=True
            )

            ws = websocket.create_connection(
                "wss://ws.twelvedata.com/v1/quotes/price",
                timeout=30
            )

            print(
                "TWELVE DATA GOLD CONNECTED",
                flush=True
            )

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                },
```
