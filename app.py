from flask import Flask, Response, jsonify, render_template_string
import os
import json
import time
import threading
import queue
import requests
import websocket
from datetime import datetime, timezone

app = Flask(__name__)

# =========================================================
# CONFIG
# =========================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# Your current Twelve Data Basic plan does not provide WTI/USD.
# Keep this False so Oil cannot break the Gold connection.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

CANDLE_INTERVALS = [
    "1min",
    "5min",
    "15min",
    "30min",
    "1h",
    "4h",
    "1day",
    "1week",
    "1month"
]

# =========================================================
# GLOBAL STATE
# =========================================================

lock = threading.RLock()

workers_started = False

clients = []

clients_lock = threading.Lock()

state = {
    "gold": {
        "symbol": GOLD_SYMBOL,
        "name": "Gold — XAU/USD",

        "price": None,

        "trend": "WAITING",
        "momentum": "WAITING",
        "structure": "WAITING",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "support": None,
        "resistance": None,

        "liquidity_high": None,
        "liquidity_low": None,

        "sweep": "NONE",

        "signal": "WAIT",
        "confidence": 50,
        "score": 0,

        "connection": "CONNECTING",

        "updated": None,

        "timeframes": {
            "5min": {},
            "15min": {},
            "1h": {},
            "1day": {}
        },

        "candles": {},

        "signal_history": [],

        "trade_plan": {
            "signal": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
            "target_3": None,
            "risk_reward": None,
            "invalidation": None,
            "reason": []
        },

        "error": None
    },

    "oil": {
        "symbol": OIL_SYMBOL,
        "name": "Crude Oil — WTI",

        "price": None,

        "trend": "UNAVAILABLE",
        "momentum": "UNAVAILABLE",
        "structure": "UNAVAILABLE",

        "rsi": None,
        "ema20": None,
        "ema50": None,

        "support": None,
        "resistance": None,

        "liquidity_high": None,
        "liquidity_low": None,

        "sweep": "NONE",

        "signal": "WAIT",
        "confidence": 0,
        "score": 0,

        "connection": "PLAN LIMIT",

        "updated": None,

        "timeframes": {},

        "error": (
            "WTI/USD is not available on the current "
            "Twelve Data plan."
        )
    }
}


# =========================================================
# HELPERS
# =========================================================

def now_text():
    return time.strftime("%H:%M:%S")


def number(value):
    try:
        return float(value)
    except Exception:
        return None


def broadcast():
    """
    Send the latest state to every connected browser.
    """

    with lock:
        payload = json.dumps(
            state,
            separators=(",", ":")
        )

    dead = []

    with clients_lock:

        for q in clients:

            try:

                q.put_nowait(payload)

            except Exception:

                dead.append(q)

        for q in dead:

            if q in clients:
                clients.remove(q)


def update_gold_price(price):

    price = number(price)

    if price is None:
        return

    with lock:

        state["gold"]["price"] = price

        state["gold"]["updated"] = now_text()

        state["gold"]["connection"] = "CONNECTED"

        state["gold"]["error"] = None

    broadcast()

    print(
        f"STATE UPDATED: GOLD = {price}"
    )


# =========================================================
# TECHNICAL INDICATORS
# =========================================================

def ema(values, period):

    if not values or len(values) < period:
        return None

    multiplier = 2.0 / (period + 1.0)

    result = (
        sum(values[:period])
        / period
    )

    for value in values[period:]:

        result = (
            (value - result)
            * multiplier
        ) + result

    return result


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i]
            - values[i - 1]
        )

        if change >= 0:

            gains.append(change)
            losses.append(0)

        else:

            gains.append(0)
            losses.append(abs(change))

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    result = 100.0 - (
        100.0 / (1.0 + rs)
    )

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

        if avg_loss == 0:
            result = 100.0

        else:

            rs = (
                avg_gain
                / avg_loss
            )

            result = 100.0 - (
                100.0
                / (1.0 + rs)
            )

    return result
