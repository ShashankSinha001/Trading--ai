```python
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

# Current Twelve Data plan does not provide the required WTI feed.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

# ------------------------------------------------------------
# Global state
# ------------------------------------------------------------

state_lock = threading.Lock()

live_price = {
    "gold": None,
    "gold_timestamp": None,
}

connection_state = {
    "gold_ws": "DISCONNECTED",
}

# Local candles.
# Each key contains candles generated from historical bootstrap
# and/or live WebSocket ticks.
local_candles = defaultdict(list)

# Current unfinished 1-minute candle.
current_1m = None

# Last tick received.
last_tick_time = None

# HTTP bootstrap state.
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

# AI state.
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

# Signal history.
signal_history = []

# Subscribers for SSE.
subscribers = []
subscribers_lock = threading.Lock()


# ============================================================
# Utility
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def unix_minute(timestamp):
    return int(timestamp // 60) * 60


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def broadcast():
    payload = build_market_payload()

    dead = []

    with subscribers_lock:
        for q in subscribers:
            try:
                q.append(payload)
            except Exception:
                dead.append(q)

        for q in dead:
            if q in subscribers:
                subscribers.remove(q)


# ============================================================
# Candle helpers
# ============================================================

def candle_from_dict(item):
    try:
        return Candle(
            open=safe_float(item["open"]),
            high=safe_float(item["high"]),
            low=safe_float(item["low"]),
            close=safe_float(item["close"]),
            volume=safe_float(item.get("volume", 0)),
        )
    except Exception:
        return None


def candle_to_dict(candle, timestamp=None):
    result = {
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
    }

    if timestamp is not None:
        result["timestamp"] = timestamp

    return result


def normalize_candles(items):
    result = []

    for item in items:
        candle = candle_from_dict(item)

        if candle:
            result.append(candle)

    return result


def aggregate_candles(source, minutes):
    """
    Aggregate candles into a larger timeframe.

    Example:
        5min -> 15min
        5min -> 30min
        5min -> 1h
    """

    if not source:
        return []

    bucket_seconds = minutes * 60

    buckets = {}

    for item in source:
        if isinstance(item, dict):
            timestamp = item.get("timestamp")
            candle = candle_from_dict(item)
        else:
            timestamp = getattr(item, "timestamp", None)
            candle = item

        if candle is None or timestamp is None:
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

            b["high"] = max(b["high"], candle.high)
            b["low"] = min(b["low"], candle.low)
            b["close"] = candle.close
            b["volume"] += candle.volume

    output = []

    for timestamp in sorted(buckets.keys()):
        b = buckets[timestamp]

        output.append(
            {
                "timestamp": timestamp,
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b["volume"],
            }
        )

    return output


# ============================================================
# Live 1-minute candle builder
# ============================================================

def update_local_1m(price, timestamp):
    """
    Convert WebSocket ticks into local 1-minute candles.
    """

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

        current_minute = current_1m["timestamp"]

        # Same minute.
        if minute == current_minute:
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

        # New minute.
        finished = current_1m.copy()

        local_candles["1min"].append(
            candle_from_dict(finished)
        )

        # Keep memory under control.
        local_candles["1min"] = local_candles["1min"][-1000:]

        current_1m = {
            "timestamp": minute,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
        }


# ============================================================
# Local timeframe builder
# ============================================================

def rebuild_local_timeframes():

    with state_lock:

        one_min = []

        for candle in local_candles["1min"]:
            if candle:
                one_min.append(
                    {
                        "timestamp": getattr(
                            candle,
                            "timestamp",
                            None
                        ),
                        "open": candle.open,
                        "high": candle.high,
                        "low": candle.low,
                        "close": candle.close,
                        "volume": candle.volume,
                    }
                )

        # In live mode timestamps are stored separately below.
        # If 1m data does not have timestamps, don't aggregate it.
        one_min = [
            x for x in one_min
            if x
```
