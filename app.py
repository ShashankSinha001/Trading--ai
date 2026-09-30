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

app = Flask(**name**)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

# ============================================================

# GLOBAL STATE

# ============================================================

state_lock = threading.Lock()

live_price = {
"gold": None,
"gold_timestamp": None,
}

connection_state = {
"gold_ws": "DISCONNECTED",
}

local_candles = defaultdict(list)

current_1m = None
last_tick_time = None

live_1m_records = []

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

signal_history = []

_started = False
_start_lock = threading.Lock()

last_recorded_decision = None
last_recorded_time = 0

# ============================================================

# BASIC HELPERS

# ============================================================

def safe_float(value, default=0.0):
try:
return float(value)
except Exception:
return default

def unix_minute(timestamp):
return int(timestamp // 60) * 60

def candle_from_dict(item):
try:
return Candle(
open=safe_float(item.get("open")),
high=safe_float(item.get("high")),
low=safe_float(item.get("low")),
close=safe_float(item.get("close")),
volume=safe_float(item.get("volume", 0)),
)
except Exception:
return None

# ============================================================

# CANDLE AGGREGATION

# ============================================================

def aggregate_candles(source, minutes):

```
if not source:
    return []

bucket_seconds = minutes * 60
buckets = {}

for item in source:

    timestamp = item.get("timestamp")

    if timestamp is None:
        continue

    candle = candle_from_dict(item)

    if candle is None:
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

        b["high"] = max(
            b["high"],
            candle.high
        )

        b["low"] = min(
            b["low"],
            candle.low
        )

        b["close"] = candle.close

        b["volume"] += candle.volume

return [
    buckets[x]
    for x in sorted(buckets.keys())
]
```

# ============================================================

# LIVE WEBSOCKET CANDLE BUILDER

# ============================================================

def update_live_candle(price, timestamp):

```
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

    if minute == current_1m["timestamp"]:

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

    finished = current_1m.copy()

    live_1m_records.append(finished)

    if len(live_1m_records) > 1500:
        del live_1m_records[:-1500]

    current_1m = {
        "timestamp": minute,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0.0,
    }
```

def build_local_timeframes():

```
with state_lock:

    source = list(live_1m_records)

    if current_1m is not None:
        source.append(current_1m.copy())

if not source:
    return

for timeframe, minutes in [
    ("1min", 1),
    ("5min", 5),
    ("15min", 15),
    ("30min", 30),
    ("1h", 60),
]:

    aggregated = aggregate_candles(
        source,
        minutes
    )

    candles = []

    for item in aggregated:

        candle = candle_from_dict(item)

        if candle:
            candles.append(candle)

    with state_lock:
        local_candles[timeframe] = candles[-300:]
```

# ============================================================

# TWELVE DATA HISTORICAL BOOTSTRAP

# ============================================================

def twelve_data_candles(interval, outputsize=100):

```
if not API_KEY:

    print(
        "TWELVE DATA API KEY MISSING"
    )

    return []

url = (
    "https://api.twelvedata.com/"
    "time_series"
)

params = {
    "symbol": GOLD_SYMBOL,
    "interval": interval,
    "outputsize": outputsize,
    "apikey": API_KEY,
    "format": "JSON",
}

try:

    response = requests.get(
        url,
        params=params,
        timeout=15,
    )

    if response.status_code == 429:

        print(
            f"TWELVE DATA {interval}: "
            f"429 RATE LIMIT"
        )

        return []

    if response.status_code != 200:

        print(
            f"TWELVE DATA {interval}: "
            f"HTTP {response.status_code}"
        )

        return []

    data = response.json()

    if data.get("status") == "error":

        print(
            f"TWELVE DATA {interval}: "
            f"{data.get('message')}"
        )

        return []

    values = data.get(
        "values",
        []
    )

    values = list(
        reversed(values)
    )

    candles = []

    for item in values:

        candle = candle_from_dict(item)

        if candle is None:
            continue

        t
```
