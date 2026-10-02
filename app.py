from flask import Flask, Response, jsonify, render_template_string
import os
import json
import time
import threading
import queue
import requests
import websocket
from datetime import datetime, timezone

# ============================================================
# TRADING AI
# REAL-TIME MARKET INTELLIGENCE
# STABLE GOLD VERSION
#
# Architecture:
# 1. WebSocket = live price
# 2. REST = candle history
# 3. Cached candles survive API errors
# 4. REST price fallback if WebSocket fails
# 5. Browser polls state independently
# 6. One failed component cannot blank the dashboard
# ============================================================

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# Oil intentionally disabled.
# It must never be allowed to break Gold.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

REST_PRICE_URL = "https://api.twelvedata.com/price"
REST_TIME_SERIES_URL = "https://api.twelvedata.com/time_series"

# IMPORTANT:
# API key is supplied in the WebSocket URL.
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

CANDLE_INTERVALS = [
    "1min",
    "5min",
    "15min",
    "1h",
    "4h",
    "1day",
]

# Keep API usage low.
FETCH_INTERVALS = {
    "1min": 120,
    "5min": 180,
    "15min": 300,
    "1h": 600,
    "4h": 1200,
    "1day": 1800,
}

# ============================================================
# GLOBALS
# ============================================================

lock = threading.RLock()

workers_started = False
workers_start_lock = threading.Lock()

# ------------------------------------------------------------
# Candle cache
# ------------------------------------------------------------

candle_cache = {}
candle_cache_lock = threading.RLock()

# ------------------------------------------------------------
# Price cache
# ------------------------------------------------------------

price_cache = {
    "price": None,
    "timestamp": 0,
}

price_cache_lock = threading.RLock()

# ------------------------------------------------------------
# API backoff
# ------------------------------------------------------------

api_backoff_until = 0
api_backoff_lock = threading.Lock()

# ------------------------------------------------------------
# WebSocket state
# ------------------------------------------------------------

ws_state = {
    "connected": False,
    "last_message": 0,
    "last_error": None,
    "reconnect_count": 0,
}

ws_state_lock = threading.RLock()

# ============================================================
# APPLICATION STATE
# ============================================================

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
        "data_source": "Starting...",

        "updated": None,

        "timeframes": {},

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
            "reason": [],
        },

        "market_phase": {
            "phase": "WAITING",
            "duration": "Waiting for market data.",
        },

        "error": None,
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
            "reason": [],
        },

        "market_phase": {
            "phase": "UNAVAILABLE",
            "duration": "WTI disabled on current data plan.",
        },

        "error": (
            "WTI/USD is disabled because it is not enabled "
            "on the current Twelve Data plan."
        ),
    },
}


# ============================================================
# HELPERS
# ============================================================

def now_text():
    return time.strftime("%H:%M:%S")


def number(value):
    try:
        return float(value)
    except Exception:
        return None


def safe_round(value, digits=3):
    try:
        return round(float(value), digits)
    except Exception:
        return None


def api_is_backing_off():
    with api_backoff_lock:
        return time.time() < api_backoff_until


def set_api_backoff(seconds=120):
    global api_backoff_until

    with api_backoff_lock:
        api_backoff_until = max(
            api_backoff_until,
            time.time() + seconds,
        )


def clear_api_backoff():
    global api_backoff_until

    with api_backoff_lock:
        api_backoff_until = 0


def deep_copy_state():
    with lock:
        return json.loads(json.dumps(state))


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def ema(values, period):

    if not values or len(values) < period:
        return None

    multiplier = 2.0 / (period + 1.0)

    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            (value - result) * multiplier
        ) + result

    return result


def rsi(values, period=14):

    if not values or len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = values[i] - values[i - 1]

        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

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

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


# ============================================================
# PRICE CACHE
# ============================================================

def save_price_cache(price):

    with price_cache_lock:

        price_cache["price"] = price
        price_cache["timestamp"] = time.time()


def get_cached_price():

    with price_cache_lock:

        return price_cache.get("price")


# ============================================================
# LIVE PRICE REST FALLBACK
# ============================================================

def get_live_price():

    if not API_KEY:
        print("ERROR: TWELVE_DATA_API_KEY missing")
        return get_cached_price()

    # During backoff, NEVER hammer the API.
    if api_is_backing_off():
        return get_cached_price()

    now = time.time()

    # Do not request every second.
    with price_cache_lock:

        cached_price = price_cache.get("price")
        cached_time = price_cache.get("timestamp", 0)

        if (
            cached_price is not None
            and now - cached_time < 30
        ):
            return cached_price

    try:

        response = requests.get(
            REST_PRICE_URL,
            params={
                "symbol": GOLD_SYMBOL,
                "apikey": API_KEY,
            },
            timeout=12,
        )

        if response.status_code == 429:

            print("PRICE 429 - enabling API backoff")
            set_api_backoff(120)

            return get_cached_price()

        if response.status_code != 200:

            print(
                "PRICE HTTP ERROR:",
                response.status_code,
            )

            return get_cached_price()

        data = response.json()

        if data.get("status") == "error":

            print(
                "PRICE API ERROR:",
                data.get("message", ""),
            )

            return get_cached_price()

        price = number(
            data.get("price")
        )

        if price is None:
            return get_cached_price()

        save_price_cache(price)

        clear_api_backoff()

        return price

    except Exception as exc:

        print(
            "PRICE ERROR:",
            repr(exc),
        )

        return get_cached_price()


# ============================================================
# CANDLE CACHE
# ============================================================

def cache_key(symbol, interval):
    return f"{symbol}:{interval}"


def get_cached_candles(symbol, interval):

    key = cache_key(symbol, interval)

    with candle_cache_lock:

        item = candle_cache.get(key)

        if not item:
            return []

        return list(
            item.get("values", [])
        )


def save_cached_candles(
    symbol,
    interval,
    values,
):

    key = cache_key(symbol, interval)

    with candle_cache_lock:

        candle_cache[key] = {
            "timestamp": time.time(),
            "values": list(values),
        }


# ============================================================
# REST CANDLES
# ============================================================

def get_candles(
    symbol,
    interval,
    outputsize=100,
):

    if not API_KEY:
        return get_cached_candles(
            symbol,
            interval,
        )

    key = cache_key(
        symbol,
        interval,
    )

    now = time.time()

    # If global API backoff is active,
    # return OLD data immediately.
    if api_is_backing_off():

        return get_cached_candles(
            symbol,
            interval,
        )

    # Normal cache TTL.
    with candle_cache_lock:

        cached = candle_cache.get(key)

        if cached:

            timestamp = cached.get(
                "timestamp",
                0,
            )

            values = cached.get(
                "values",
                [],
            )

            refresh_after = FETCH_INTERVALS.get(
                interval,
                300,
            )

            if (
                values
                and
                now - timestamp < refresh_after
            ):

                return list(values)

    try:

        response = requests.get(
            REST_TIME_SERIES_URL,
            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": API_KEY,
            },
            timeout=15,
        )

        if response.status_code == 429:

            print(
                "CANDLE 429:",
                interval,
            )

            set_api_backoff(120)

            # CRITICAL:
            # Never erase old candles.
            return get_cached_candles(
                symbol,
                interval,
            )

        if response.status_code != 200:

            print(
                "CANDLE HTTP ERROR:",
                interval,
                response.status_code,
            )

            return get_cached_candles(
                symbol,
                interval,
            )

        data = response.json()

        if data.get("status") == "error":

            print(
                "CANDLE API ERROR:",
                interval,
                data.get("message", ""),
            )

            return get_cached_candles(
                symbol,
                interval,
            )

        values = data.get(
            "values",
            [],
        )

        if not values:

            return get_cached_candles(
                symbol,
                interval,
            )

        # Twelve Data normally sends newest first.
        # Convert to oldest -> newest.
        values = list(
            reversed(values)
        )

        # Remove malformed candles.
        clean = []

        for candle in values:

            if not isinstance(
                candle,
                dict,
            ):
                continue

            if (
                number(candle.get("open")) is None
                or number(candle.get("high")) is None
                or number(candle.get("low")) is None
                or number(candle.get("close")) is None
            ):
                continue

            clean.append(candle)

        if not clean:

            return get_cached_candles(
                symbol,
                interval,
            )

        save_cached_candles(
            symbol,
            interval,
            clean,
        )

        clear_api_backoff()

        return clean

    except Exception as exc:

        print(
            "CANDLE ERROR:",
            interval,
            repr(exc),
        )

        return get_cached_candles(
            symbol,
            interval,
        )


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def analyze_timeframe(candles):

    if not candles:
        return {}

    closes = []
    highs = []
    lows = []

    for candle in candles:

        close = number(
            candle.get("close")
        )

        high = number(
            candle.get("high")
        )

        low = number(
            candle.get("low")
        )

        if (
            close is None
            or high is None
            or low is None
        ):
            continue

        closes.append(close)
        highs.append(high)
        lows.append(low)

    if len(closes) < 20:
        return {
            "data_status": "INSUFFICIENT",
            "candle_count": len(closes),
        }

    current = closes[-1]

    ema20_value = ema(
        closes,
        20,
    )

    ema50_value = ema(
        closes,
        50,
    )

    current_rsi = rsi(
        closes,
        14,
    )

    recent_highs = highs[-20:]
    recent_lows = lows[-20:]

    support = (
        min(recent_lows)
        if recent_lows
        else None
    )

    resistance = (
        max(recent_highs)
        if recent_highs
        else None
    )

    # --------------------------------------------------------
    # Trend
    # --------------------------------------------------------

    if (
        ema20_value is not None
        and ema50_value is not None
    ):

        if current > ema20_value > ema50_value:
            trend = "BULLISH"

        elif current < ema20_value < ema50_value:
            trend = "BEARISH"

        else:
            trend = "NEUTRAL"

    else:
        trend = "NEUTRAL"

    # --------------------------------------------------------
    # Momentum
    # --------------------------------------------------------

    if current_rsi is None:
        momentum = "NEUTRAL"

    elif current_rsi >= 55:
        momentum = "BUYING"

    elif current_rsi <= 45:
        momentum = "SELLING"

    else:
        momentum = "NEUTRAL"

    # --------------------------------------------------------
    # Structure
    # --------------------------------------------------------

    recent = closes[-10:]

    if len(recent) >= 10:

        old_average = (
            sum(recent[:5]) / 5
        )

        new_average = (
            sum(recent[-5:]) / 5
        )

        if new_average > old_average:
            structure = "HIGHER"

        elif new_average < old_average:
            structure = "LOWER"

        else:
            structure = "RANGE"

    else:
        structure = "RANGE"

    # --------------------------------------------------------
    # Liquidity
    # --------------------------------------------------------

    liquidity_high = (
        max(highs[-10:])
        if len(highs) >= 10
        else resistance
    )

    liquidity_low = (
        min(lows[-10:])
        if len(lows) >= 10
        else support
    )

    # --------------------------------------------------------
    # Sweep
    # --------------------------------------------------------

    sweep = "NONE"

    if len(candles) >= 3:

        previous_high = max(
            highs[-3:-1]
        )

        previous_low = min(
            lows[-3:-1]
        )

        latest_high = highs[-1]
        latest_low = lows[-1]
        latest_close = closes[-1]

        if (
            latest_high > previous_high
            and latest_close < previous_high
        ):

            sweep = "HIGH SWEEP"

        elif (
            latest_low < previous_low
            and latest_close > previous_low
        ):

            sweep = "LOW SWEEP"

    return {
        "data_status": "OK",
        "candle_count": len(closes),

        "price": safe_round(
            current,
            3,
        ),

        "trend": trend,

        "momentum": momentum,

        "structure": structure,

        "rsi": (
            safe_round(
                current_rsi,
                2,
            )
            if current_rsi is not None
            else None
        ),

        "ema20": (
            safe_round(
                ema20_value,
                3,
            )
            if ema20_value is not None
            else None
        ),

        "ema50": (
            safe_round(
                ema50_value,
                3,
            )
            if ema50_value is not None
            else None
        ),

        "support": (
            safe_round(
                support,
                3,
            )
            if support is not None
            else None
        ),

        "resistance": (
            safe_round(
                resistance,
                3,
            )
            if resistance is not None
            else None
        ),

        "liquidity_high": (
            safe_round(
                liquidity_high,
                3,
            )
            if liquidity_high is not None
            else None
        ),

        "liquidity_low": (
            safe_round(
                liquidity_low,
                3,
            )
            if liquidity_low is not None
            else None
        ),

        "sweep": sweep,
    }


# ============================================================
# MULTI-TIMEFRAME AI
# ============================================================

def build_ai_analysis(
    timeframes,
    live_price,
):

    score = 0
    reasons = []

    five = timeframes.get(
        "5min",
        {},
    )

    fifteen = timeframes.get(
        "15min",
        {},
    )

    one_hour = timeframes.get(
        "1h",
        {},
    )

    four_hour = timeframes.get(
        "4h",
        {},
    )

    def add(
        condition,
        points,
        reason,
    ):

        nonlocal score

        if condition:
            score += points
            reasons.append(reason)

    # --------------------------------------------------------
    # 5m
    # --------------------------------------------------------

    add(
        five.get("trend") == "BULLISH",
        2,
        "5m trend bullish",
    )

    add(
        five.get("trend") == "BEARISH",
        -2,
        "5m trend bearish",
    )

    add(
        five.get("momentum") == "BUYING",
        1,
        "5m momentum buying",
    )

    add(
        five.get("momentum") == "SELLING",
        -1,
        "5m momentum selling",
    )

    # --------------------------------------------------------
    # 15m
    # --------------------------------------------------------

    add(
        fifteen.get("trend") == "BULLISH",
        2,
        "15m trend bullish",
    )

    add(
        fifteen.get("trend") == "BEARISH",
        -2,
        "15m trend bearish",
    )

    add(
        fifteen.get("structure") == "HIGHER",
        1,
        "15m structure higher",
    )

    add(
        fifteen.get("structure") == "LOWER",
        -1,
        "15m structure lower",
    )

    # --------------------------------------------------------
    # 1H
    # --------------------------------------------------------

    add(
        one_hour.get("trend") == "BULLISH",
        2,
        "1H trend bullish",
    )

    add(
        one_hour.get("trend") == "BEARISH",
        -2,
        "1H trend bearish",
    )

    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    add(
        four_hour.get("trend") == "BULLISH",
        1,
        "4H trend bullish",
    )

    add(
        four_hour.get("trend") == "BEARISH",
        -1,
        "4H trend bearish",
    )

    # --------------------------------------------------------
    # Liquidity sweep
    # --------------------------------------------------------

    sweep = five.get(
        "sweep",
        "NONE",
    )

    add(
        sweep == "LOW SWEEP",
        1,
        "5m low-liquidity sweep",
    )

    add(
        sweep == "HIGH SWEEP",
        -1,
        "5m high-liquidity sweep",
    )

    # --------------------------------------------------------
    # Pivot bias
    # --------------------------------------------------------

    pivot = None

    try:

        high = float(
            fifteen.get("resistance")
        )

        low = float(
            fifteen.get("support")
        )

        close = float(
            fifteen.get("price")
        )

        pivot = (
            high + low + close
        ) / 3.0

    except Exception:
        pivot = None

    if pivot is not None and live_price is not None:

        if live_price > pivot:

            score += 1

            reasons.append(
                "Price above 15m pivot"
            )

        elif live_price < pivot:

            score -= 1

            reasons.append(
                "Price below 15m pivot"
            )

    score = max(
        -10,
        min(10, score),
    )

    if score >= 6:
        decision = "BUY"

    elif score <= -6:
        decision = "SELL"

    else:
        decision = "WAIT"

    confidence = max(
        50,
        min(
            95,
            50 + abs(score) * 5,
        ),
    )

    return {
        "signal": decision,
        "confidence": confidence,
        "score": score,
        "reasons": reasons,
        "pivot": (
            safe_round(
                pivot,
                3,
            )
            if pivot is not None
            else None
        ),
    }


# ============================================================
# MARKET PHASE
# ============================================================

def build_market_phase(
    timeframes,
    ai,
):

    five = timeframes.get(
        "5min",
        {},
    )

    fifteen = timeframes.get(
        "15min",
        {},
    )

    one_hour = timeframes.get(
        "1h",
        {},
    )

    score = ai.get(
        "score",
        0,
    )

    if score >= 6:

        phase = "BULLISH EXPANSION"

        duration = (
            "Multi-timeframe buying pressure "
            "is currently aligned."
        )

    elif score <= -6:

        phase = "BEARISH EXPANSION"

        duration = (
            "Multi-timeframe selling pressure "
            "is currently aligned."
        )

    elif (
        five.get("sweep") == "LOW SWEEP"
        and score > 0
    ):

        phase = "LIQUIDITY RECLAIM"

        duration = (
            "Low-side liquidity was swept; "
            "confirmation is still required."
        )

    elif (
        five.get("sweep") == "HIGH SWEEP"
        and score < 0
    ):

        phase = "LIQUIDITY REJECTION"

        duration = (
            "High-side liquidity was swept; "
            "confirmation is still required."
        )

    elif (
        five.get("structure") == "HIGHER"
        and fifteen.get("structure") == "HIGHER"
    ):

        phase = "ACCUMULATION / BUILDUP"

        duration = (
            "Short and medium structure is "
            "gradually improving."
        )

    elif (
        five.get("structure") == "LOWER"
        and fifteen.get("structure") == "LOWER"
    ):

        phase = "DISTRIBUTION / BUILDUP"

        duration = (
            "Short and medium structure is "
            "gradually weakening."
        )

    else:

        phase = "RANGE / CONFLICT"

        duration = (
            "Timeframes are not sufficiently "
            "aligned for a strong directional signal."
        )

    return {
        "phase": phase,
        "duration": duration,
    }


# ============================================================
# TRADE PLAN
# ============================================================

def build_trade_plan(
    timeframes,
    ai,
    live_price,
):

    signal = ai.get(
        "signal",
        "WAIT",
    )

    reasons = list(
        ai.get(
            "reasons",
            [],
        )
    )

    five = timeframes.get(
        "5min",
        {},
    )

    fifteen = timeframes.get(
        "15min",
        {},
    )

    if live_price is None:

        return {
            "signal": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
            "target_3": None,
            "risk_reward": None,
            "invalidation": None,
            "reason": [
                "Waiting for live price",
            ],
        }

    # Do not generate a fake setup for WAIT.
    if signal == "WAIT":

        return {
            "signal": "WAIT",
            "entry": safe_round(
                live_price,
                3,
            ),
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
            "target_3": None,
            "risk_reward": None,
            "invalidation": None,
            "reason": (
                reasons[-4:]
                if reasons
                else [
                    "Waiting for multi-timeframe confirmation"
                ]
            ),
        }

    support = number(
        five.get("support")
    )

    resistance = number(
        five.get("resistance")
    )

    if signal == "BUY":

        entry = live_price

        reference = support

        if reference is None:
            reference = (
                number(
                    fifteen.get(
                        "support"
                    )
                )
            )

        if (
            reference is None
            or reference >= entry
        ):

            risk = entry * 0.003

            stop = entry - risk

        else:

            stop = reference

            risk = entry - stop

            if risk <= 0:
                risk = entry * 0.003
                stop = entry - risk

        target1 = entry + risk * 1.0
        target2 = entry + risk * 2.0
        target3 = entry + risk * 3.0

        rr = 3.0

        invalidation = stop

    else:

        entry = live_price

        reference = resistance

        if reference is None:
            reference = (
                number(
                    fifteen.get(
                        "resistance"
                    )
                )
            )

        if (
            reference is None
            or reference <= entry
        ):

            risk = entry * 0.003

            stop = entry + risk

        else:

            stop = reference

            risk = stop - entry

            if risk <= 0:
                risk = entry * 0.003
                stop = entry + risk

        target1 = entry - risk * 1.0
        target2 = entry - risk * 2.0
        target3 = entry - risk * 3.0

        rr = 3.0

        invalidation = stop

    return {
        "signal": signal,

        "entry": safe_round(
            entry,
            3,
        ),

        "stop_loss": safe_round(
            stop,
            3,
        ),

        "target_1": safe_round(
            target1,
            3,
        ),

        "target_2": safe_round(
            target2,
            3,
        ),

        "target_3": safe_round(
            target3,
            3,
        ),

        "risk_reward": f"1:{rr:.1f}",

        "invalidation": safe_round(
            invalidation,
            3,
        ),

        "reason": (
            reasons[-5:]
            if reasons
            else []
        ),
    }


# ============================================================
# SIGNAL HISTORY
# ============================================================

def update_signal_history(
    signal,
    score,
    price,
    reasons,
):

    with lock:

        history = state[
            "gold"
        ][
            "signal_history"
        ]

        previous = (
            history[-1]
            if history
            else None
        )

        # Record only meaningful changes.
        changed = (
            previous is None
            or previous.get("signal") != signal
            or previous.get("score") != score
        )

        if not changed:
            return

        history.append(
            {
                "time": now_text(),
                "price": safe_round(
                    price,
                    3,
                ),
                "score": score,
                "signal": signal,
                "reasons": list(
                    reasons[:5]
                ),
            }
        )

        # Keep memory bounded.
        if len(history) > 100:
            del history[:-100]


# ============================================================
# UPDATE ANALYSIS
# ============================================================

def update_analysis():

    # --------------------------------------------------------
    # Fetch live price.
    # WebSocket normally supplies it.
    # REST is fallback.
    # --------------------------------------------------------

    live_price = get_cached_price()

    if live_price is None:

        live_price = get_live_price()

    # --------------------------------------------------------
    # Fetch all required candles.
    # Each timeframe has its own cache.
    # --------------------------------------------------------

    timeframes = {}
    candle_output = {}

    for interval in CANDLE_INTERVALS:

        candles = get_candles(
            GOLD_SYMBOL,
            interval,
            outputsize=100,
        )

        candle_output[
            interval
        ] = candles

        analysis = analyze_timeframe(
            candles
        )

        if analysis:

            timeframes[
                interval
            ] = analysis

    # --------------------------------------------------------
    # If candle data exists, it can also provide price.
    # --------------------------------------------------------

    if live_price is None:

        for interval in [
            "1min",
            "5min",
            "15min",
            "1h",
        ]:

            tf = timeframes.get(
                interval,
                {},
            )

            candidate = number(
                tf.get("price")
            )

            if candidate is not None:

                live_price = candidate
                save_price_cache(
                    candidate
                )

                break

    # --------------------------------------------------------
    # Build AI.
    # --------------------------------------------------------

    ai = build_ai_analysis(
        timeframes,
        live_price,
    )

    phase = build_market_phase(
        timeframes,
        ai,
    )

    trade_plan = build_trade_plan(
        timeframes,
        ai,
        live_price,
    )

    # --------------------------------------------------------
    # Select primary 5m values.
    # --------------------------------------------------------

    primary = (
        timeframes.get(
            "5min",
            {}
        )
    )

    if not primary:

        primary = (
            timeframes.get(
                "15min",
                {}
            )
        )

    # --------------------------------------------------------
    # Update global state atomically.
    # --------------------------------------------------------

    with lock:

        gold = state[
            "gold"
        ]

        if live_price is not None:

            gold[
                "price"
            ] = live_price

            gold[
                "updated"
            ] = now_text()

        if primary:

            gold[
                "trend"
            ] = primary.get(
                "trend",
                gold["trend"],
            )

            gold[
                "momentum"
            ] = primary.get(
                "momentum",
                gold["momentum"],
            )

            gold[
                "structure"
            ] = primary.get(
                "structure",
                gold["structure"],
            )

            gold[
                "rsi"
            ] = primary.get(
                "rsi"
            )

            gold[
                "ema20"
            ] = primary.get(
                "ema20"
            )

            gold[
                "ema50"
            ] = primary.get(
                "ema50"
            )

            gold[
                "support"
            ] = primary.get(
                "support"
            )

            gold[
                "resistance"
            ] = primary.get(
                "resistance"
            )

            gold[
                "liquidity_high"
            ] = primary.get(
                "liquidity_high"
            )

            gold[
                "liquidity_low"
            ] = primary.get(
                "liquidity_low"
            )

            gold[
                "sweep"
            ] = primary.get(
                "sweep",
                "NONE",
            )

        gold[
            "timeframes"
        ] = timeframes

        gold[
            "candles"
        ] = candle_output

        gold[
            "signal"
        ] = ai[
            "signal"
        ]

        gold[
            "confidence"
        ] = ai[
            "confidence"
        ]

        gold[
            "score"
        ] = ai[
            "score"
        ]

        gold[
            "trade_plan"
        ] = trade_plan

        gold[
            "market_phase"
        ] = phase

        if live_price is not None:

            gold[
                "connection"
            ] = (
                "CONNECTED"
                if ws_state["connected"]
                else "REST FALLBACK"
            )

            gold[
                "data_source"
            ] = (
                "Twelve Data WebSocket"
                if ws_state["connected"]
                else "Twelve Data REST"
            )

            gold[
                "error"
            ] = None

        else:

            gold[
                "connection"
            ] = "WAITING"

            gold[
                "data_source"
            ] = "Twelve Data"

            gold[
                "error"
            ] = (
                "No live price available yet. "
                "Cached candle data is preserved."
            )

    update_signal_history(
        ai["signal"],
        ai["score"],
        live_price,
        ai["reasons"],
    )


# ============================================================
# WEBSOCKET
# ============================================================

def websocket_loop():

    global api_backoff_until

    if not API_KEY:

        print(
            "WEBSOCKET: API key missing"
        )

        return

    while True:

        ws = None

        try:

            # Current Twelve Data endpoint:
            # API key belongs in URL.
            url = (
                WS_URL
                + "?apikey="
                + API_KEY
            )

            print(
                "Connecting Twelve Data Gold WebSocket..."
            )

            ws = websocket.create_connection(
                url,
                timeout=20,
                enable_multithread=True,
            )

            with ws_state_lock:

                ws_state[
                    "connected"
                ] = True

                ws_state[
                    "last_error"
                ] = None

            print(
                "GOLD WS CONNECTED"
            )

            subscribe = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL,
                },
            }

            ws.send(
                json.dumps(
                    subscribe
                )
            )

            print(
                "GOLD WS SUBSCRIBED:",
                GOLD_SYMBOL,
            )

            last_heartbeat = time.time()

            while True:

                # Send heartbeat periodically.
                if (
                    time.time()
                    - last_heartbeat
                    >= 10
                ):

                    try:

                        ws.send(
                            json.dumps(
                                {
                                    "action":
                                    "heartbeat"
                                }
                            )
                        )

                    except Exception:
                        break

                    last_heartbeat = time.time()

                ws.settimeout(12)

                try:

                    raw = ws.recv()

                except websocket.WebSocketTimeoutException:

                    continue

                if not raw:
                    break

                with ws_state_lock:

                    ws_state[
                        "last_message"
                    ] = time.time()

                try:

                    message = json.loads(
                        raw
                    )

                except Exception:

                    continue

                # ------------------------------------------------
                # Twelve Data price event
                # ------------------------------------------------

                price = number(
                    message.get(
                        "price"
                    )
                )

                if price is None:

                    # Some responses may contain
                    # price under close.
                    price = number(
                        message.get(
                            "close"
                        )
                    )

                if price is not None:

                    save_price_cache(
                        price
                    )

                    with lock:

                        state[
                            "gold"
                        ][
                            "price"
                        ] = price

                        state[
                            "gold"
                        ][
                            "updated"
                        ] = now_text()

                        state[
                            "gold"
                        ][
                            "connection"
                        ] = "CONNECTED"

                        state[
                            "gold"
                        ][
                            "data_source"
                        ] = (
                            "Twelve Data WebSocket"
                        )

                        state[
                            "gold"
                        ][
                            "error"
                        ] = None

                # Server error message.
                if (
                    message.get(
                        "status"
                    ) == "error"
                ):

                    error_message = (
                        message.get(
                            "message",
                            "WebSocket error",
                        )
                    )

                    print(
                        "GOLD WS SERVER ERROR:",
                        error_message,
                    )

                    with lock:

                        state[
                            "gold"
                        ][
                            "error"
                        ] = str(
                            error_message
                        )

            print(
                "GOLD WS DISCONNECTED"
            )

        except Exception as exc:

            error_text = repr(exc)

            print(
                "GOLD WS ERROR:",
                error_text,
            )

            with ws_state_lock:

                ws_state[
                    "last_error"
                ] = error_text

                ws_state[
                    "reconnect_count"
                ] += 1

        finally:

            with ws_state_lock:

                ws_state[
                    "connected"
                ] = False

            try:

                if ws is not None:
                    ws.close()

            except Exception:
                pass

        # --------------------------------------------------------
        # IMPORTANT:
        # WebSocket failure must NOT stop the application.
        # REST fallback continues separately.
        # --------------------------------------------------------

        with lock:

            if state[
                "gold"
            ][
                "price"
            ] is not None:

                state[
                    "gold"
                ][
                    "connection"
                ] = "REST FALLBACK"

        time.sleep(5)


# ============================================================
# PRICE FALLBACK WORKER
# ============================================================

def price_fallback_worker():

    while True:

        try:

            # If WebSocket is not connected,
            # REST keeps the price alive.
            with ws_state_lock:

                ws_connected = (
                    ws_state[
                        "connected"
                    ]
                )

            if not ws_connected:

                price = get_live_price()

                if price is not None:

                    with lock:

                        state[
                            "gold"
                        ][
                            "price"
                        ] = price

                        state[
                            "gold"
                        ][
                            "updated"
                        ] = now_text()

                        state[
                            "gold"
                        ][
                            "connection"
                        ] = "REST FALLBACK"

                        state[
                            "gold"
                        ][
                            "data_source"
                        ] = (
                            "Twelve Data REST"
                        )

            time.sleep(30)

        except Exception as exc:

            print(
                "PRICE WORKER ERROR:",
                repr(exc),
            )

            time.sleep(30)


# ============================================================
# ANALYSIS WORKER
# ============================================================

def analysis_worker():

    # First run shortly after startup.
    time.sleep(2)

    while True:

        try:

            update_analysis()

        except Exception as exc:

            print(
                "ANALYSIS WORKER ERROR:",
                repr(exc),
            )

            with lock:

                state[
                    "gold"
                ][
                    "error"
                ] = (
                    "Analysis worker recovered "
                    "from an internal error."
                )

        # Do not hammer Twelve Data.
        time.sleep(20)


# ============================================================
# START WORKERS ONCE
# ============================================================

def start_workers():

    global workers_started

    with workers_start_lock:

        if workers_started:
            return

        workers_started = True

        threads = [
            threading.Thread(
                target=websocket_loop,
                daemon=True,
                name="gold-websocket",
            ),

            threading.Thread(
                target=price_fallback_worker,
                daemon=True,
                name="price-fallback",
            ),

            threading.Thread(
                target=analysis_worker,
                daemon=True,
                name="analysis-worker",
            ),
        ]

        for thread in threads:
            thread.start()

        print(
            "TRADING AI WORKERS STARTED"
        )


# ============================================================
# API: STATE
# ============================================================

@app.route("/api/state")
def api_state():

    start_workers()

    return jsonify(
        deep_copy_state()
    )


# ============================================================
# API: HEALTH
# ============================================================

@app.route("/health")
def health():

    with ws_state_lock:

        ws = dict(
            ws_state
        )

    return jsonify(
        {
            "status": "ok",
            "api_key_present": bool(
                API_KEY
            ),
            "websocket": ws,
            "workers_started": (
                workers_started
            ),
            "price": get_cached_price(),
            "time": now_text(),
        }
    )


# ============================================================
# API: CANDLES
#
# Browser can request one timeframe directly.
# This is independent of the dashboard state.
# ============================================================

@app.route("/api/candles/<interval>")
def api_candles(interval):

    start_workers()

    if interval not in CANDLE_INTERVALS:

        return jsonify(
            {
                "error": "Invalid interval",
                "allowed": CANDLE_INTERVALS,
            }
        ), 400

    candles = get_candles(
        GOLD_SYMBOL,
        interval,
        outputsize=100,
    )

    return jsonify(
        {
            "symbol": GOLD_SYMBOL,
            "interval": interval,
            "candles": candles,
            "count": len(candles),
        }
    )


# ============================================================
# API: FORCE REFRESH
#
# Does NOT delete cache.
# It simply allows the next worker cycle to refresh.
# ============================================================

@app.route("/api/refresh")
def api_refresh():

    start_workers()

    return jsonify(
        {
            "status": "refresh scheduled",
            "message": (
                "Existing cached data is preserved."
            ),
        }
    )


# ============================================================
# DASHBOARD
# ============================================================

HTML = r"""
<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width,initial-scale=1.0"
>

<title>
Trading AI — Real-Time Market Intelligence
</title>

<script
src="https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js">
</script>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    background:#020b18;
    color:#eaf4ff;
    font-family:
        Inter,
        Arial,
        Helvetica,
        sans-serif;
}

.page{
    max-width:1500px;
    margin:0 auto;
    padding:22px;
}

.header{
    margin-bottom:20px;
}

.header h1{
    margin:0;
    font-size:30px;
    letter-spacing:.3px;
}

.header p{
    margin:7px 0 0;
    color:#8ca8c7;
}

.card{
    background:
        linear-gradient(
            145deg,
            #071a32,
            #041224
        );
    border:1px solid #16395f;
    border-radius:18px;
    padding:20px;
    box-shadow:
        0 15px 50px
        rgba(0,0,0,.30);
}

.asset{
    font-size:21px;
    font-weight:800;
}

.price{
    font-size:48px;
    font-weight:900;
    margin-top:10px;
    letter-spacing:-1px;
}

.live{
    display:inline-block;
    margin-top:5px;
    color:#36e0a0;
    font-weight:800;
}

.connection{
    margin-top:8px;
    color:#86a8cd;
    font-size:13px;
}

.metrics{
    display:grid;
    grid-template-columns:
        repeat(4,minmax(0,1fr));
    gap:10px;
    margin-top:20px;
}

.metric{
    background:#06172c;
    border:1px solid #123557;
    border-radius:12px;
    padding:12px;
    min-height:72px;
}

.label{
    color:#7296bd;
    font-size:11px;
    font-weight:800;
    letter-spacing:.6px;
}

.value{
    margin-top:8px;
    font-size:17px;
    font-weight:800;
}

.analysis{
    margin-top:18px;
    padding:18px;
    border-radius:14px;
    background:#04172d;
    border:1px solid #17466f;
}

.analysis-label{
    color:#7296bd;
    font-size:11px;
    font-weight:900;
    letter-spacing:1px;
}

.signal{
    margin-top:5px;
    font-size:32px;
    font-weight:900;
}

.confidence{
    margin-top:4px;
    color:#9bb9d8;
}

.score{
    margin-top:9px;
    font-size:22px;
    font-weight:900;
}

.phase{
    margin-top:13px;
    color:#a9c4e1;
    line-height:1.5;
}

.toolbar{
    display:flex;
    flex-wrap:wrap;
    gap:8px;
    margin-top:18px;
}

.tf{
    border:1px solid #24517b;
    background:#071a30;
    color:#b9d4f1;
    padding:9px 13px;
    border-radius:9px;
    cursor:pointer;
    font-weight:800;
}

.tf:hover{
    background:#0b2846;
}

.tf.active{
    background:#123e67;
    color:white;
}

.chart{
    width:100%;
    height:390px;
    margin-top:14px;
    border:1px solid #123557;
    border-radius:12px;
    overflow:hidden;
}

.grid2{
    display:grid;
    grid-template-columns:
        minmax(0,1fr)
        minmax(0,1fr);
    gap:14px;
    margin-top:16px;
}

.panel{
    background:#06172b;
    border:1px solid #123557;
    border-radius:14px;
    padding:16px;
}

.panel h3{
    margin:0 0 14px;
    font-size:15px;
}

.tradegrid{
    display:grid;
    grid-template-columns:
        repeat(2,minmax(0,1fr));
    gap:9px;
}

.tradebox{
    padding:12px;
    background:#041327;
    border:1px solid #123557;
    border-radius:10px;
}

.tradebox b{
    display:block;
    margin-top:6px;
    font-size:16px;
}

.reason{
    color:#91afd0;
    line-height:1.55;
    font-size:13px;
}

.history{
    margin-top:10px;
}

.row{
    display:grid;
    grid-template-columns:
        75px
        85px
        55px
        60px
        1fr;
    gap:8px;
    padding:8px 0;
    border-bottom:1px solid #102e4b;
    align-items:center;
    font-size:12px;
}

.footer{
    margin-top:20px;
    text-align:center;
    color:#53728f;
    font-size:11px;
}

.error{
    margin-top:10px;
    padding:10px;
    border-radius:9px;
    background:#27151b;
    border:1px solid #64303d;
    color:#ff9caf;
    font-size:12px;
}

.status{
    position:fixed;
    right:16px;
    bottom:16px;
    background:#061a30;
    border:1px solid #17456d;
    border-radius:10px;
    padding:9px 12px;
    font-size:12px;
    color:#91b2d2;
    box-shadow:0 8px 30px rgba(0,0,0,.35);
}

@media(max-width:900px){

    .metrics{
        grid-template-columns:
            repeat(2,minmax(0,1fr));
    }

    .grid2{
        grid-template-columns:1fr;
    }

}

@media(max-width:600px){

    .page{
        padding:12px;
    }

    .header h1{
        font-size:24px;
    }

    .price{
        font-size:38px;
    }

    .metrics{
        grid-template-columns:1fr 1fr;
    }

    .chart{
        height:320px;
    }

    .row{
        grid-template-columns:
            55px
            70px
            45px
            50px
            1fr;
        font-size:10px;
    }

}

</style>

</head>

<body>

<div class="page">

    <div class="header">

        <h1>
            Trading AI
        </h1>

        <p>
            Real-Time Market Intelligence
            • Explainable Multi-Timeframe Analysis
        </p>

    </div>

    <div id="gold">
        Loading Gold...
    </div>

    <div class="footer">
        LIVE MARKET DATA • TRADING-AI
    </div>

</div>

<div class="status" id="status">
    Connecting...
</div>


<script>

let activeTF = "5min";

let chart = null;
let candleSeries = null;

let latestData = null;

let refreshTimer = null;
let chartRequestId = 0;


/* ==========================================================
   FORMAT
   ========================================================== */

function fmt(v,d=3){

    if(
        v === null ||
        v === undefined ||
        v === ""
    ){
        return "—";
    }

    const n = Number(v);

    if(
        !Number.isFinite(n)
    ){
        return "—";
    }

    return n.toFixed(d);
}


function tfLabel(x){

    return {
        "1min":"1m",
        "5min":"5m",
        "15min":"15m",
        "30min":"30m",
        "1h":"1H",
        "4h":"4H",
        "1day":"1D"
    }[x] || x;
}


function metric(a,b){

    return `
        <div class="metric">
            <div class="label">${a}</div>
            <div class="value">${b}</div>
        </div>
    `;
}


function box(a,b){

    return `
        <div class="tradebox">
            <span class="label">${a}</span>
            <b>${b}</b>
        </div>
    `;
}


/* ==========================================================
   CHART
   ========================================================== */

function ensureChart(){

    const el =
        document.getElementById(
            "price-chart"
        );

    if(!el){
        return false;
    }

    if(
        chart &&
        candleSeries
    ){
        return true;
    }

    chart =
        LightweightCharts.createChart(
            el,
            {
                layout:{
                    background:{
                        type:"solid",
                        color:"#06162d"
                    },
                    textColor:"#9fc5f5"
                },

                grid:{
                    vertLines:{
                        color:
                        "rgba(100,150,220,.08)"
                    },

                    horzLines:{
                        color:
                        "rgba(100,150,220,.08)"
                    }
                },

                rightPriceScale:{
                    borderColor:"#183b60"
                },

                timeScale:{
                    timeVisible:true,
                    secondsVisible:false,
                    borderColor:"#183b60"
                }
            }
        );

    candleSeries =
        chart.addCandlestickSeries({
            upColor:"#20c997",
            downColor:"#ff5c73",
            borderVisible:false,
            wickUpColor:"#20c997",
            wickDownColor:"#ff5c73"
        });

    if(
        typeof ResizeObserver
        !== "undefined"
    ){

        const ro =
            new ResizeObserver(
                ()=>{
                    if(
                        chart &&
                        el
                    ){
                        try{
                            chart.applyOptions({
                                width:
                                el.clientWidth
                            });
                        }catch(e){}
                    }
                }
            );

        ro.observe(el);
    }

    return true;
}


/* ==========================================================
   NORMALIZE CANDLES
   ========================================================== */

function normalizeCandles(c){

    if(
        !Array.isArray(c)
    ){
        return [];
    }

    const result = [];

    const seen = new Set();

    for(
        const x of c
    ){

        const raw =
            x.datetime ||
            x.date ||
            x.timestamp;

        let t = null;

        if(
            typeof raw === "number"
        ){

            t = Math.floor(
                raw > 10000000000
                    ? raw / 1000
                    : raw
            );

        }else{

            const parsed =
                new Date(raw).getTime();

            if(
                Number.isFinite(parsed)
            ){
                t =
                    Math.floor(
                        parsed / 1000
                    );
            }
        }

        const o = Number(x.open);
        const h = Number(x.high);
        const l = Number(x.low);
        const cl = Number(x.close);

        if(
            !Number.isFinite(t) ||
            !Number.isFinite(o) ||
            !Number.isFinite(h) ||
            !Number.isFinite(l) ||
            !Number.isFinite(cl)
        ){
            continue;
        }

        if(
            seen.has(t)
        ){
            continue;
        }

        seen.add(t);

        result.push({
            time:t,
            open:o,
            high:h,
            low:l,
            close:cl
        });
    }

    result.sort(
        (a,b)=>a.time-b.time
    );

    return result;
}


/* ==========================================================
   DRAW CHART FROM STATE
   ========================================================== */

function drawChartFromState(){

    if(
        !latestData ||
        !latestData.gold
    ){
        return;
    }

    const g =
        latestData.gold;

    const candles =
        g.candles &&
        g.candles[activeTF]
            ? g.candles[activeTF]
            : [];

    const d =
        normalizeCandles(
            candles
        );

    if(
        !ensureChart()
    ){
        return;
    }

    if(
        d.length
    ){

        try{

            candleSeries.setData(
                d
            );

            chart.timeScale()
                .fitContent();

        }catch(e){

            console.error(
                "Chart setData:",
                e
            );
        }
    }
}


/* ==========================================================
   DIRECT CHART FALLBACK
   ========================================================== */

async function fetchChartDirectly(){

    const requestId =
        ++chartRequestId;

    try{

        const response =
            await fetch(
                "/api/candles/"
                + encodeURIComponent(
                    activeTF
                )
                + "?t="
                + Date.now(),
                {
                    cache:"no-store"
                }
            );

        if(
            requestId !== chartRequestId
        ){
            return;
        }

        if(
            !response.ok
        ){
            return;
        }

        const data =
            await response.json();

        if(
            requestId !== chartRequestId
        ){
            return;
        }

        const d =
            normalizeCandles(
                data.candles
            );

        if(
            !ensureChart()
        ){
            return;
        }

        if(
            d.length
        ){

            candleSeries.setData(
                d
            );

            chart.timeScale()
                .fitContent();
        }

    }catch(e){

        console.log(
            "Direct chart fetch:",
            e
        );
    }
}


/* ==========================================================
   HISTORY
   ========================================================== */

function hist(h){

    if(
        !Array.isArray(h) ||
        !h.length
    ){

        return `
            <div class="reason">
                No signal changes recorded yet.
            </div>
        `;
    }

    return h
        .slice()
        .reverse()
        .map(
            x=>`

            <div class="row">

                <span>
                    ${x.time || "—"}
                </span>

                <span>
                    ${fmt(x.price)}
                </span>

                <b>
                    ${
                        x.score > 0
                        ? "+"
                        : ""
                    }${x.score ?? 0}
                </b>

                <span>
                    ${x.signal || "WAIT"}
                </span>

                <span class="reason">
                    ${
                        (x.reasons || [])
                        .slice(0,3)
                        .join(" • ")
                    }
                </span>

            </div>
            `
        )
        .join("");
}


/* ==========================================================
   RENDER
   ========================================================== */

function render(data){

    latestData = data;

    const g =
        data.gold || {};

    const p =
        g.trade_plan || {};

    const phase =
        g.market_phase || {};

    document.getElementById(
        "gold"
    ).innerHTML = `

        <div class="card">

            <div class="asset">
                🥇 Gold — XAU/USD
            </div>

            <div class="price">
                ${fmt(g.price)}
            </div>

            <div class="live">
                ● LIVE
            </div>

            <div class="connection">
                Connection:
                ${g.connection || "CONNECTING"}
                • Source:
                ${g.data_source || "Twelve Data"}
                • Updated:
                ${g.updated || "—"}
            </div>


            <div class="metrics">

                ${metric(
                    "TREND",
                    g.trend
                )}

                ${metric(
                    "MOMENTUM",
                    g.momentum
                )}

                ${metric(
                    "STRUCTURE",
                    g.structure
                )}

                ${metric(
                    "RSI",
                    fmt(g.rsi,2)
                )}

                ${metric(
                    "EMA 20",
                    fmt(g.ema20)
                )}

                ${metric(
                    "EMA 50",
                    fmt(g.ema50)
                )}

                ${metric(
                    "SUPPORT",
                    fmt(g.support)
                )}

                ${metric(
                    "RESISTANCE",
                    fmt(g.resistance)
                )}

                ${metric(
                    "LIQUIDITY HIGH",
                    fmt(g.liquidity_high)
                )}

                ${metric(
                    "LIQUIDITY LOW",
                    fmt(g.liquidity_low)
                )}

                ${metric(
                    "SWEEP",
                    g.sweep || "NONE"
                )}

                ${metric(
                    "TIMEFRAME",
                    tfLabel(activeTF)
                )}

            </div>


            <div class="analysis">

                <div class="analysis-label">
                    TRADING-AI CONCLUSION
                </div>

                <div class="signal">
                    ${g.signal || "WAIT"}
                </div>

                <div class="confidence">
                    Confidence:
                    ${g.confidence || 0}%
                </div>

                <div class="score">
                    AI SCORE:
                    ${g.score ?? 0}
                </div>

                <div class="phase">

                    <b>
                        ${
                            phase.phase ||
                            "MARKET PHASE"
                        }
                    </b>

                    <br>

                    ${
                        phase.duration ||
                        "Waiting for market data."
                    }

                </div>

            </div>


            <div class="toolbar">

                ${
                    [
                        "1min",
                        "5min",
                        "15min",
                        "1h",
                        "4h",
                        "1day"
                    ]
                    .map(
                        x=>`

                        <button
                            class="tf ${
                                activeTF === x
                                ? "active"
                                : ""
                            }"
                            onclick=
                            "selectTF('${x}')"
                        >
                            ${tfLabel(x)}
                        </button>

                        `
                    )
                    .join("")
                }

            </div>


            <div
                id="price-chart"
                class="chart"
            ></div>


            <div class="grid2">

                <div class="panel">

                    <h3>
                        🎯 AI TRADE SETUP
                    </h3>

                    <div class="tradegrid">

                        ${box(
                            "ENTRY",
                            fmt(p.entry)
                        )}

                        ${box(
                            "STOP LOSS",
                            fmt(p.stop_loss)
                        )}

                        ${box(
                            "TARGET 1",
                            fmt(p.target_1)
                        )}

                        ${box(
                            "TARGET 2",
                            fmt(p.target_2)
                        )}

                        ${box(
                            "TARGET 3",
                            fmt(p.target_3)
                        )}

                        ${box(
                            "R:R",
                            p.risk_reward || "—"
                        )}

                        ${box(
                            "INVALIDATION",
                            fmt(p.invalidation)
                        )}

                        ${box(
                            "SETUP",
                            p.signal || "WAIT"
                        )}

                    </div>

                    <div
                        class="reason"
                        style="margin-top:12px"
                    >

                        <b>Why:</b>

                        ${
                            (p.reason || [])
                            .join(" • ")
                            ||
                            "Waiting for confirmation."
                        }

                    </div>

                </div>


                <div class="panel">

                    <h3>
                        📊 SIGNAL HISTORY
                    </h3>

                    <div class="history">

                        ${hist(
                            g.signal_history
                        )}

                    </div>

                </div>

            </div>


            ${
                g.error
                ?
                `
                <div class="error">
                    ${g.error}
                </div>
                `
                :
                ""
            }

        </div>
    `;

    // Chart DOM has just been recreated.
    // Recreate chart safely.
    chart = null;
    candleSeries = null;

    drawChartFromState();

    // Also try direct candle endpoint.
    // If cache already supplied the chart, this is harmless.
    fetchChartDirectly();
}


/* ==========================================================
   SELECT TIMEFRAME
   ========================================================== */

function selectTF(tf){

    activeTF = tf;

    render(
        latestData || {
            gold:{
                price:null
            }
        }
    );
}


/* ==========================================================
   STATE POLLING
   ========================================================== */

async function loadState(){

    try{

        const response =
            await fetch(
                "/api/state?t="
                + Date.now(),
                {
                    cache:"no-store"
                }
            );

        if(
            !response.ok
        ){
            throw new Error(
                "HTTP "
                + response.status
            );
        }

        const data =
            await response.json();

        render(data);

        document.getElementById(
            "status"
        ).textContent =
            "Dashboard connected";

    }catch(e){

        console.error(
            "STATE ERROR:",
            e
        );

        document.getElementById(
            "status"
        ).textContent =
            "Reconnecting...";

    }
}


/* ==========================================================
   START
   ========================================================== */

loadState();

refreshTimer =
    setInterval(
        loadState,
        5000
    );

</script>

</body>

</html>
"""


# ============================================================
# INDEX
# ============================================================

@app.route("/")
def index():

    start_workers()

    return render_template_string(
        HTML
    )


# ============================================================
# STARTUP
# ============================================================

# Do NOT start workers at import time.
# This prevents duplicate worker sets with Gunicorn reloads/workers.
#
# Use:
# gunicorn --workers 1 --timeout 0 --bind 0.0.0.0:$PORT app:app


if __name__ == "__main__":

    start_workers()

    port = int(
        os.environ.get(
            "PORT",
            "5000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        debug=False,
    )
