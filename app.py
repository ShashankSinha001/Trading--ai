from flask import Flask, Response, jsonify, render_template_string
import os
import json
import time
import threading
import queue
import requests
import websocket

app = Flask(__name__)

# ============================================================
# TRADING AI
# REAL-TIME MARKET INTELLIGENCE
# GOLD XAU/USD
# STABLE / CACHE-SAFE VERSION
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# WTI intentionally disabled for the current Twelve Data plan.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

REST_PRICE_URL = "https://api.twelvedata.com/price"
REST_TIME_SERIES_URL = "https://api.twelvedata.com/time_series"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"


# ============================================================
# TIMEFRAMES
# ============================================================

CANDLE_INTERVALS = [
    "1min",
    "5min",
    "15min",
    "1h",
    "4h",
    "1day"
]

# These are minimum cache lifetimes.
#
# Important:
# We intentionally do NOT continuously hammer Twelve Data.
#
FETCH_INTERVALS = {
    "1min": 600,       # 10 min
    "5min": 180,       # 3 min
    "15min": 300,      # 5 min
    "1h": 900,         # 15 min
    "4h": 1800,        # 30 min
    "1day": 21600      # 6 hours
}


# ============================================================
# ANALYSIS PRIORITY
# ============================================================

# These timeframes are required by the AI engine.
CORE_ANALYSIS_INTERVALS = [
    "5min",
    "15min",
    "1h",
    "4h"
]

# Chart-only timeframes.
CHART_ONLY_INTERVALS = [
    "1min",
    "1day"
]


# ============================================================
# CACHE
# ============================================================

candle_cache = {}
candle_cache_lock = threading.RLock()

price_cache = {
    "price": None,
    "timestamp": 0
}

price_cache_lock = threading.RLock()


# ============================================================
# PER-RESOURCE BACKOFF
# ============================================================
#
# IMPORTANT:
# Do NOT use one global API backoff for every endpoint.
#
# If 5m receives a 429, 15m/1h/4h should not automatically
# become unavailable.
#

price_backoff_until = 0

candle_backoff_until = {
    interval: 0
    for interval in CANDLE_INTERVALS
}

price_backoff_lock = threading.Lock()
candle_backoff_lock = threading.Lock()


# ============================================================
# REQUEST LOCKS
# ============================================================
#
# Prevent duplicate requests for the same timeframe.
#

candle_request_locks = {
    interval: threading.Lock()
    for interval in CANDLE_INTERVALS
}

price_request_lock = threading.Lock()


# ============================================================
# GLOBAL STATE
# ============================================================

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

        "timeframes": {},
        "candles": {},

        "signal_history": [],

        "market_phase": {
            "phase": "MARKET PHASE",
            "duration": "Waiting for market data."
        },

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

        "error": None,

        "data_source": "Twelve Data WebSocket + REST candles"
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
            "WTI/USD is disabled because it is not enabled "
            "on the current Twelve Data plan."
        )
    }
}


# ============================================================
# HELPERS
# ============================================================

def now_text():
    return time.strftime("%H:%M:%S")


def number(value):
    try:
        if value is None:
            return None

        return float(value)

    except Exception:
        return None


def safe_json(data):
    return json.loads(
        json.dumps(
            data,
            default=str
        )
    )


def broadcast():

    with lock:

        payload = json.dumps(
            state,
            separators=(",", ":"),
            default=str
        )

    dead = []

    with clients_lock:

        for q in clients:

            try:

                q.put_nowait(payload)

            except Exception:

                dead.append(q)

        for q in dead:

            try:

                clients.remove(q)

            except ValueError:

                pass


def set_gold_price(
    price,
    source="Twelve Data WebSocket"
):

    price = number(price)

    if price is None:
        return

    with lock:

        state["gold"]["price"] = price

        state["gold"]["updated"] = now_text()

        state["gold"]["connection"] = "CONNECTED"

        state["gold"]["data_source"] = source

        # Do not erase existing analytical state here.
        #
        # This is important because live price can continue
        # arriving even when historical candle API is temporarily
        # unavailable.

    broadcast()


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def ema(values, period):

    if not values or len(values) < period:
        return None

    multiplier = 2.0 / (period + 1.0)

    result = sum(
        values[:period]
    ) / period

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

        change = (
            values[i] -
            values[i - 1]
        )

        if change >= 0:

            gains.append(change)
            losses.append(0)

        else:

            gains.append(0)
            losses.append(abs(change))

    avg_gain = (
        sum(gains[:period]) /
        period
    )

    avg_loss = (
        sum(losses[:period]) /
        period
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain * (period - 1)
            )
            +
            gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss * (period - 1)
            )
            +
            losses[i]
        ) / period

    if avg_loss == 0:

        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (
        100.0 /
        (1.0 + rs)
    )


# ============================================================
# CANDLE NORMALIZATION
# ============================================================

def normalize_candles(values):

    if not isinstance(values, list):
        return []

    clean = []

    for candle in values:

        if not isinstance(candle, dict):
            continue

        dt = (
            candle.get("datetime")
            or candle.get("date")
            or candle.get("time")
        )

        open_value = number(
            candle.get("open")
        )

        high_value = number(
            candle.get("high")
        )

        low_value = number(
            candle.get("low")
        )

        close_value = number(
            candle.get("close")
        )

        if dt is None:
            continue

        if any(
            x is None
            for x in (
                open_value,
                high_value,
                low_value,
                close_value
            )
        ):
            continue

        clean.append({

            "datetime": str(dt),

            "open": open_value,

            "high": high_value,

            "low": low_value,

            "close": close_value

        })

    # Twelve Data normally returns newest first.
    # Convert to oldest -> newest.
    clean.reverse()

    # Remove duplicate timestamps.
    unique = []

    seen = set()

    for candle in clean:

        key = candle["datetime"]

        if key in seen:
            continue

        seen.add(key)

        unique.append(candle)

    return unique


# ============================================================
# CACHE ACCESS
# ============================================================

def get_cached_candles(
    symbol,
    interval
):

    cache_key = f"{symbol}:{interval}"

    with candle_cache_lock:

        cached = candle_cache.get(
            cache_key
        )

        if not cached:
            return []

        values = cached.get(
            "values",
            []
        )

        if not values:
            return []

        return list(values)


def cache_candles(
    symbol,
    interval,
    values
):

    if not values:
        return

    cache_key = f"{symbol}:{interval}"

    with candle_cache_lock:

        candle_cache[cache_key] = {

            "timestamp": time.time(),

            "values": list(values)

        }


def candle_cache_age(
    symbol,
    interval
):

    cache_key = f"{symbol}:{interval}"

    with candle_cache_lock:

        cached = candle_cache.get(
            cache_key
        )

        if not cached:
            return None

        timestamp = cached.get(
            "timestamp",
            0
        )

    if timestamp <= 0:
        return None

    return max(
        0,
        time.time() - timestamp
    )


# ============================================================
# PRICE BACKOFF
# ============================================================

def price_backoff_active():

    with price_backoff_lock:

        return (
            time.time()
            <
            price_backoff_until
        )


def set_price_backoff(seconds=180):

    global price_backoff_until

    with price_backoff_lock:

        price_backoff_until = (
            time.time() +
            seconds
        )


# ============================================================
# CANDLE BACKOFF
# ============================================================

def candle_backoff_active(
    interval
):

    with candle_backoff_lock:

        return (
            time.time()
            <
            candle_backoff_until.get(
                interval,
                0
            )
        )


def set_candle_backoff(
    interval,
    seconds=180
):

    with candle_backoff_lock:

        candle_backoff_until[
            interval
        ] = (
            time.time() +
            seconds
        )


# ============================================================
# LIVE PRICE
# ============================================================

def get_live_price():

    global price_backoff_until

    if not API_KEY:

        print(
            "ERROR: TWELVE_DATA_API_KEY missing"
        )

        return None

    # --------------------------------------------------------
    # If WebSocket is already providing a fresh price,
    # do NOT waste a REST API credit.
    # --------------------------------------------------------

    with lock:

        ws_price = state["gold"].get(
            "price"
        )

        ws_connection = state["gold"].get(
            "connection"
        )

        ws_updated = state["gold"].get(
            "updated"
        )

    if (
        ws_price is not None
        and ws_connection == "CONNECTED"
        and ws_updated
    ):

        try:

            current_seconds = (
                time.time()
            )

            parsed = time.strptime(
                ws_updated,
                "%H:%M:%S"
            )

            now_struct = time.localtime(
                current_seconds
            )

            updated_seconds = (
                time.mktime(
                    (
                        now_struct.tm_year,
                        now_struct.tm_mon,
                        now_struct.tm_mday,
                        parsed.tm_hour,
                        parsed.tm_min,
                        parsed.tm_sec,
                        0,
                        0,
                        -1
                    )
                )
            )

            age = (
                current_seconds -
                updated_seconds
            )

            # Small protection around midnight.
            if age < 0:
                age = 0

            if age < 90:

                return ws_price

        except Exception:

            # If timestamp parsing fails,
            # continue to cache / REST fallback.
            pass

    if price_backoff_active():

        return None

    now = time.time()

    with price_cache_lock:

        cached_price = price_cache.get(
            "price"
        )

        cached_time = price_cache.get(
            "timestamp",
            0
        )

        if (
            cached_price is not None
            and
            now - cached_time < 30
        ):

            return cached_price

    # Prevent duplicate price requests.
    if not price_request_lock.acquire(
        blocking=False
    ):

        return None

    try:

        response = requests.get(
            REST_PRICE_URL,
            params={
                "symbol": GOLD_SYMBOL,
                "apikey": API_KEY
            },
            timeout=15
        )

        if response.status_code == 429:

            print(
                "PRICE 429 - Twelve Data rate limit"
            )

            set_price_backoff(
                300
            )

            return None

        if response.status_code != 200:

            print(
                "PRICE HTTP ERROR:",
                response.status_code
            )

            return None

        data = response.json()

        if data.get("status") == "error":

            message = data.get(
                "message",
                ""
            )

            print(
                "PRICE API ERROR:",
                message
            )

            # Credit/rate-limit style errors can sometimes
            # arrive inside a JSON response with HTTP 200.
            if "credit" in message.lower() \
                    or "rate" in message.lower() \
                    or "limit" in message.lower():

                set_price_backoff(
                    300
                )

            return None

        price = number(
            data.get("price")
        )

        if price is None:
            return None

        with price_cache_lock:

            price_cache["price"] = price

            price_cache["timestamp"] = (
                time.time()
            )

        return price

    except Exception as exc:

        print(
            "PRICE ERROR:",
            repr(exc)
        )

        return None

    finally:

        try:
            price_request_lock.release()
        except Exception:
            pass


# ============================================================
# CANDLE DATA
# ============================================================

def get_candles(
    symbol,
    interval,
    outputsize=100
):

    if not API_KEY:

        print(
            "ERROR: TWELVE_DATA_API_KEY missing"
        )

        return []

    if interval not in CANDLE_INTERVALS:

        print(
            "INVALID CANDLE INTERVAL:",
            interval
        )

        return []

    now = time.time()

    # --------------------------------------------------------
    # First: return fresh cache.
    # --------------------------------------------------------

    cached_values = get_cached_candles(
        symbol,
        interval
    )

    cached_age = candle_cache_age(
        symbol,
        interval
    )

    refresh_after = FETCH_INTERVALS.get(
        interval,
        300
    )

    if (
        cached_values
        and
        cached_age is not None
        and
        cached_age < refresh_after
    ):

        return cached_values

    # --------------------------------------------------------
    # If this particular timeframe is in backoff,
    # return its own cache instead of blocking everything.
    # --------------------------------------------------------

    if candle_backoff_active(
        interval
    ):

        return cached_values

    request_lock = candle_request_locks.get(
        interval
    )

    if request_lock is None:
        return cached_values

    # --------------------------------------------------------
    # Prevent duplicate request.
    # --------------------------------------------------------

    if not request_lock.acquire(
        blocking=False
    ):

        return cached_values

    try:

        # Another thread may have refreshed it while
        # this thread was waiting.
        latest_cached = get_cached_candles(
            symbol,
            interval
        )

        latest_age = candle_cache_age(
            symbol,
            interval
        )

        if (
            latest_cached
            and
            latest_age is not None
            and
            latest_age < refresh_after
        ):

            return latest_cached

        print(
            "REQUESTING CANDLES:",
            interval
        )

        response = requests.get(
            REST_TIME_SERIES_URL,
            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": API_KEY
            },
            timeout=20
        )

        # ----------------------------------------------------
        # RATE LIMIT
        # ----------------------------------------------------

        if response.status_code == 429:

            print(
                "TWELVE DATA 429:",
                interval,
                "- preserving cache"
            )

            set_candle_backoff(
                interval,
                300
            )

            return get_cached_candles(
                symbol,
                interval
            )

        # ----------------------------------------------------
        # OTHER HTTP ERRORS
        # ----------------------------------------------------

        if response.status_code != 200:

            print(
                "CANDLE HTTP ERROR:",
                interval,
                response.status_code
            )

            return get_cached_candles(
                symbol,
                interval
            )

        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        try:

            data = response.json()

        except Exception as exc:

            print(
                "CANDLE JSON ERROR:",
                interval,
                repr(exc)
            )

            return get_cached_candles(
                symbol,
                interval
            )

        # ----------------------------------------------------
        # API ERROR
        # ----------------------------------------------------

        if data.get("status") == "error":

            message = data.get(
                "message",
                ""
            )

            print(
                "CANDLE API ERROR:",
                interval,
                message
            )

            message_lower = str(
                message
            ).lower()

            if (
                "credit" in message_lower
                or
                "rate" in message_lower
                or
                "limit" in message_lower
                or
                "quota" in message_lower
            ):

                set_candle_backoff(
                    interval,
                    300
                )

            return get_cached_candles(
                symbol,
                interval
            )

        # ----------------------------------------------------
        # VALUES
        # ----------------------------------------------------

        raw_values = data.get(
            "values",
            []
        )

        values = normalize_candles(
            raw_values
        )

        if not values:

            print(
                "CANDLE EMPTY:",
                interval,
                "- preserving cache"
            )

            return get_cached_candles(
                symbol,
                interval
            )

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        cache_candles(
            symbol,
            interval,
            values
        )

        print(
            "CANDLES UPDATED:",
            interval,
            len(values)
        )

        return values

    except requests.RequestException as exc:

        print(
            "CANDLE REQUEST ERROR:",
            interval,
            repr(exc)
        )

        return get_cached_candles(
            symbol,
            interval
        )

    except Exception as exc:

        print(
            "CANDLE ERROR:",
            interval,
            repr(exc)
        )

        return get_cached_candles(
            symbol,
            interval
        )

    finally:

        try:
            request_lock.release()
        except Exception:
            pass


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

        if close is not None:
            closes.append(close)

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    if len(closes) < 20:
        return {}

    current = closes[-1]

    ema20_value = ema(
        closes,
        20
    )

    ema50_value = ema(
        closes,
        50
    )

    current_rsi = rsi(
        closes,
        14
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
    # TREND
    # --------------------------------------------------------

    if (
        ema20_value is not None
        and
        ema50_value is not None
    ):

        if (
            current >
            ema20_value >
            ema50_value
        ):

            trend = "BULLISH"

        elif (
            current <
            ema20_value <
            ema50_value
        ):

            trend = "BEARISH"

        else:

            trend = "NEUTRAL"

    else:

        trend = "NEUTRAL"

    # --------------------------------------------------------
    # MOMENTUM
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
    # STRUCTURE
    # --------------------------------------------------------

    recent = closes[-10:]

    if len(recent) >= 10:

        old_average = (
            sum(recent[:5]) /
            5
        )

        new_average = (
            sum(recent[-5:]) /
            5
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
    # LIQUIDITY
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
    # LIQUIDITY SWEEP
    # --------------------------------------------------------

    sweep = "NONE"

    if (
        len(candles) >= 3
        and
        len(highs) >= 3
        and
        len(lows) >= 3
        and
        len(closes) >= 3
    ):

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
            latest_high >
            previous_high
            and
            latest_close <
            previous_high
        ):

            sweep = "HIGH SWEEP"

        elif (
            latest_low <
            previous_low
            and
            latest_close >
            previous_low
        ):

            sweep = "LOW SWEEP"

    return {

        "price": current,

        "trend": trend,

        "momentum": momentum,

        "structure": structure,

        "rsi": (
            round(
                current_rsi,
                2
            )
            if current_rsi is not None
            else None
        ),

        "ema20": (
            round(
                ema20_value,
                3
            )
            if ema20_value is not None
            else None
        ),

        "ema50": (
            round(
                ema50_value,
                3
            )
            if ema50_value is not None
            else None
        ),

        "support": (
            round(
                support,
                3
            )
            if support is not None
            else None
        ),

        "resistance": (
            round(
                resistance,
                3
            )
            if resistance is not None
            else None
        ),

        "liquidity_high": (
            round(
                liquidity_high,
                3
            )
            if liquidity_high is not None
            else None
        ),

        "liquidity_low": (
            round(
                liquidity_low,
                3
            )
            if liquidity_low is not None
            else None
        ),

        "sweep": sweep
    }


# ============================================================
# AI ANALYSIS
# ============================================================

def build_ai_analysis(
    timeframes,
    live_price
):

    score = 0

    reasons = []

    five = timeframes.get(
        "5min",
        {}
    )

    fifteen = timeframes.get(
        "15min",
        {}
    )

    one_hour = timeframes.get(
        "1h",
        {}
    )

    four_hour = timeframes.get(
        "4h",
        {}
    )

    def add(
        condition,
        points,
        reason
    ):

        nonlocal score

        if condition:

            score += points

            reasons.append(
                reason
            )

    # --------------------------------------------------------
    # 5M
    # --------------------------------------------------------

    add(
        five.get("trend") == "BULLISH",
        2,
        "5m trend bullish"
    )

    add(
        five.get("trend") == "BEARISH",
        -2,
        "5m trend bearish"
    )

    add(
        five.get("momentum") == "BUYING",
        1,
        "5m momentum buying"
    )

    add(
        five.get("momentum") == "SELLING",
        -1,
        "5m momentum selling"
    )

    # --------------------------------------------------------
    # 15M
    # --------------------------------------------------------

    add(
        fifteen.get("trend") == "BULLISH",
        2,
        "15m trend bullish"
    )

    add(
        fifteen.get("trend") == "BEARISH",
        -2,
        "15m trend bearish"
    )

    add(
        fifteen.get("structure") == "HIGHER",
        1,
        "15m structure higher"
    )

    add(
        fifteen.get("structure") == "LOWER",
        -1,
        "15m structure lower"
    )

    # --------------------------------------------------------
    # 1H
    # --------------------------------------------------------

    add(
        one_hour.get("trend") == "BULLISH",
        2,
        "1H trend bullish"
    )

    add(
        one_hour.get("trend") == "BEARISH",
        -2,
        "1H trend bearish"
    )

    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    add(
        four_hour.get("trend") == "BULLISH",
        1,
        "4H trend bullish"
    )

    add(
        four_hour.get("trend") == "BEARISH",
        -1,
        "4H trend bearish"
    )

    # --------------------------------------------------------
    # LIQUIDITY SWEEP
    # --------------------------------------------------------

    sweep = five.get(
        "sweep",
        "NONE"
    )

    add(
        sweep == "LOW SWEEP",
        1,
        "5m low-liquidity sweep"
    )

    add(
        sweep == "HIGH SWEEP",
        -1,
        "5m high-liquidity sweep"
    )

    # --------------------------------------------------------
    # BOUND SCORE
    # --------------------------------------------------------

    score = max(
        -10,
        min(
            10,
            score
        )
    )

    # --------------------------------------------------------
    # DECISION
    # --------------------------------------------------------

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
            50 + abs(score) * 5
        )
    )

    return {

        "signal": decision,

        "confidence": confidence,

        "score": score,

        "reasons": reasons
    }


# ============================================================
# MARKET PHASE
# ============================================================

def build_market_phase(
    timeframes,
    ai
):

    five = timeframes.get(
        "5min",
        {}
    )

    fifteen = timeframes.get(
        "15min",
        {}
    )

    one_hour = timeframes.get(
        "1h",
        {}
    )

    signal = ai.get(
        "signal",
        "WAIT"
    )

    if signal == "BUY":

        phase = "BULLISH PHASE"

        if (
            five.get("trend") == "BULLISH"
            and
            fifteen.get("trend") == "BULLISH"
            and
            one_hour.get("trend") == "BULLISH"
        ):

            duration = (
                "Trend alignment is strong; "
                "continuation can persist while "
                "5m structure remains higher."
            )

        else:

            duration = (
                "Short-term bullish pressure; "
                "watch for 5m structure failure."
            )

    elif signal == "SELL":

        phase = "BEARISH PHASE"

        if (
            five.get("trend") == "BEARISH"
            and
            fifteen.get("trend") == "BEARISH"
            and
            one_hour.get("trend") == "BEARISH"
        ):

            duration = (
                "Trend alignment is strong; "
                "downside pressure can persist while "
                "5m structure remains lower."
            )

        else:

            duration = (
                "Short-term bearish pressure; "
                "watch for 5m structure recovery."
            )

    else:

        phase = "TRANSITION / RANGE"

        duration = (
            "No high-confidence directional alignment. "
            "Wait for multi-timeframe confirmation."
        )

    return {

        "phase": phase,

        "duration": duration
    }


# ============================================================
# TRADE PLAN
# ============================================================

def build_trade_plan(
    timeframes,
    live_price,
    ai
):

    if (
        live_price is None
        or
        ai["signal"] == "WAIT"
    ):

        return {

            "signal": "WAIT",

            "entry": None,

            "stop_loss": None,

            "target_1": None,

            "target_2": None,

            "target_3": None,

            "risk_reward": None,

            "invalidation": None,

            "reason": ai.get(
                "reasons",
                []
            )
        }

    five = timeframes.get(
        "5min",
        {}
    )

    fifteen = timeframes.get(
        "15min",
        {}
    )

    support = (
        five.get("support")
        or
        fifteen.get("support")
    )

    resistance = (
        five.get("resistance")
        or
        fifteen.get("resistance")
    )

    entry = float(
        live_price
    )

    if ai["signal"] == "SELL":

        stop = (
            resistance
            if (
                resistance
                and
                resistance > entry
            )
            else
            entry * 1.005
        )

        risk = max(
            stop - entry,
            entry * 0.001
        )

        t1 = entry - risk

        t2 = entry - (
            2 * risk
        )

        t3 = entry - (
            3 * risk
        )

    else:

        stop = (
            support
            if (
                support
                and
                support < entry
            )
            else
            entry * 0.995
        )

        risk = max(
            entry - stop,
            entry * 0.001
        )

        t1 = entry + risk

        t2 = entry + (
            2 * risk
        )

        t3 = entry + (
            3 * risk
        )

    return {

        "signal": ai["signal"],

        "entry": round(
            entry,
            3
        ),

        "stop_loss": round(
            stop,
            3
        ),

        "target_1": round(
            t1,
            3
        ),

        "target_2": round(
            t2,
            3
        ),

        "target_3": round(
            t3,
            3
        ),

        "risk_reward":
            "1:1 / 1:2 / 1:3",

        "invalidation": round(
            stop,
            3
        ),

        "reason": ai.get(
            "reasons",
            []
        )
    }


# ============================================================
# SIGNAL HISTORY
# ============================================================

def record_signal_history(
    ai,
    live_price
):

    if live_price is None:
        return

    history = state["gold"].setdefault(
        "signal_history",
        []
    )

    previous = (
        history[-1]
        if history
        else None
    )

    if (
        previous
        and
        previous.get("score") ==
        ai["score"]
        and
        previous.get("signal") ==
        ai["signal"]
    ):

        return

    history.append({

        "time": now_text(),

        "price": round(
            float(live_price),
            3
        ),

        "score": ai["score"],

        "signal": ai["signal"],

        "confidence":
            ai["confidence"],

        "reasons": ai.get(
            "reasons",
            []
        )[:8]
    })

    state["gold"]["signal_history"] = (
        history[-50:]
    )


# ============================================================
# ANALYSIS STATE UPDATE
# ============================================================

def apply_analysis(
    timeframe_data,
    live_price
):

    if not timeframe_data:
        return False

    # --------------------------------------------------------
    # Keep previously valid timeframe data.
    # --------------------------------------------------------

    with lock:

        previous_timeframes = (
            state["gold"].get(
                "timeframes",
                {}
            )
        )

    merged_timeframes = dict(
        previous_timeframes
    )

    for interval, result in (
        timeframe_data.items()
    ):

        if result:

            merged_timeframes[
                interval
            ] = result

    # Need enough core data before generating fresh AI.
    required = [
        "5min",
        "15min",
        "1h",
        "4h"
    ]

    available_core = sum(
        1
        for interval in required
        if merged_timeframes.get(
            interval
        )
    )

    if available_core == 0:

        return False

    with lock:

        current_price = (
            state["gold"].get(
                "price"
            )
        )

    if live_price is None:

        live_price = current_price

    if live_price is None:

        live_price = (
            merged_timeframes
            .get("5min", {})
            .get("price")
        )

    if live_price is None:

        return False

    # --------------------------------------------------------
    # AI
    # --------------------------------------------------------

    ai = build_ai_analysis(
        merged_timeframes,
        live_price
    )

    phase = build_market_phase(
        merged_timeframes,
        ai
    )

    trade_plan = build_trade_plan(
        merged_timeframes,
        live_price,
        ai
    )

    five = merged_timeframes.get(
        "5min",
        {}
    )

    # --------------------------------------------------------
    # Update state atomically.
    # --------------------------------------------------------

    with lock:

        state["gold"]["timeframes"] = (
            merged_timeframes
        )

        state["gold"]["candles"] = {

            k: v.get(
                "candles",
                []
            )

            for k, v
            in merged_timeframes.items()

            if isinstance(v, dict)
        }

        state["gold"]["trade_plan"] = (
            trade_plan
        )

        state["gold"]["market_phase"] = (
            phase
        )

        # Only replace primary metrics when
        # 5m analysis is actually available.

        if five:

            state["gold"].update({

                "trend": five.get(
                    "trend",
                    "NEUTRAL"
                ),

                "momentum": five.get(
                    "momentum",
                    "NEUTRAL"
                ),

                "structure": five.get(
                    "structure",
                    "RANGE"
                ),

                "rsi": five.get(
                    "rsi"
                ),

                "ema20": five.get(
                    "ema20"
                ),

                "ema50": five.get(
                    "ema50"
                ),

                "support": five.get(
                    "support"
                ),

                "resistance": five.get(
                    "resistance"
                ),

                "liquidity_high":
                    five.get(
                        "liquidity_high"
                    ),

                "liquidity_low":
                    five.get(
                        "liquidity_low"
                    ),

                "sweep": five.get(
                    "sweep",
                    "NONE"
                )
            })

        state["gold"]["signal"] = (
            ai["signal"]
        )

        state["gold"]["confidence"] = (
            ai["confidence"]
        )

        state["gold"]["score"] = (
            ai["score"]
        )

        state["gold"]["updated"] = (
            now_text()
        )

        # Do not overwrite a healthy WebSocket
        # connection with REST state.

        if (
            state["gold"].get(
                "connection"
            )
            !=
            "CONNECTED"
        ):

            state["gold"]["connection"] = (
                "CONNECTED"
            )

        state["gold"]["error"] = None

        record_signal_history(
            ai,
            live_price
        )

    print(
        "AI UPDATE:",
        f"price={live_price}",
        f"score={ai['score']}",
        f"decision={ai['signal']}"
    )

    broadcast()

    return True


# ============================================================
# CANDLE REFRESH SCHEDULER
# ============================================================
#
# One controlled scheduler handles all timeframes.
#
# Core analysis timeframes receive priority.
# 1m and 1D are intentionally lower priority.
#
# This prevents six simultaneous API requests every 3 minutes.
# ============================================================

def refresh_one_interval(
    interval
):

    candles = get_candles(
        GOLD_SYMBOL,
        interval,
        100
    )

    if not candles:

        return False

    result = analyze_timeframe(
        candles
    )

    if not result:

        return False

    result["candles"] = (
        candles[-120:]
    )

    with lock:

        # Preserve the existing analysis
        # for every other timeframe.

        current = dict(
            state["gold"].get(
                "timeframes",
                {}
            )
        )

        current[interval] = result

    with lock:

        live_price = (
            state["gold"].get(
                "price"
            )
        )

    return apply_analysis(
        {
            interval: result
        },
        live_price
    )


def get_stale_intervals():

    stale = []

    for interval in CORE_ANALYSIS_INTERVALS:

        age = candle_cache_age(
            GOLD_SYMBOL,
            interval
        )

        refresh_after = FETCH_INTERVALS.get(
            interval,
            300
        )

        if age is None:

            stale.append(
                (
                    interval,
                    True,
                    0
                )
            )

        elif age >= refresh_after:

            stale.append(
                (
                    interval,
                    False,
                    age
                )
            )

    # Core first.
    return stale


# ============================================================
# ANALYSIS LOOP
# ============================================================

def gold_analysis_loop():

    # Stagger first startup requests.
    #
    # This is deliberate:
    # 5m -> 15m -> 1h -> 4h
    #
    # We do not fire six requests at once.

    startup_sequence = [
        "5min",
        "15min",
        "1h",
        "4h"
    ]

    for interval in startup_sequence:

        try:

            print(
                "STARTUP CANDLE LOAD:",
                interval
            )

            refresh_one_interval(
                interval
            )

        except Exception as exc:

            print(
                "STARTUP ANALYSIS ERROR:",
                interval,
                repr(exc)
            )

        # Small spacing between API calls.
        time.sleep(5)

    # --------------------------------------------------------
    # Continuous operation
    # --------------------------------------------------------

    last_chart_refresh = {
        "1min": 0,
        "1day": 0
    }

    while True:

        try:

            did_work = False

            # ------------------------------------------------
            # CORE ANALYSIS TIMEFRAMES
            # ------------------------------------------------

            stale = get_stale_intervals()

            for (
                interval,
                is_missing,
                age
            ) in stale:

                try:

                    print(
                        "REFRESH CORE:",
                        interval,
                        "age=",
                        round(age, 1)
                    )

                    success = refresh_one_interval(
                        interval
                    )

                    if success:
                        did_work = True

                except Exception as exc:

                    print(
                        "CORE REFRESH ERROR:",
                        interval,
                        repr(exc)
                    )

                # Do not fire requests back-to-back.
                time.sleep(3)

            # ------------------------------------------------
            # 1MIN CHART
            # ------------------------------------------------

            now = time.time()

            if (
                now -
                last_chart_refresh["1min"]
                >=
                FETCH_INTERVALS["1min"]
            ):

                try:

                    candles = get_candles(
                        GOLD_SYMBOL,
                        "1min",
                        100
                    )

                    if candles:

                        result = analyze_timeframe(
                            candles
                        )

                        if result:

                            result["candles"] = (
                                candles[-120:]
                            )

                            with lock:

                                current = dict(
                                    state["gold"].get(
                                        "timeframes",
                                        {}
                                    )
                                )

                                current["1min"] = (
                                    result
                                )

                            # Chart data is stored even if
                            # it is not used for AI scoring.

                            with lock:

                                state["gold"].setdefault(
                                    "timeframes",
                                    {}
                                )["1min"] = result

                                state["gold"].setdefault(
                                    "candles",
                                    {}
                                )["1min"] = (
                                    candles[-120:]
                                )

                            broadcast()

                        last_chart_refresh[
                            "1min"
                        ] = now

                except Exception as exc:

                    print(
                        "1MIN REFRESH ERROR:",
                        repr(exc)
                    )

                    last_chart_refresh[
                        "1min"
                    ] = now

            # ------------------------------------------------
            # 1DAY CHART
            # ------------------------------------------------

            now = time.time()

            if (
                now -
                last_chart_refresh["1day"]
                >=
                FETCH_INTERVALS["1day"]
            ):

                try:

                    candles = get_candles(
                        GOLD_SYMBOL,
                        "1day",
                        100
                    )

                    if candles:

                        result = analyze_timeframe(
                            candles
                        )

                        if result:

                            result["candles"] = (
                                candles[-120:]
                            )

                            with lock:

                                state["gold"].setdefault(
                                    "timeframes",
                                    {}
                                )["1day"] = result

                                state["gold"].setdefault(
                                    "candles",
                                    {}
                                )["1day"] = (
                                    candles[-120:]
                                )

                            broadcast()

                        last_chart_refresh[
                            "1day"
                        ] = now

                except Exception as exc:

                    print(
                        "1DAY REFRESH ERROR:",
                        repr(exc)
                    )

                    last_chart_refresh[
                        "1day"
                    ] = now

            # ------------------------------------------------
            # If nothing needed updating, sleep modestly.
            # ------------------------------------------------

            if not did_work:

                time.sleep(15)

            else:

                time.sleep(10)

        except Exception as exc:

            print(
                "AI LOOP ERROR:",
                repr(exc)
            )

            # Critical:
            # Do NOT wipe the existing valid analysis.
            with lock:

                state["gold"]["error"] = (
                    str(exc)
                )

            broadcast()

            time.sleep(20)


# ============================================================
# REST PRICE FALLBACK LOOP
# ============================================================

def gold_price_loop():

    while True:

        try:

            # ------------------------------------------------
            # WebSocket is the preferred live price source.
            # REST is fallback only when WS is not fresh.
            # ------------------------------------------------

            with lock:

                connection = (
                    state["gold"].get(
                        "connection"
                    )
                )

                price = (
                    state["gold"].get(
                        "price"
                    )
                )

            if (
                connection == "CONNECTED"
                and
                price is not None
            ):

                # WS is healthy.
                # Do not spend REST credits.
                time.sleep(30)
                continue

            rest_price = get_live_price()

            if rest_price is not None:

                set_gold_price(
                    rest_price,
                    "Twelve Data REST"
                )

                print(
                    "GOLD REST FALLBACK PRICE:",
                    rest_price
                )

        except Exception as exc:

            print(
                "PRICE LOOP ERROR:",
                repr(exc)
            )

        time.sleep(30)


# ============================================================
# WEBSOCKET
# ============================================================

def gold_websocket_loop():

    reconnect_delay = 30

    while True:

        ws = None

        try:

            if not API_KEY:

                raise RuntimeError(
                    "TWELVE_DATA_API_KEY is missing"
                )

            print(
                "Connecting Twelve Data Gold WebSocket..."
            )

            ws_url = (
                WS_URL
                + "?apikey="
                + API_KEY
            )

            ws = websocket.create_connection(
                ws_url,
                timeout=20
            )

            print(
                "GOLD WS CONNECTED"
            )

            subscribe = {

                "action": "subscribe",

                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(
                json.dumps(
                    subscribe
                )
            )

            print(
                "GOLD WS SUBSCRIBED:",
                GOLD_SYMBOL
            )

            with lock:

                state["gold"]["connection"] = (
                    "CONNECTED"
                )

                state["gold"]["error"] = None

                state["gold"]["data_source"] = (
                    "Twelve Data WebSocket + REST candles"
                )

            broadcast()

            last_heartbeat = time.time()

            while True:

                # ------------------------------------------------
                # Heartbeat
                # ------------------------------------------------

                if (
                    time.time()
                    -
                    last_heartbeat
                    >
                    10
                ):

                    try:

                        ws.send(
                            json.dumps({
                                "action":
                                "heartbeat"
                            })
                        )

                        last_heartbeat = (
                            time.time()
                        )

                    except Exception:

                        raise RuntimeError(
                            "WebSocket heartbeat failed"
                        )

                ws.settimeout(15)

                try:

                    raw = ws.recv()

                except websocket.WebSocketTimeoutException:

                    continue

                if not raw:

                    raise RuntimeError(
                        "WebSocket closed"
                    )

                try:

                    message = json.loads(
                        raw
                    )

                except Exception:

                    continue

                event = message.get(
                    "event"
                )

                # ------------------------------------------------
                # PRICE
                # ------------------------------------------------

                if event == "price":

                    symbol = message.get(
                        "symbol"
                    )

                    price = number(
                        message.get(
                            "price"
                        )
                    )

                    if (
                        symbol == GOLD_SYMBOL
                        and
                        price is not None
                    ):

                        set_gold_price(
                            price,
                            "Twelve Data WebSocket"
                        )

                # ------------------------------------------------
                # SUBSCRIBE STATUS
                # ------------------------------------------------

                elif event == "subscribe-status":

                    print(
                        "GOLD SUBSCRIBE STATUS:",
                        message
                    )

                # ------------------------------------------------
                # HEARTBEAT
                # ------------------------------------------------

                elif event == "heartbeat":

                    pass

                # ------------------------------------------------
                # ERROR
                # ------------------------------------------------

                elif event == "error":

                    print(
                        "GOLD WS SERVER ERROR:",
                        message
                    )

        except Exception as exc:

            error_text = str(exc)

            print(
                "GOLD WEBSOCKET ERROR:",
                repr(exc)
            )

            with lock:

                state["gold"]["connection"] = (
                    "REST FALLBACK"
                )

                state["gold"]["error"] = (
                    "WebSocket unavailable; "
                    "using REST fallback. "
                    + error_text
                )

            broadcast()

        finally:

            try:

                if ws is not None:
                    ws.close()

            except Exception:

                pass

        print(
            "Gold WebSocket retry in "
            f"{reconnect_delay}s..."
        )

        time.sleep(
            reconnect_delay
        )


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    global workers_started

    with lock:

        if workers_started:

            return

        workers_started = True

    print(
        "Trading AI workers started."
    )

    ws_thread = threading.Thread(
        target=gold_websocket_loop,
        daemon=True,
        name="GoldWebSocket"
    )

    ws_thread.start()

    price_thread = threading.Thread(
        target=gold_price_loop,
        daemon=True,
        name="GoldPriceREST"
    )

    price_thread.start()

    analysis_thread = threading.Thread(
        target=gold_analysis_loop,
        daemon=True,
        name="GoldAnalysis"
    )

    analysis_thread.start()


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    start_workers()

    return render_template_string(
        HTML
    )


@app.route("/api/market")
def api_market():

    start_workers()

    with lock:

        data = safe_json(
            state
        )

    return jsonify(
        data
    )


# ============================================================
# DIRECT CANDLE API
# ============================================================
#
# Additive route.
# Existing dashboard does not depend on it, but it is useful
# for reliable chart access and future UI improvements.
# ============================================================

@app.route(
    "/api/candles/<interval>"
)
def api_candles(interval):

    start_workers()

    if interval not in CANDLE_INTERVALS:

        return jsonify({
            "ok": False,
            "error": "Invalid timeframe",
            "interval": interval,
            "candles": []
        }), 400

    candles = get_candles(
        GOLD_SYMBOL,
        interval,
        100
    )

    return jsonify({

        "ok": bool(candles),

        "symbol": GOLD_SYMBOL,

        "interval": interval,

        "candles": candles[-120:]

    })


@app.route("/stream")
def stream():

    start_workers()

    client_queue = queue.Queue(
        maxsize=20
    )

    with clients_lock:

        clients.append(
            client_queue
        )

    def generate():

        try:

            with lock:

                initial = json.dumps(
                    state,
                    separators=(
                        ",",
                        ":"
                    ),
                    default=str
                )

            yield (
                "event: market\n"
                f"data: {initial}\n\n"
            )

            while True:

                try:

                    payload = (
                        client_queue.get(
                            timeout=25
                        )
                    )

                    yield (
                        "event: market\n"
                        f"data: {payload}\n\n"
                    )

                except queue.Empty:

                    yield (
                        ": heartbeat\n\n"
                    )

        except GeneratorExit:

            pass

        except Exception as exc:

            print(
                "STREAM ERROR:",
                repr(exc)
            )

        finally:

            with clients_lock:

                try:

                    if client_queue in clients:

                        clients.remove(
                            client_queue
                        )

                except ValueError:

                    pass

    return Response(

        generate(),

        mimetype="text/event-stream",

        headers={

            "Cache-Control":
                "no-cache",

            "Connection":
                "keep-alive",

            "X-Accel-Buffering":
                "no"

        }
    )


@app.route("/health")
def health():

    with lock:

        gold_connection = (
            state["gold"]["connection"]
        )

        gold_price = (
            state["gold"]["price"]
        )

        gold_error = (
            state["gold"].get(
                "error"
            )
        )

    return jsonify({

        "status": "ok",

        "service": "Trading-AI",

        "gold_connection":
            gold_connection,

        "gold_price":
            gold_price,

        "gold_error":
            gold_error,

        "time":
            now_text()
    })


# ============================================================
# HTML DASHBOARD
# ============================================================

HTML = r"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width, initial-scale=1.0"
>

<title>Trading AI</title>

<script src="https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js"></script>

<style>

*{
box-sizing:border-box
}

body{
margin:0;
min-height:100vh;
font-family:Arial,sans-serif;
color:white;
background:
radial-gradient(
circle at top,
#183d75,
#08172f 45%,
#020817
);
}

.page{
width:94%;
max-width:1280px;
margin:auto;
padding:22px 0 45px;
}

.header{
text-align:center;
margin-bottom:20px;
}

.header h1{
margin:0;
font-size:30px;
}

.header p{
margin:7px 0;
color:#7fa9e8;
}

.card{
padding:20px;
margin-bottom:18px;
border-radius:18px;
border:1px solid rgba(110,160,230,.2);
background:
linear-gradient(
145deg,
rgba(20,43,82,.96),
rgba(5,19,42,.97)
);
box-shadow:
0 18px 45px rgba(0,0,0,.32);
}

.asset{
font-size:20px;
font-weight:700;
}

.price{
margin-top:8px;
font-size:38px;
font-weight:700;
}

.live{
margin-top:4px;
color:#2be48e;
font-size:13px;
font-weight:700;
}

.connection{
margin-top:4px;
color:#719bd4;
font-size:11px;
}

.metrics{
display:grid;
grid-template-columns:repeat(6,1fr);
gap:8px;
margin-top:18px;
}

.metric{
padding:12px;
min-height:68px;
border-radius:11px;
background:rgba(11,35,70,.72);
border:1px solid rgba(100,150,220,.13);
}

.label{
color:#78a5e5;
font-size:10px;
margin-bottom:6px;
}

.value{
font-size:15px;
font-weight:700;
}

.analysis{
margin-top:14px;
padding:17px;
border-radius:13px;
background:
linear-gradient(
100deg,
#12529f,
#0b3977
);
}

.analysis-label{
color:#82b0f5;
font-size:10px;
font-weight:700;
}

.signal{
margin-top:4px;
font-size:27px;
font-weight:800;
}

.confidence,
.score{
margin-top:5px;
font-size:14px;
font-weight:700;
}

.phase{
margin-top:10px;
padding:12px;
border-radius:10px;
background:rgba(0,0,0,.18);
font-size:12px;
line-height:1.5;
}

.toolbar{
display:flex;
gap:7px;
flex-wrap:wrap;
margin:15px 0 10px;
}

.tf{
border:1px solid #38679e;
background:#092447;
color:#bcd8ff;
padding:8px 12px;
border-radius:8px;
cursor:pointer;
font-weight:700;
}

.tf.active{
background:#1a64ad;
color:white;
}

.chart{
height:430px;
border-radius:12px;
overflow:hidden;
border:1px solid rgba(100,150,220,.16);
}

.grid2{
display:grid;
grid-template-columns:1.3fr .7fr;
gap:14px;
margin-top:14px;
}

.panel{
padding:16px;
border-radius:13px;
background:rgba(4,17,37,.55);
border:1px solid rgba(100,150,220,.13);
}

.panel h3{
margin:0 0 12px;
font-size:14px;
color:#9fc5f5;
}

.tradegrid{
display:grid;
grid-template-columns:repeat(4,1fr);
gap:8px;
}

.tradebox{
padding:11px;
border-radius:9px;
background:#081d39;
}

.tradebox b{
display:block;
font-size:14px;
margin-top:4px;
}

.history{
max-height:250px;
overflow:auto;
}

.row{
display:grid;
grid-template-columns:
75px 80px 55px 70px 1fr;
gap:7px;
padding:8px 0;
border-bottom:
1px solid rgba(130,170,220,.1);
font-size:11px;
}

.reason{
color:#8eafd8;
}

.error{
margin-top:12px;
color:#ffb3b3;
font-size:12px;
}

.footer{
text-align:center;
color:#6489ba;
font-size:11px;
margin-top:15px;
}

@media(max-width:900px){

.metrics{
grid-template-columns:repeat(3,1fr);
}

.grid2{
grid-template-columns:1fr;
}

.tradegrid{
grid-template-columns:repeat(2,1fr);
}

}

@media(max-width:480px){

.page{
width:96%;
}

.price{
font-size:31px;
}

.metrics{
grid-template-columns:repeat(2,1fr);
}

.chart{
height:330px;
}

}

</style>

</head>

<body>

<div class="page">

<div class="header">

<h1>Trading AI</h1>

<p>
Real-Time Market Intelligence •
Explainable Multi-Timeframe Analysis
</p>

</div>

<div id="gold"></div>

<div id="oil"></div>

<div class="footer">
LIVE MARKET DATA • TRADING-AI
</div>

</div>

<script>

let activeTF="5min";

let chart=null;

let resizeObserver=null;

let latestData=null;


function fmt(
v,
d=3
){

if(
v===null ||
v===undefined
)
return "—";

let n=Number(v);

return Number.isNaN(n)
? "—"
: n.toFixed(d);

}


function tfLabel(x){

return {

"1min":"1m",
"5min":"5m",
"15min":"15m",
"1h":"1H",
"4h":"4H",
"1day":"1D"

}[x] || x;

}


function metric(
a,
b
){

return `
<div class="metric">

<div class="label">
${a}
</div>

<div class="value">
${b}
</div>

</div>
`;

}


function box(
a,
b
){

return `
<div class="tradebox">

<span class="label">
${a}
</span>

<b>
${b}
</b>

</div>
`;

}


function draw(c){

const el =
document.getElementById(
"price-chart"
);

if(!el)
return;

if(chart){

try{
chart.remove();
}
catch(e){}

chart=null;

}

if(
typeof LightweightCharts
===
"undefined"
)
return;


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

timeScale:{
timeVisible:true,
secondsVisible:false
}

}
);


const s =
chart.addCandlestickSeries({

upColor:"#20c997",
downColor:"#ff5c73",
borderVisible:false,
wickUpColor:"#20c997",
wickDownColor:"#ff5c73"

});


const d =
(c || [])
.map(x=>{

const rawTime =
x.datetime ||
x.date;

const time =
Math.floor(
new Date(rawTime)
.getTime()/1000
);

return {

time:time,

open:+x.open,

high:+x.high,

low:+x.low,

close:+x.close

};

})
.filter(
x=>
Number.isFinite(x.time)
&&
[
x.open,
x.high,
x.low,
x.close
]
.every(Number.isFinite)
);


const unique=[];

let previous=null;


for(
const candle of d
){

if(
previous===null
||
candle.time>previous
){

unique.push(candle);

previous=
candle.time;

}

}


if(unique.length){

s.setData(
unique
);

chart.timeScale()
.fitContent();

}


if(resizeObserver){

try{
resizeObserver.disconnect();
}
catch(e){}

}


if(
typeof ResizeObserver
!=="undefined"
){

resizeObserver =
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

}
catch(e){}

}

}
);

resizeObserver.observe(el);

}

}


function hist(h){

if(
!h ||
!h.length
)

return `
<div class="reason">
No signal changes recorded yet.
</div>
`;

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
${x.score>0?"+":""}
${x.score}
</b>

<span>
${x.signal || "WAIT"}
</span>

<span class="reason">
${
(x.reasons||[])
.slice(0,3)
.join(" • ")
}
</span>

</div>

`
)
.join("");

}


function render(data){

latestData=data;

let g=data.gold;

let p=
g.trade_plan || {};

let phase =
g.market_phase || {};


document.getElementById(
"gold"
).innerHTML=`

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
•
Source:
${g.data_source || "Twelve Data"}
•
Updated:
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
${phase.phase || "MARKET PHASE"}
</b>

<br>

${phase.duration || "Waiting for market data."}

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
class="tf
${activeTF===x?"active":""}"
onclick="selectTF('${x}')"
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

<b>
Why:
</b>

${
(p.reason||[])
.join(" • ")
||
"Waiting for confirmation."
}

</div>

</div>


<div class="panel">

<h3>
🕐 AI SIGNAL HISTORY
</h3>

<div class="history">

${hist(
g.signal_history
)}

</div>

</div>


</div>

</div>

`;


document.getElementById(
"oil"
).innerHTML=`

<div class="card">

<div class="asset">
🛢️ Crude Oil — WTI
</div>

<div class="price">
—
</div>

<div class="live">
● PLAN LIMIT
</div>

<div class="connection">
Connection:
${data.oil.connection ||
"UNAVAILABLE"}
</div>

<div class="error">
${data.oil.error || ""}
</div>

</div>

`;


draw(
(g.candles || {})[
activeTF
] || []
);

}


function selectTF(x){

activeTF=x;

if(latestData)
render(latestData);

}


fetch(
"/api/market",
{
cache:"no-store"
}
)
.then(
r=>r.json()
)
.then(
render
)
.catch(
console.error
);


const source =
new EventSource(
"/stream"
);


source.addEventListener(
"market",
e=>{

try{

render(
JSON.parse(
e.data
)
);

}
catch(err){

console.error(err);

}

}
);


source.onerror=()=>{

console.log(
"Market stream reconnecting..."
);

};

</script>

</body>

</html>
"""


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    start_workers()

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
