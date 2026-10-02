from flask import Flask, jsonify, render_template_string
import os
import json
import time
import threading
import requests
import websocket
from datetime import datetime, timezone


# ============================================================
# TRADING AI
# REAL-TIME MARKET INTELLIGENCE
# EXPLAINABLE MULTI-TIMEFRAME ANALYSIS
#
# GOLD XAU/USD
#
# STABILITY VERSION
# ============================================================

app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# Oil intentionally disabled for current Twelve Data plan.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

REST_PRICE_URL = "https://api.twelvedata.com/price"
REST_TIME_SERIES_URL = "https://api.twelvedata.com/time_series"

# KEEP THIS WEBSOCKET CONFIG BECAUSE IT IS CURRENTLY WORKING.
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"


# Supported dashboard intervals
CANDLE_INTERVALS = [
    "1min",
    "5min",
    "15min",
    "1h",
    "4h",
    "1day",
]


# ============================================================
# CANDLE REFRESH CONTROL
#
# Conservative refresh values reduce unnecessary API usage.
#
# 1min  -> chart/on-demand
# 5min  -> primary analysis
# 15min -> confirmation
# 1h    -> confirmation
# 4h    -> higher timeframe
# 1day  -> very slow background refresh
# ============================================================

FETCH_INTERVALS = {
    "1min": 900,       # 15 min
    "5min": 300,       # 5 min
    "15min": 600,      # 10 min
    "1h": 1800,        # 30 min
    "4h": 3600,        # 1 hour
    "1day": 21600,     # 6 hours
}


# Only these are continuously used by AI analysis.
ANALYSIS_INTERVALS = [
    "5min",
    "15min",
    "1h",
    "4h",
]


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.RLock()

workers_started = False


# ------------------------------------------------------------
# Candle cache
# ------------------------------------------------------------

candle_cache = {}

# Per-timeframe API backoff.
# IMPORTANT:
# One failed timeframe must NOT block all other timeframes.
candle_backoff_until = {
    interval: 0
    for interval in CANDLE_INTERVALS
}

candle_status = {
    interval: {
        "last_success": 0,
        "last_attempt": 0,
        "last_error": None,
        "count": 0,
        "credits_used": None,
        "credits_left": None,
    }
    for interval in CANDLE_INTERVALS
}


# ------------------------------------------------------------
# Price cache
# ------------------------------------------------------------

price_cache = {
    "price": None,
    "timestamp": 0,
}

price_backoff_until = 0


# ------------------------------------------------------------
# WebSocket state
# ------------------------------------------------------------

ws_state = {
    "connected": False,
    "last_message": 0,
    "last_error": None,
    "reconnect_count": 0,
}


# ------------------------------------------------------------
# Application state
# ------------------------------------------------------------

state = {
    "gold": {
        "symbol": GOLD_SYMBOL,

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
        "source": "Twelve Data WebSocket",
        "updated": None,

        "data_status": "WAITING FOR CANDLES",
        "analysis_status": "WAITING FOR DATA",

        "timeframes": {},

        "candles": {},

        "signal_history": [],

        "trade_plan": {
            "signal": "WAIT",
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "risk_reward": None,
        },

        "market_phase": "WAITING",

        "error": None,
    }
}


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now_string():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def clamp(value, low, high):
    return max(low, min(high, value))


def format_number(value, decimals=3):
    if value is None:
        return None
    try:
        return round(float(value), decimals)
    except Exception:
        return None


def set_gold_field(key, value):
    with state_lock:
        state["gold"][key] = value


# ============================================================
# EMA
# ============================================================

def ema(values, period):
    if not values or len(values) < period:
        return None

    values = [float(x) for x in values]

    multiplier = 2.0 / (period + 1.0)

    current = sum(values[:period]) / period

    for price in values[period:]:
        current = (
            (price - current) * multiplier
        ) + current

    return current


# ============================================================
# RSI
# ============================================================

def rsi(values, period=14):
    if not values or len(values) <= period:
        return None

    values = [float(x) for x in values]

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

    return 100.0 - (100.0 / (1.0 + rs))


# ============================================================
# DATETIME SORTING
# ============================================================

def candle_datetime_value(item):
    """
    Converts Twelve Data datetime into sortable numeric value.

    We do not assume API order.
    This prevents accidental reverse ordering.
    """

    raw = str(item.get("datetime", ""))

    try:
        value = raw.replace("Z", "")

        if value.endswith("+00:00"):
            dt = datetime.fromisoformat(value)
        else:
            dt = datetime.fromisoformat(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.timestamp()

    except Exception:
        return 0


# ============================================================
# API CREDIT LOGGING
# ============================================================

def read_credit_headers(response):
    used = response.headers.get("api-credits-used")
    left = response.headers.get("api-credits-left")

    return used, left


# ============================================================
# GET CANDLES
# ============================================================

def get_candles(
    symbol,
    interval,
    outputsize=120,
    force=False,
):
    """
    Stable candle loader.

    Rules:
    - Uses per-timeframe cache.
    - Uses per-timeframe backoff.
    - A 429 on one timeframe does not affect others.
    - Existing valid cache is always preserved.
    """

    now = time.time()

    if interval not in CANDLE_INTERVALS:
        return candle_cache.get(interval, [])

    # --------------------------------------------------------
    # Existing cache
    # --------------------------------------------------------

    cached = candle_cache.get(interval)

    if (
        not force
        and cached
        and (now - cached.get("timestamp", 0))
        < FETCH_INTERVALS.get(interval, 300)
    ):
        return cached.get("values", [])


    # --------------------------------------------------------
    # Per-timeframe backoff
    # --------------------------------------------------------

    if now < candle_backoff_until.get(interval, 0):

        if cached:
            return cached.get("values", [])

        return []


    # --------------------------------------------------------
    # API key check
    # --------------------------------------------------------

    if not API_KEY:

        candle_status[interval]["last_error"] = (
            "TWELVE_DATA_API_KEY missing"
        )

        if cached:
            return cached.get("values", [])

        return []


    candle_status[interval]["last_attempt"] = now


    # --------------------------------------------------------
    # Request
    # --------------------------------------------------------

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "timezone": "UTC",
    }


    try:

        response = requests.get(
            REST_TIME_SERIES_URL,
            params=params,
            timeout=15,
        )


        used, left = read_credit_headers(response)

        candle_status[interval]["credits_used"] = used
        candle_status[interval]["credits_left"] = left


        # ----------------------------------------------------
        # 429
        # ----------------------------------------------------

        if response.status_code == 429:

            candle_backoff_until[interval] = now + 120

            error_text = (
                "HTTP 429 - API credit/rate limit"
            )

            candle_status[interval]["last_error"] = error_text

            print(
                f"CANDLE {interval} 429 | "
                f"credits_used={used} | "
                f"credits_left={left}"
            )

            if cached:
                return cached.get("values", [])

            return []


        # ----------------------------------------------------
        # HTTP error
        # ----------------------------------------------------

        if response.status_code != 200:

            error_text = (
                f"HTTP {response.status_code}"
            )

            candle_status[interval]["last_error"] = error_text

            print(
                f"CANDLE {interval} ERROR | "
                f"{error_text}"
            )

            if cached:
                return cached.get("values", [])

            return []


        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        data = response.json()


        # Twelve Data API-level error
        if isinstance(data, dict) and data.get("status") == "error":

            message = data.get(
                "message",
                "Unknown Twelve Data error",
            )

            candle_status[interval]["last_error"] = message

            print(
                f"CANDLE {interval} API ERROR | "
                f"{message}"
            )

            if cached:
                return cached.get("values", [])

            return []


        values = data.get("values", [])


        if not isinstance(values, list) or not values:

            candle_status[interval]["last_error"] = (
                "Empty candle response"
            )

            print(
                f"CANDLE {interval} EMPTY RESPONSE"
            )

            if cached:
                return cached.get("values", [])

            return []


        # ----------------------------------------------------
        # Clean candles
        # ----------------------------------------------------

        clean = []

        for item in values:

            if not isinstance(item, dict):
                continue

            dt = item.get("datetime")

            o = safe_float(item.get("open"))
            h = safe_float(item.get("high"))
            l = safe_float(item.get("low"))
            c = safe_float(item.get("close"))

            if (
                not dt
                or o is None
                or h is None
                or l is None
                or c is None
            ):
                continue

            clean.append(
                {
                    "datetime": str(dt),
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                }
            )


        if len(clean) < 2:

            candle_status[interval]["last_error"] = (
                "Not enough valid candles"
            )

            if cached:
                return cached.get("values", [])

            return []


        # ----------------------------------------------------
        # Sort oldest -> newest
        # ----------------------------------------------------

        clean.sort(
            key=candle_datetime_value
        )


        # ----------------------------------------------------
        # Save cache
        # ----------------------------------------------------

        candle_cache[interval] = {
            "timestamp": now,
            "values": clean,
        }


        candle_status[interval]["last_success"] = now
        candle_status[interval]["count"] = len(clean)
        candle_status[interval]["last_error"] = None


        print(
            f"CANDLE {interval} OK | "
            f"{len(clean)} candles | "
            f"credits_used={used} | "
            f"credits_left={left}"
        )


        return clean


    except requests.RequestException as exc:

        candle_status[interval]["last_error"] = (
            str(exc)
        )

        print(
            f"CANDLE {interval} REQUEST ERROR | "
            f"{exc}"
        )

        if cached:
            return cached.get("values", [])

        return []


    except Exception as exc:

        candle_status[interval]["last_error"] = (
            str(exc)
        )

        print(
            f"CANDLE {interval} PARSE ERROR | "
            f"{exc}"
        )

        if cached:
            return cached.get("values", [])

        return []


# ============================================================
# LIVE PRICE REST FALLBACK
# ============================================================

def get_rest_price(symbol):

    global price_backoff_until

    now = time.time()

    if now < price_backoff_until:

        return price_cache.get("price")


    if not API_KEY:
        return price_cache.get("price")


    try:

        response = requests.get(
            REST_PRICE_URL,
            params={
                "symbol": symbol,
                "apikey": API_KEY,
            },
            timeout=10,
        )


        if response.status_code == 429:

            price_backoff_until = now + 120

            print(
                "PRICE REST 429 - backing off"
            )

            return price_cache.get("price")


        if response.status_code != 200:

            return price_cache.get("price")


        data = response.json()

        value = safe_float(
            data.get("price")
        )


        if value is not None:

            with state_lock:

                price_cache["price"] = value
                price_cache["timestamp"] = now

                state["gold"]["price"] = value
                state["gold"]["source"] = (
                    "Twelve Data REST fallback"
                )
                state["gold"]["updated"] = (
                    utc_now_string()
                )

            return value


    except Exception as exc:

        print(
            f"REST PRICE ERROR: {exc}"
        )


    return price_cache.get("price")


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def analyze_timeframe(candles):

    if not candles or len(candles) < 20:
        return None


    closes = [
        float(x["close"])
        for x in candles
    ]

    highs = [
        float(x["high"])
        for x in candles
    ]

    lows = [
        float(x["low"])
        for x in candles
    ]


    current = closes[-1]


    ema20_value = ema(
        closes,
        20,
    )

    ema50_value = ema(
        closes,
        50,
    )

    rsi_value = rsi(
        closes,
        14,
    )


    # --------------------------------------------------------
    # Support / resistance
    # --------------------------------------------------------

    recent_highs = highs[-20:]
    recent_lows = lows[-20:]

    resistance = max(recent_highs)
    support = min(recent_lows)


    # --------------------------------------------------------
    # Trend
    # --------------------------------------------------------

    trend = "RANGE"

    if (
        ema20_value is not None
        and ema50_value is not None
    ):

        if (
            current > ema20_value
            and ema20_value > ema50_value
        ):
            trend = "BULLISH"

        elif (
            current < ema20_value
            and ema20_value < ema50_value
        ):
            trend = "BEARISH"


    # --------------------------------------------------------
    # Momentum
    # --------------------------------------------------------

    momentum = "NEUTRAL"

    if len(closes) >= 6:

        move = (
            closes[-1]
            - closes[-6]
        )

        threshold = max(
            abs(current) * 0.00025,
            0.01,
        )

        if move > threshold:
            momentum = "BULLISH"

        elif move < -threshold:
            momentum = "BEARISH"


    # --------------------------------------------------------
    # Structure
    # --------------------------------------------------------

    structure = "RANGE"

    if len(closes) >= 10:

        recent = closes[-5:]
        previous = closes[-10:-5]

        recent_avg = (
            sum(recent) / len(recent)
        )

        previous_avg = (
            sum(previous) / len(previous)
        )

        difference = (
            recent_avg
            - previous_avg
        )

        threshold = max(
            abs(current) * 0.00015,
            0.005,
        )

        if difference > threshold:
            structure = "HIGHER"

        elif difference < -threshold:
            structure = "LOWER"


    # --------------------------------------------------------
    # Liquidity
    # --------------------------------------------------------

    liquidity_high = max(
        highs[-10:]
    )

    liquidity_low = min(
        lows[-10:]
    )


    # --------------------------------------------------------
    # Sweep
    #
    # Compare latest candle with previous liquidity.
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
            latest_low < previous_low
            and latest_close > previous_low
        ):
            sweep = "LOW SWEEP"


        elif (
            latest_high > previous_high
            and latest_close < previous_high
        ):
            sweep = "HIGH SWEEP"


    return {
        "price": format_number(current),

        "trend": trend,
        "momentum": momentum,
        "structure": structure,

        "rsi": format_number(
            rsi_value,
            2,
        ),

        "ema20": format_number(
            ema20_value,
        ),

        "ema50": format_number(
            ema50_value,
        ),

        "support": format_number(
            support,
        ),

        "resistance": format_number(
            resistance,
        ),

        "liquidity_high": format_number(
            liquidity_high,
        ),

        "liquidity_low": format_number(
            liquidity_low,
        ),

        "sweep": sweep,
    }


# ============================================================
# MULTI-TIMEFRAME AI SCORE
# ============================================================

def build_ai_analysis(
    tf,
    current_price,
):

    score = 0


    five = tf.get("5min")
    fifteen = tf.get("15min")
    one_hour = tf.get("1h")
    four_hour = tf.get("4h")


    # --------------------------------------------------------
    # 5 MIN
    # --------------------------------------------------------

    if five:

        if five["trend"] == "BULLISH":
            score += 2

        elif five["trend"] == "BEARISH":
            score -= 2


        if five["momentum"] == "BULLISH":
            score += 1

        elif five["momentum"] == "BEARISH":
            score -= 1


        if five["sweep"] == "LOW SWEEP":
            score += 1

        elif five["sweep"] == "HIGH SWEEP":
            score -= 1


    # --------------------------------------------------------
    # 15 MIN
    # --------------------------------------------------------

    if fifteen:

        if fifteen["trend"] == "BULLISH":
            score += 2

        elif fifteen["trend"] == "BEARISH":
            score -= 2


        if fifteen["structure"] == "HIGHER":
            score += 1

        elif fifteen["structure"] == "LOWER":
            score -= 1


        # Pivot-like directional context
        support = fifteen.get("support")
        resistance = fifteen.get("resistance")

        if (
            support is not None
            and resistance is not None
            and current_price is not None
        ):

            midpoint = (
                support + resistance
            ) / 2.0

            if current_price > midpoint:
                score += 1

            elif current_price < midpoint:
                score -= 1


    # --------------------------------------------------------
    # 1 HOUR
    # --------------------------------------------------------

    if one_hour:

        if one_hour["trend"] == "BULLISH":
            score += 2

        elif one_hour["trend"] == "BEARISH":
            score -= 2


    # --------------------------------------------------------
    # 4 HOUR
    # --------------------------------------------------------

    if four_hour:

        if four_hour["trend"] == "BULLISH":
            score += 1

        elif four_hour["trend"] == "BEARISH":
            score -= 1


    score = int(
        clamp(score, -10, 10)
    )


    if score >= 6:
        signal = "BUY"

    elif score <= -6:
        signal = "SELL"

    else:
        signal = "WAIT"


    confidence = int(
        clamp(
            50 + abs(score) * 5,
            50,
            95,
        )
    )


    return {
        "signal": signal,
        "score": score,
        "confidence": confidence,
    }


# ============================================================
# MARKET PHASE
# ============================================================

def build_market_phase(
    tf,
    ai,
):

    five = tf.get("5min")
    fifteen = tf.get("15min")


    if not five or not fifteen:
        return "WAITING"


    signal = ai["signal"]


    if (
        signal == "BUY"
        and five["trend"] == "BULLISH"
        and fifteen["trend"] == "BULLISH"
    ):
        return "BULLISH EXPANSION"


    if (
        signal == "SELL"
        and five["trend"] == "BEARISH"
        and fifteen["trend"] == "BEARISH"
    ):
        return "BEARISH EXPANSION"


    if five["sweep"] == "LOW SWEEP":
        return "LIQUIDITY RECLAIM"


    if five["sweep"] == "HIGH SWEEP":
        return "LIQUIDITY REJECTION"


    if (
        five["structure"] == "HIGHER"
        and five["trend"] == "BULLISH"
    ):
        return "ACCUMULATION / BUILD-UP"


    if (
        five["structure"] == "LOWER"
        and five["trend"] == "BEARISH"
    ):
        return "DISTRIBUTION / BUILD-UP"


    return "RANGE / CONFLICT"


# ============================================================
# TRADE PLAN
# ============================================================

def build_trade_plan(
    signal,
    current_price,
    tf,
):

    plan = {
        "signal": signal,
        "entry": None,
        "stop_loss": None,
        "target1": None,
        "target2": None,
        "target3": None,
        "risk_reward": None,
    }


    if current_price is None:
        return plan


    if signal not in ("BUY", "SELL"):
        return plan


    five = tf.get("5min")
    fifteen = tf.get("15min")


    support = None
    resistance = None


    if five:

        support = five.get("support")
        resistance = five.get("resistance")


    if support is None and fifteen:
        support = fifteen.get("support")

    if resistance is None and fifteen:
        resistance = fifteen.get("resistance")


    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    if signal == "BUY":

        entry = current_price


        if support is not None and support < entry:
            stop = support

        else:
            stop = entry * 0.997


        risk = entry - stop


        if risk <= 0:
            return plan


        t1 = entry + risk * 1.5
        t2 = entry + risk * 2.0
        t3 = entry + risk * 3.0


        # Resistance can be useful as first target,
        # but only if it is above entry.
        if (
            resistance is not None
            and resistance > entry
        ):
            t1 = resistance


        plan.update({
            "entry": format_number(entry),
            "stop_loss": format_number(stop),
            "target1": format_number(t1),
            "target2": format_number(t2),
            "target3": format_number(t3),
            "risk_reward": "1:3",
        })


    # --------------------------------------------------------
    # SELL
    # --------------------------------------------------------

    elif signal == "SELL":

        entry = current_price


        if (
            resistance is not None
            and resistance > entry
        ):
            stop = resistance

        else:
            stop = entry * 1.003


        risk = stop - entry


        if risk <= 0:
            return plan


        t1 = entry - risk * 1.5
        t2 = entry - risk * 2.0
        t3 = entry - risk * 3.0


        if (
            support is not None
            and support < entry
        ):
            t1 = support


        plan.update({
            "entry": format_number(entry),
            "stop_loss": format_number(stop),
            "target1": format_number(t1),
            "target2": format_number(t2),
            "target3": format_number(t3),
            "risk_reward": "1:3",
        })


    return plan


# ============================================================
# SIGNAL HISTORY
# ============================================================

def add_signal_history(
    signal,
    score,
):

    with state_lock:

        history = state["gold"].get(
            "signal_history",
            [],
        )


        last = (
            history[-1]
            if history
            else None
        )


        # Only record meaningful changes.
        if (
            last
            and last.get("signal") == signal
            and last.get("score") == score
        ):
            return


        history.append({
            "time": utc_now_string(),
            "signal": signal,
            "score": score,
        })


        # Keep last 30
        state["gold"]["signal_history"] = (
            history[-30:]
        )


# ============================================================
# UPDATE ANALYSIS
# ============================================================

def update_analysis():

    try:

        # ----------------------------------------------------
        # Current live price
        # ----------------------------------------------------

        with state_lock:
            current_price = (
                price_cache.get("price")
                or state["gold"].get("price")
            )


        if current_price is None:

            current_price = get_rest_price(
                GOLD_SYMBOL
            )


        if current_price is None:

            with state_lock:

                state["gold"]["analysis_status"] = (
                    "WAITING FOR LIVE PRICE"
                )

            return


        # ----------------------------------------------------
        # Get analysis timeframes
        #
        # Cache prevents unnecessary API requests.
        # ----------------------------------------------------

        tf_results = {}


        for interval in ANALYSIS_INTERVALS:

            candles = get_candles(
                GOLD_SYMBOL,
                interval,
                outputsize=120,
                force=False,
            )


            # IMPORTANT:
            # If this timeframe temporarily fails,
            # do NOT delete existing valid analysis.
            if candles:

                result = analyze_timeframe(
                    candles
                )

                if result:

                    tf_results[interval] = result

                    # Save candles into state.
                    with state_lock:
                        state["gold"]["candles"][
                            interval
                        ] = candles


            # Small delay prevents an unnecessary burst.
            time.sleep(0.2)


        # ----------------------------------------------------
        # Merge with existing valid timeframe analysis.
        # ----------------------------------------------------

        with state_lock:

            old_tf = dict(
                state["gold"].get(
                    "timeframes",
                    {}
                )
            )


        merged_tf = old_tf.copy()
        merged_tf.update(tf_results)


        # ----------------------------------------------------
        # Need at least 5m analysis for primary dashboard.
        # ----------------------------------------------------

        if not merged_tf.get("5min"):

            with state_lock:

                state["gold"]["data_status"] = (
                    "LIVE PRICE • WAITING FOR 5M CANDLES"
                )

                state["gold"]["analysis_status"] = (
                    "WAITING FOR CANDLE DATA"
                )

                state["gold"]["error"] = (
                    candle_status["5min"].get(
                        "last_error"
                    )
                )

            return


        # ----------------------------------------------------
        # AI
        # ----------------------------------------------------

        ai = build_ai_analysis(
            merged_tf,
            current_price,
        )


        # ----------------------------------------------------
        # Market phase
        # ----------------------------------------------------

        market_phase = build_market_phase(
            merged_tf,
            ai,
        )


        # ----------------------------------------------------
        # Trade plan
        # ----------------------------------------------------

        trade_plan = build_trade_plan(
            ai["signal"],
            current_price,
            merged_tf,
        )


        # ----------------------------------------------------
        # Primary metrics
        #
        # 5m is primary.
        # ----------------------------------------------------

        primary = merged_tf.get(
            "5min"
        )


        # ----------------------------------------------------
        # Update state
        # ----------------------------------------------------

        with state_lock:

            gold = state["gold"]


            gold["price"] = format_number(
                current_price
            )


            gold["trend"] = primary.get(
                "trend",
                gold.get("trend", "WAITING"),
            )

            gold["momentum"] = primary.get(
                "momentum",
                gold.get("momentum", "WAITING"),
            )

            gold["structure"] = primary.get(
                "structure",
                gold.get("structure", "WAITING"),
            )


            gold["rsi"] = primary.get(
                "rsi"
            )

            gold["ema20"] = primary.get(
                "ema20"
            )

            gold["ema50"] = primary.get(
                "ema50"
            )


            gold["support"] = primary.get(
                "support"
            )

            gold["resistance"] = primary.get(
                "resistance"
            )


            gold["liquidity_high"] = primary.get(
                "liquidity_high"
            )

            gold["liquidity_low"] = primary.get(
                "liquidity_low"
            )


            gold["sweep"] = primary.get(
                "sweep",
                "NONE",
            )


            gold["signal"] = ai["signal"]
            gold["score"] = ai["score"]
            gold["confidence"] = ai["confidence"]


            gold["timeframes"] = merged_tf


            gold["trade_plan"] = trade_plan


            gold["market_phase"] = market_phase


            gold["data_status"] = (
                "LIVE PRICE + CANDLE DATA"
            )

            gold["analysis_status"] = (
                "ANALYSIS ACTIVE"
            )


            gold["error"] = None


            gold["updated"] = (
                utc_now_string()
            )


        add_signal_history(
            ai["signal"],
            ai["score"],
        )


    except Exception as exc:

        print(
            f"ANALYSIS ERROR: {exc}"
        )

        # Do NOT destroy last valid state.
        with state_lock:

            state["gold"]["error"] = str(
                exc
            )

            state["gold"]["analysis_status"] = (
                "LAST VALID ANALYSIS PRESERVED"
            )


# ============================================================
# WEBSOCKET
# ============================================================

def gold_websocket_worker():

    while True:

        if not API_KEY:

            with state_lock:
                ws_state["connected"] = False
                ws_state["last_error"] = (
                    "TWELVE_DATA_API_KEY missing"
                )

                state["gold"]["connection"] = (
                    "API KEY MISSING"
                )

            time.sleep(10)
            continue


        ws = None


        try:

            print(
                "Connecting Twelve Data Gold WebSocket..."
            )


            url = (
                WS_URL
                + "?apikey="
                + API_KEY
            )


            ws = websocket.create_connection(
                url,
                timeout=20,
                enable_multithread=True,
            )


            with state_lock:

                ws_state["connected"] = True
                ws_state["last_error"] = None

                state["gold"]["connection"] = (
                    "CONNECTED"
                )

                state["gold"]["source"] = (
                    "Twelve Data WebSocket"
                )


            print(
                "GOLD WS CONNECTED"
            )


            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                },
            }


            ws.send(
                json.dumps(
                    subscribe_message
                )
            )


            print(
                "GOLD WS SUBSCRIBED:",
                GOLD_SYMBOL,
            )


            last_heartbeat = time.time()


            while True:

                # ------------------------------------------------
                # Heartbeat
                # ------------------------------------------------

                if (
                    time.time()
                    - last_heartbeat
                    >= 10
                ):

                    try:

                        ws.send(
                            json.dumps({
                                "action": "heartbeat"
                            })
                        )

                    except Exception:
                        pass

                    last_heartbeat = (
                        time.time()
                    )


                # ------------------------------------------------
                # Receive
                # ------------------------------------------------

                ws.settimeout(5)


                try:

                    raw = ws.recv()

                except websocket.WebSocketTimeoutException:

                    continue


                if not raw:
                    raise RuntimeError(
                        "WebSocket closed"
                    )


                try:
                    message = json.loads(raw)
                except Exception:
                    continue


                # ------------------------------------------------
                # Error message
                # ------------------------------------------------

                if (
                    isinstance(message, dict)
                    and message.get("status") == "error"
                ):

                    error_message = (
                        message.get(
                            "message",
                            "WebSocket error"
                        )
                    )

                    print(
                        "GOLD WS ERROR:",
                        error_message,
                    )

                    with state_lock:
                        ws_state["last_error"] = (
                            error_message
                        )

                    continue


                # ------------------------------------------------
                # Price
                # ------------------------------------------------

                value = None


                if isinstance(message, dict):

                    value = (
                        message.get("price")
                        or message.get("close")
                    )


                    # Some Twelve Data WS messages
                    # may contain nested data.
                    if value is None:

                        data = message.get(
                            "data"
                        )

                        if isinstance(data, dict):

                            value = (
                                data.get("price")
                                or data.get("close")
                            )


                price = safe_float(
                    value
                )


                if price is None:
                    continue


                now = time.time()


                with state_lock:

                    price_cache["price"] = price
                    price_cache["timestamp"] = now


                    ws_state["connected"] = True
                    ws_state["last_message"] = now


                    state["gold"]["price"] = (
                        format_number(price)
                    )

                    state["gold"]["connection"] = (
                        "CONNECTED"
                    )

                    state["gold"]["source"] = (
                        "Twelve Data WebSocket"
                    )

                    state["gold"]["updated"] = (
                        utc_now_string()
                    )


        except Exception as exc:

            print(
                "GOLD WS ERROR:",
                repr(exc),
            )


            with state_lock:

                ws_state["connected"] = False
                ws_state["last_error"] = (
                    str(exc)
                )

                ws_state["reconnect_count"] += 1

                state["gold"]["connection"] = (
                    "RECONNECTING"
                )


        finally:

            try:

                if ws is not None:
                    ws.close()

            except Exception:
                pass


        time.sleep(5)


# ============================================================
# REST PRICE FALLBACK WORKER
# ============================================================

def price_fallback_worker():

    while True:

        try:

            with state_lock:

                connected = (
                    ws_state["connected"]
                )


            if not connected:

                get_rest_price(
                    GOLD_SYMBOL
                )


        except Exception as exc:

            print(
                f"PRICE FALLBACK ERROR: {exc}"
            )


        # Conservative REST fallback.
        time.sleep(30)


# ============================================================
# ANALYSIS WORKER
# ============================================================

def analysis_worker():

    # Give WebSocket a moment to initialize.
    time.sleep(2)


    while True:

        try:

            update_analysis()

        except Exception as exc:

            print(
                f"ANALYSIS WORKER ERROR: {exc}"
            )


        # State is polled by browser every 5 sec,
        # but candles are protected by their own TTL.
        time.sleep(20)


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    global workers_started


    with state_lock:

        if workers_started:
            return

        workers_started = True


    threading.Thread(
        target=gold_websocket_worker,
        daemon=True,
        name="gold-websocket",
    ).start()


    threading.Thread(
        target=price_fallback_worker,
        daemon=True,
        name="price-fallback",
    ).start()


    threading.Thread(
        target=analysis_worker,
        daemon=True,
        name="analysis-worker",
    ).start()


    print(
        "Trading AI workers started."
    )


# ============================================================
# API ROUTES
# ============================================================

@app.route("/health")
def health():

    with state_lock:

        gold = state["gold"]

        return jsonify({
            "status": "ok",

            "api_key_configured": bool(
                API_KEY
            ),

            "websocket": {
                "connected": ws_state[
                    "connected"
                ],
                "last_message": ws_state[
                    "last_message"
                ],
                "last_error": ws_state[
                    "last_error"
                ],
                "reconnect_count": ws_state[
                    "reconnect_count"
                ],
            },

            "gold": {
                "price": gold["price"],
                "signal": gold["signal"],
                "score": gold["score"],
                "analysis_status": gold[
                    "analysis_status"
                ],
            },

            "candles": {
                interval: {
                    "count": candle_status[
                        interval
                    ]["count"],

                    "last_success": candle_status[
                        interval
                    ]["last_success"],

                    "last_error": candle_status[
                        interval
                    ]["last_error"],

                    "credits_used": candle_status[
                        interval
                    ]["credits_used"],

                    "credits_left": candle_status[
                        interval
                    ]["credits_left"],
                }
                for interval in CANDLE_INTERVALS
            },
        })


@app.route("/api/state")
def api_state():

    start_workers()


    with state_lock:

        payload = {
            "gold": dict(
                state["gold"]
            )
        }


        # Add diagnostic candle status
        payload["gold"][
            "candle_status"
        ] = {
            interval: dict(
                candle_status[interval]
            )
            for interval in CANDLE_INTERVALS
        }


        payload["gold"][
            "websocket"
        ] = dict(
            ws_state
        )


    return jsonify(payload)


@app.route("/api/candles/<interval>")
def api_candles(interval):

    start_workers()


    if interval not in CANDLE_INTERVALS:

        return jsonify({
            "error": "Unsupported interval",
            "supported": CANDLE_INTERVALS,
        }), 400


    candles = get_candles(
        GOLD_SYMBOL,
        interval,
        outputsize=150,
        force=False,
    )


    return jsonify({
        "symbol": GOLD_SYMBOL,
        "interval": interval,
        "candles": candles,
        "count": len(candles),
        "status": candle_status[
            interval
        ],
    })


@app.route("/api/refresh")
def api_refresh():

    start_workers()


    # Force only primary 5m timeframe.
    # Do not hammer every timeframe.
    candles = get_candles(
        GOLD_SYMBOL,
        "5min",
        outputsize=150,
        force=True,
    )


    if candles:

        result = analyze_timeframe(
            candles
        )


        if result:

            with state_lock:

                state["gold"]["candles"][
                    "5min"
                ] = candles

                state["gold"]["timeframes"][
                    "5min"
                ] = result


    # Run complete analysis using available cache.
    update_analysis()


    with state_lock:

        return jsonify({
            "ok": True,
            "gold": state["gold"],
        })


# ============================================================
# DASHBOARD HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>
Trading AI — Real-Time Market Intelligence
</title>


<script src="https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js"></script>


<style>

* {
    box-sizing: border-box;
}


body {
    margin: 0;
    background: #080b12;
    color: #e8edf5;
    font-family:
        Inter,
        Arial,
        sans-serif;
}


.container {
    width: min(1500px, 96%);
    margin: 0 auto;
    padding: 22px 0 40px;
}


.header {
    margin-bottom: 20px;
}


.title {
    font-size: 30px;
    font-weight: 800;
    letter-spacing: -0.5px;
}


.subtitle {
    color: #8d98aa;
    margin-top: 6px;
}


.card {
    background: #10151f;
    border: 1px solid #202938;
    border-radius: 14px;
    padding: 18px;
    box-shadow:
        0 10px 30px rgba(0,0,0,.18);
}


.hero {
    display: grid;
    grid-template-columns:
        1.4fr
        1fr
        1fr
        1fr;
    gap: 14px;
    margin-bottom: 14px;
}


.symbol {
    color: #8d98aa;
    font-size: 13px;
    text-transform: uppercase;
}


.price {
    font-size: 38px;
    font-weight: 800;
    margin-top: 8px;
}


.live {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    margin-top: 8px;
    font-size: 13px;
}


.dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #39d98a;
}


.metric-label {
    color: #7e899b;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .7px;
}


.metric-value {
    font-size: 20px;
    font-weight: 750;
    margin-top: 8px;
}


.grid {
    display: grid;
    grid-template-columns:
        repeat(4, minmax(0,1fr));
    gap: 14px;
    margin-bottom: 14px;
}


.grid-3 {
    display: grid;
    grid-template-columns:
        repeat(3, minmax(0,1fr));
    gap: 14px;
    margin-bottom: 14px;
}


.grid-2 {
    display: grid;
    grid-template-columns:
        repeat(2, minmax(0,1fr));
    gap: 14px;
    margin-bottom: 14px;
}


.signal-card {
    text-align: center;
}


.signal {
    font-size: 34px;
    font-weight: 900;
    margin-top: 7px;
}


.score {
    font-size: 46px;
    font-weight: 900;
    margin-top: 3px;
}


.confidence {
    color: #aab4c3;
    margin-top: 5px;
}


.status {
    color: #8d98aa;
    font-size: 12px;
    line-height: 1.55;
}


.row {
    display: flex;
    justify-content: space-between;
    gap: 15px;
    padding: 9px 0;
    border-bottom: 1px solid #1d2532;
}


.row:last-child {
    border-bottom: 0;
}


.label {
    color: #8c97a8;
}


.value {
    font-weight: 700;
    text-align: right;
}


.timeframes {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-bottom: 12px;
}


.tf {
    border: 1px solid #2a3444;
    background: #111722;
    color: #cbd4df;
    border-radius: 8px;
    padding: 8px 13px;
    cursor: pointer;
    font-weight: 700;
}


.tf:hover {
    background: #192130;
}


.tf.active {
    background: #263244;
    color: #fff;
    border-color: #4b5d76;
}


#chart {
    width: 100%;
    height: 520px;
}


.chart-wrap {
    min-height: 560px;
}


.history {
    max-height: 340px;
    overflow: auto;
}


.history-item {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 9px 0;
    border-bottom: 1px solid #1d2532;
    font-size: 13px;
}


.muted {
    color: #7f8999;
}


.warning {
    color: #e9bd65;
}


.setup-grid {
    display: grid;
    grid-template-columns:
        repeat(3, minmax(0,1fr));
    gap: 10px;
}


.setup-box {
    background: #0c1119;
    border: 1px solid #1d2634;
    border-radius: 10px;
    padding: 13px;
}


.setup-value {
    font-size: 18px;
    font-weight: 800;
    margin-top: 5px;
}


@media(max-width: 1000px) {

    .hero {
        grid-template-columns:
            repeat(2, minmax(0,1fr));
    }

    .grid,
    .grid-3 {
        grid-template-columns:
            repeat(2, minmax(0,1fr));
    }

}


@media(max-width: 650px) {

    .container {
        width: 94%;
    }

    .title {
        font-size: 23px;
    }

    .hero,
    .grid,
    .grid-2,
    .grid-3,
    .setup-grid {
        grid-template-columns: 1fr;
    }

    .price {
        font-size: 32px;
    }

    #chart {
        height: 420px;
    }

    .chart-wrap {
        min-height: 460px;
    }

}

</style>

</head>


<body>

<div class="container">

    <div class="header">

        <div class="title">
            Trading AI — Real-Time Market Intelligence
        </div>

        <div class="subtitle">
            Explainable Multi-Timeframe Analysis
        </div>

    </div>


    <!-- HERO -->

    <div class="hero">

        <div class="card">

            <div class="symbol">
                Gold — XAU/USD
            </div>

            <div
                id="price"
                class="price"
            >
                —
            </div>

            <div class="live">

                <span class="dot"></span>

                <span id="connection">
                    CONNECTING
                </span>

            </div>

            <div
                id="updated"
                class="status"
                style="margin-top:7px;"
            >
                Waiting...
            </div>

        </div>


        <div class="card signal-card">

            <div class="metric-label">
                Signal
            </div>

            <div
                id="signal"
                class="signal"
            >
                WAIT
            </div>

            <div
                id="confidence"
                class="confidence"
            >
                Confidence 50%
            </div>

        </div>


        <div class="card signal-card">

            <div class="metric-label">
                AI Score
            </div>

            <div
                id="score"
                class="score"
            >
                0
            </div>

            <div
                id="phase"
                class="confidence"
            >
                WAITING
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Data Status
            </div>

            <div
                id="data-status"
                class="metric-value"
            >
                WAITING
            </div>

            <div
                id="analysis-status"
                class="status"
                style="margin-top:8px;"
            >
                Waiting for candle data
            </div>

        </div>

    </div>


    <!-- MARKET -->

    <div class="grid">

        <div class="card">

            <div class="metric-label">
                Trend
            </div>

            <div
                id="trend"
                class="metric-value"
            >
                WAITING
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Momentum
            </div>

            <div
                id="momentum"
                class="metric-value"
            >
                WAITING
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Structure
            </div>

            <div
                id="structure"
                class="metric-value"
            >
                WAITING
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                RSI
            </div>

            <div
                id="rsi"
                class="metric-value"
            >
                —
            </div>

        </div>

    </div>


    <!-- INDICATORS -->

    <div class="grid">

        <div class="card">

            <div class="metric-label">
                EMA 20
            </div>

            <div
                id="ema20"
                class="metric-value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                EMA 50
            </div>

            <div
                id="ema50"
                class="metric-value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Support
            </div>

            <div
                id="support"
                class="metric-value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Resistance
            </div>

            <div
                id="resistance"
                class="metric-value"
            >
                —
            </div>

        </div>

    </div>


    <!-- LIQUIDITY -->

    <div class="grid">

        <div class="card">

            <div class="metric-label">
                Liquidity High
            </div>

            <div
                id="liq-high"
                class="metric-value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Liquidity Low
            </div>

            <div
                id="liq-low"
                class="metric-value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Sweep
            </div>

            <div
                id="sweep"
                class="metric-value"
            >
                NONE
            </div>

        </div>


        <div class="card">

            <div class="metric-label">
                Current Phase
            </div>

            <div
                id="market-phase"
                class="metric-value"
            >
                WAITING
            </div>

        </div>

    </div>


    <!-- CHART -->

    <div class="card chart-wrap">

        <div
            class="metric-label"
            style="margin-bottom:12px;"
        >
            Gold Price Chart
        </div>


        <div class="timeframes">

            <button
                class="tf"
                data-tf="1min"
            >
                1m
            </button>

            <button
                class="tf active"
                data-tf="5min"
            >
                5m
            </button>

            <button
                class="tf"
                data-tf="15min"
            >
                15m
            </button>

            <button
                class="tf"
                data-tf="1h"
            >
                1H
            </button>

            <button
                class="tf"
                data-tf="4h"
            >
                4H
            </button>

            <button
                class="tf"
                data-tf="1day"
            >
                1D
            </button>

        </div>


        <div id="chart"></div>

        <div
            id="chart-status"
            class="status"
            style="margin-top:8px;"
        >
            Loading chart...
        </div>

    </div>


    <!-- TRADE SETUP -->

    <div class="card" style="margin-top:14px;">

        <div
            class="metric-label"
            style="margin-bottom:14px;"
        >
            Trade Setup
        </div>


        <div class="setup-grid">

            <div class="setup-box">

                <div class="metric-label">
                    Entry
                </div>

                <div
                    id="entry"
                    class="setup-value"
                >
                    —
                </div>

            </div>


            <div class="setup-box">

                <div class="metric-label">
                    Stop Loss
                </div>

                <div
                    id="sl"
                    class="setup-value"
                >
                    —
                </div>

            </div>


            <div class="setup-box">

                <div class="metric-label">
                    Risk / Reward
                </div>

                <div
                    id="rr"
                    class="setup-value"
                >
                    —
                </div>

            </div>


            <div class="setup-box">

                <div class="metric-label">
                    Target 1
                </div>

                <div
                    id="t1"
                    class="setup-value"
                >
                    —
                </div>

            </div>


            <div class="setup-box">

                <div class="metric-label">
                    Target 2
                </div>

                <div
                    id="t2"
                    class="setup-value"
                >
                    —
                </div>

            </div>


            <div class="setup-box">

                <div class="metric-label">
                    Target 3
                </div>

                <div
                    id="t3"
                    class="setup-value"
                >
                    —
                </div>

            </div>

        </div>

    </div>


    <!-- TIMEFRAME ANALYSIS -->

    <div class="grid-2" style="margin-top:14px;">

        <div class="card">

            <div
                class="metric-label"
                style="margin-bottom:12px;"
            >
                Multi-Timeframe Analysis
            </div>

            <div id="tf-analysis">
                <div class="muted">
                    Waiting for data...
                </div>
            </div>

        </div>


        <div class="card">

            <div
                class="metric-label"
                style="margin-bottom:12px;"
            >
                Signal History
            </div>

            <div
                id="history"
                class="history"
            >
                <div class="muted">
                    No signal history yet.
                </div>
            </div>

        </div>

    </div>


    <!-- STATUS -->

    <div class="card">

        <div
            class="metric-label"
            style="margin-bottom:10px;"
        >
            System Status
        </div>

        <div
            id="system-status"
            class="status"
        >
            Starting...
        </div>

    </div>

</div>


<script>

let chart = null;
let candleSeries = null;

let activeTimeframe = "5min";

let chartInitialized = false;

let chartLastSignature = "";

let chartRequestInProgress = false;

let chartLastRequestTime = 0;

let latestState = null;


/* =========================================================
   HELPERS
   ========================================================= */

function valueOrDash(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "—";
    }

    return value;

}


function setText(id, value) {

    const el = document.getElementById(id);

    if (!el) return;

    el.textContent = valueOrDash(value);

}


/* =========================================================
   CHART INIT
   ========================================================= */

function ensureChart() {

    if (chartInitialized) {
        return;
    }


    const container =
        document.getElementById("chart");


    if (!container) {
        return;
    }


    chart = LightweightCharts.createChart(
        container,
        {
            layout: {
                background: {
                    type: "solid",
                    color: "#10151f"
                },

                textColor: "#aab4c3"
            },

            grid: {
                vertLines: {
                    color: "#1b2330"
                },

                horzLines: {
                    color: "#1b2330"
                }
            },

            rightPriceScale: {
                borderColor: "#26303d"
            },

            timeScale: {
                borderColor: "#26303d",
                timeVisible: true,
                secondsVisible: false
            },

            crosshair: {
                mode: 1
            },

            width: container.clientWidth,
            height: 520
        }
    );


    candleSeries =
        chart.addCandlestickSeries({
            upColor: "#35c98a",
            downColor: "#ef6571",
            borderUpColor: "#35c98a",
            borderDownColor: "#ef6571",
            wickUpColor: "#35c98a",
            wickDownColor: "#ef6571"
        });


    chartInitialized = true;


    const observer =
        new ResizeObserver(() => {

            if (!chart) return;

            const width =
                container.clientWidth;

            if (width > 0) {

                chart.applyOptions({
                    width: width
                });

            }

        });


    observer.observe(container);

}


/* =========================================================
   NORMALIZE CANDLES
   ========================================================= */

function normalizeCandles(candles) {

    if (!Array.isArray(candles)) {
        return [];
    }


    const result = [];


    for (const c of candles) {

        if (!c) continue;


        const raw =
            c.datetime ||
            c.time;


        if (!raw) continue;


        let timestamp = null;


        if (
            typeof raw === "number"
        ) {

            timestamp = raw;

        } else {

            let text =
                String(raw);


            /*
             * Backend requests UTC.
             * Make UTC explicit for browser.
             */

            if (
                !text.endsWith("Z")
                &&
                !text.includes("+")
            ) {

                text =
                    text.replace(
                        " ",
                        "T"
                    )
                    + "Z";

            }


            const parsed =
                Date.parse(text);


            if (!Number.isNaN(parsed)) {

                timestamp =
                    Math.floor(
                        parsed / 1000
                    );

            }

        }


        if (
            !timestamp ||
            !Number.isFinite(timestamp)
        ) {
            continue;
        }


        const open =
            Number(c.open);

        const high =
            Number(c.high);

        const low =
            Number(c.low);

        const close =
            Number(c.close);


        if (
            !Number.isFinite(open) ||
            !Number.isFinite(high) ||
            !Number.isFinite(low) ||
            !Number.isFinite(close)
        ) {
            continue;
        }


        result.push({
            time: timestamp,
            open: open,
            high: high,
            low: low,
            close: close
        });

    }


    result.sort(
        (a, b) =>
            a.time - b.time
    );


    /*
     * Remove duplicate timestamps.
     */

    const unique = [];

    let lastTime = null;


    for (const candle of result) {

        if (
            candle.time === lastTime
        ) {
            continue;
        }


        unique.push(candle);

        lastTime =
            candle.time;

    }


    return unique;

}


/* =========================================================
   DRAW CHART
   ========================================================= */

function drawChart(candles) {

    ensureChart();


    if (
        !candleSeries ||
        !candles ||
        !candles.length
    ) {

        document.getElementById(
            "chart-status"
        ).textContent =
            "No candle data available yet.";

        return;

    }


    const normalized =
        normalizeCandles(candles);


    if (!normalized.length) {

        document.getElementById(
            "chart-status"
        ).textContent =
            "Candle data received but could not be parsed.";

        return;

    }


    candleSeries.setData(
        normalized
    );


    chart.timeScale().fitContent();


    document.getElementById(
        "chart-status"
    ).textContent =
        `${activeTimeframe} • ${normalized.length} candles`;

}


/* =========================================================
   FETCH CHART ONLY WHEN NEEDED
   ========================================================= */

async function loadChart(
    timeframe,
    force = false
) {

    if (chartRequestInProgress) {
        return;
    }


    const now =
        Date.now();


    /*
     * Safety:
     * Never allow chart endpoint to be hammered.
     */

    if (
        !force
        &&
        now - chartLastRequestTime < 30000
    ) {
        return;
    }


    chartRequestInProgress = true;
    chartLastRequestTime = now;


    document.getElementById(
        "chart-status"
    ).textContent =
        `Loading ${timeframe} chart...`;


    try {

        const response =
            await fetch(
                `/api/candles/${encodeURIComponent(timeframe)}`,
                {
                    cache: "default"
                }
            );


        if (!response.ok) {

            throw new Error(
                `HTTP ${response.status}`
            );

        }


        const data =
            await response.json();


        const candles =
            data.candles || [];


        if (
            candles.length
        ) {

            drawChart(
                candles
            );


            /*
             * Save into latest state too.
             */

            if (latestState) {

                latestState.candles =
                    latestState.candles || {};

                latestState.candles[
                    timeframe
                ] = candles;

            }

        } else {

            document.getElementById(
                "chart-status"
            ).textContent =
                `No ${timeframe} candle data returned.`;

        }


    } catch (error) {

        console.error(
            "Chart error:",
            error
        );


        document.getElementById(
            "chart-status"
        ).textContent =
            `Chart error: ${error.message}`;

    } finally {

        chartRequestInProgress = false;

    }

}


/* =========================================================
   CHART SIGNATURE
   ========================================================= */

function candleSignature(candles) {

    if (
        !Array.isArray(candles)
        ||
        !candles.length
    ) {
        return "";
    }


    const first =
        candles[0];

    const last =
        candles[candles.length - 1];


    return [
        candles.length,
        first.datetime || first.time,
        last.datetime || last.time,
        last.close
    ].join("|");

}


/* =========================================================
   UPDATE CHART FROM STATE
   ========================================================= */

function updateChartFromState(g) {

    ensureChart();


    if (!g) return;


    const candles =
        (
            g.candles &&
            g.candles[activeTimeframe]
        )
        || [];


    if (!candles.length) {

        /*
         * Only one controlled fallback request.
         * NOT every 5 seconds.
         */

        loadChart(
            activeTimeframe,
            false
        );

        return;

    }


    const signature =
        candleSignature(
            candles
        );


    if (
        signature !==
        chartLastSignature
    ) {

        chartLastSignature =
            signature;

        drawChart(
            candles
        );

    }

}


/* =========================================================
   TIMEFRAME BUTTONS
   ========================================================= */

document
    .querySelectorAll(".tf")
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                const tf =
                    button.dataset.tf;


                if (
                    tf ===
                    activeTimeframe
                ) {
                    return;
                }


                activeTimeframe =
                    tf;


                chartLastSignature =
                    "";


                document
                    .querySelectorAll(".tf")
                    .forEach(btn => {

                        btn.classList.toggle(
                            "active",
                            btn.dataset.tf === tf
                        );

                    });


                /*
                 * Timeframe change is an intentional
                 * chart request.
                 */

                chartLastRequestTime = 0;

                loadChart(
                    tf,
                    true
                );

            }
        );

    });


/* =========================================================
   UPDATE TIMEFRAME ANALYSIS
   ========================================================= */

function renderTimeframes(
    timeframes
) {

    const container =
        document.getElementById(
            "tf-analysis"
        );


    if (!timeframes) {

        container.innerHTML =
            '<div class="muted">Waiting for data...</div>';

        return;

    }


    const order = [
        ["5min", "5 MIN"],
        ["15min", "15 MIN"],
        ["1h", "1 HOUR"],
        ["4h", "4 HOUR"]
    ];


    let html = "";


    for (const [key, label] of order) {

        const tf =
            timeframes[key];


        if (!tf) {

            html += `
                <div class="row">
                    <div class="label">
                        ${label}
                    </div>
                    <div class="value">
                        WAITING
                    </div>
                </div>
            `;

            continue;

        }


        html += `
            <div class="row">
                <div class="label">
                    ${label}
                </div>

                <div class="value">
                    ${tf.trend || "—"}
                    /
                    ${tf.structure || "—"}
                </div>
            </div>
        `;

    }


    container.innerHTML =
        html;

}


/* =========================================================
   SIGNAL HISTORY
   ========================================================= */

function renderHistory(
    history
) {

    const container =
        document.getElementById(
            "history"
        );


    if (
        !Array.isArray(history)
        ||
        !history.length
    ) {

        container.innerHTML =
            '<div class="muted">No signal history yet.</div>';

        return;

    }


    const items =
        [...history]
            .reverse()
            .slice(0, 20);


    let html = "";


    for (const item of items) {

        html += `
            <div class="history-item">

                <span>
                    ${item.time || ""}
                </span>

                <strong>
                    ${item.signal || "WAIT"}
                    (${item.score ?? 0})
                </strong>

            </div>
        `;

    }


    container.innerHTML =
        html;

}


/* =========================================================
   RENDER DASHBOARD
   ========================================================= */

function render(g) {

    if (!g) return;


    latestState = g;


    setText(
        "price",
        g.price
    );


    setText(
        "connection",
        g.connection
    );


    setText(
        "updated",
        g.updated
    );


    setText(
        "signal",
        g.signal
    );


    setText(
        "confidence",
        `Confidence ${g.confidence ?? 50}%`
    );


    setText(
        "score",
        g.score ?? 0
    );


    setText(
        "phase",
        g.market_phase
    );


    setText(
        "data-status",
        g.data_status
    );


    setText(
        "analysis-status",
        g.analysis_status
    );


    setText(
        "trend",
        g.trend
    );


    setText(
        "momentum",
        g.momentum
    );


    setText(
        "structure",
        g.structure
    );


    setText(
        "rsi",
        g.rsi
    );


    setText(
        "ema20",
        g.ema20
    );


    setText(
        "ema50",
        g.ema50
    );


    setText(
        "support",
        g.support
    );


    setText(
        "resistance",
        g.resistance
    );


    setText(
        "liq-high",
        g.liquidity_high
    );


    setText(
        "liq-low",
        g.liquidity_low
    );


    setText(
        "sweep",
        g.sweep
    );


    setText(
        "market-phase",
        g.market_phase
    );


    const plan =
        g.trade_plan || {};


    setText(
        "entry",
        plan.entry
    );


    setText(
        "sl",
        plan.stop_loss
    );


    setText(
        "rr",
        plan.risk_reward
    );


    setText(
        "t1",
        plan.target1
    );


    setText(
        "t2",
        plan.target2
    );


    setText(
        "t3",
        plan.target3
    );


    renderTimeframes(
        g.timeframes
    );


    renderHistory(
        g.signal_history
    );


    const ws =
        g.websocket || {};


    let system =
        `Connection: ${g.connection || "UNKNOWN"}`
        + ` • Source: ${g.source || "Unknown"}`
        + ` • Analysis: ${g.analysis_status || "Unknown"}`;


    if (ws.last_error) {

        system +=
            ` • WS: ${ws.last_error}`;

    }


    if (g.error) {

        system +=
            ` • Error: ${g.error}`;

    }


    document.getElementById(
        "system-status"
    ).textContent =
        system;


    /*
     * IMPORTANT:
     * Chart is updated only if candle signature changed.
     * It is NOT recreated on every state poll.
     */

    updateChartFromState(g);

}


/* =========================================================
   LOAD STATE
   ========================================================= */

async function loadState() {

    try {

        const response =
            await fetch(
                "/api/state",
                {
                    cache: "no-store"
                }
            );


        if (!response.ok) {

            throw new Error(
                `HTTP ${response.status}`
            );

        }


        const data =
            await response.json();


        render(
            data.gold
        );


    } catch (error) {

        console.error(
            "State error:",
            error
        );


        document.getElementById(
            "system-status"
        ).textContent =
            `State error: ${error.message}`;

    }

}


/* =========================================================
   START
   ========================================================= */

ensureChart();


/*
 * First chart request.
 */

chartLastRequestTime = 0;

loadChart(
    activeTimeframe,
    true
);


/*
 * State polling is okay.
 *
 * It only asks for /api/state.
 * It does NOT request candles every 5 sec.
 */

loadState();


setInterval(
    loadState,
    5000
);

</script>

</body>
</html>
"""


# ============================================================
# MAIN PAGE
# ============================================================

@app.route("/")
def index():

    start_workers()

    return render_template_string(
        HTML
    )


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    start_workers()

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )


    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
    )
