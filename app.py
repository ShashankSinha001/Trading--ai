from flask import Flask, Response, jsonify, render_template_string
import os
import json
import time
import threading
import queue
import requests
import websocket

from signal_engine import Candle, generate_signal, signal_to_dict

app = Flask(__name__)

# =========================================================
# CONFIG
# =========================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# Current Twelve Data plan does not provide WTI/USD.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

# =========================================================
# TIMEFRAME REFRESH POLICY
# =========================================================
#
# IMPORTANT:
# We do NOT request every timeframe every minute.
#
# This reduces Twelve Data HTTP rate-limit pressure (429).
# Existing candle data is reused between refreshes.
#
# The first request for each timeframe is staggered so that
# the application does not create a burst of 9 HTTP requests.
#

TIMEFRAME_CONFIG = {
    "1min": {
        "refresh": 60,
        "initial_delay": 5,
    },

    "5min": {
        "refresh": 120,
        "initial_delay": 12,
    },

    "15min": {
        "refresh": 300,
        "initial_delay": 19,
    },

    "30min": {
        "refresh": 600,
        "initial_delay": 26,
    },

    "1h": {
        "refresh": 900,
        "initial_delay": 33,
    },

    "4h": {
        "refresh": 1800,
        "initial_delay": 40,
    },

    "1day": {
        "refresh": 3600,
        "initial_delay": 47,
    },

    "1week": {
        "refresh": 7200,
        "initial_delay": 54,
    },

    "1month": {
        "refresh": 14400,
        "initial_delay": 61,
    },
}

CANDLE_INTERVALS = list(TIMEFRAME_CONFIG.keys())

CANDLE_OUTPUTSIZE = 100

# Minimum time between HTTP requests even if something
# triggers the analysis loop repeatedly.
MIN_HTTP_GAP = 4.0

# =========================================================
# GLOBAL STATE
# =========================================================

lock = threading.RLock()

workers_started = False

clients = []
clients_lock = threading.Lock()

# Candle cache metadata.
candle_cache = {}

for _interval, _config in TIMEFRAME_CONFIG.items():
    candle_cache[_interval] = {
        "candles": [],
        "last_fetch": 0.0,
        "next_fetch": time.time() + _config["initial_delay"],
        "backoff_until": 0.0,
        "failures": 0,
        "last_status": None,
    }

last_http_request = 0.0


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
            "1min": {},
            "5min": {},
            "15min": {},
            "30min": {},
            "1h": {},
            "4h": {},
            "1day": {},
            "1week": {},
            "1month": {}
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

        "ai_reasons": [],
        "ai_warnings": [],

        "breakout": "UNKNOWN",
        "liquidity_analysis": "UNKNOWN",

        "data_status": "STARTING",
        "data_message": "Waiting for market candle data.",

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


def safe_round(value, digits=3):
    try:
        return round(float(value), digits)
    except Exception:
        return None


def broadcast():

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

    result = sum(
        values[:period]
    ) / period

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

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


# =========================================================
# TWELVE DATA CANDLE REQUEST
# =========================================================

def get_candles(
    symbol,
    interval,
    outputsize=100
):

    global last_http_request

    if not API_KEY:

        print(
            "ERROR: TWELVE_DATA_API_KEY missing"
        )

        return []


    # -----------------------------------------------------
    # Global request spacing
    # -----------------------------------------------------

    now = time.time()

    elapsed = (
        now - last_http_request
    )

    if elapsed < MIN_HTTP_GAP:

        time.sleep(
            MIN_HTTP_GAP - elapsed
        )

    last_http_request = time.time()


    try:

        response = requests.get(

            "https://api.twelvedata.com/time_series",

            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": API_KEY
            },

            timeout=15
        )


        status = response.status_code


        # -------------------------------------------------
        # RATE LIMIT
        # -------------------------------------------------

        if status == 429:

            print(
                "CANDLE RATE LIMIT 429:",
                interval
            )

            return []


        if status != 200:

            print(
                "CANDLE HTTP ERROR:",
                interval,
                status
            )

            return []


        data = response.json()


        # -------------------------------------------------
        # Twelve Data API error
        # -------------------------------------------------

        if data.get("status") == "error":

            print(
                "CANDLE API ERROR:",
                interval,
                data.get("message")
            )

            return []


        values = data.get(
            "values",
            []
        )


        if not values:

            return []


        values = list(
            reversed(values)
        )


        return values


    except Exception as exc:

        print(
            "CANDLE ERROR:",
            interval,
            repr(exc)
        )

        return []


# =========================================================
# CACHE / RATE LIMIT MANAGER
# =========================================================

def refresh_one_timeframe(interval):

    config = TIMEFRAME_CONFIG[
        interval
    ]

    cache = candle_cache[
        interval
    ]

    now = time.time()


    # Not due yet.
    if now < cache["next_fetch"]:
        return False


    # Backoff after previous 429/error.
    if now < cache["backoff_until"]:
        return False


    print(
        "CANDLE FETCH:",
        interval
    )


    candles = get_candles(
        GOLD_SYMBOL,
        interval,
        CANDLE_OUTPUTSIZE
    )


    # -----------------------------------------------------
    # Successful request
    # -----------------------------------------------------

    if candles:

        cache["candles"] = candles

        cache["last_fetch"] = now

        cache["failures"] = 0

        cache["last_status"] = 200

        cache["backoff_until"] = 0

        cache["next_fetch"] = (
            now
            + config["refresh"]
        )

        print(
            "CANDLE UPDATED:",
            interval,
            "count=",
            len(candles)
        )

        return True


    # -----------------------------------------------------
    # Failed request
    # -----------------------------------------------------

    cache["failures"] += 1

    cache["last_status"] = 429

    failure_count = min(
        cache["failures"],
        5
    )

    # Progressive backoff:
    # 30s -> 60s -> 120s -> 240s -> 300s
    backoff = min(
        30 * (2 ** (failure_count - 1)),
        300
    )

    cache["backoff_until"] = (
        now + backoff
    )

    # Keep existing data alive.
    cache["next_fetch"] = (
        now + backoff
    )

    print(
        "CANDLE RETRY DELAY:",
        interval,
        f"{backoff}s"
    )

    return False


# =========================================================
# ANALYZE ONE TIMEFRAME
# =========================================================

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


    # -----------------------------------------------------
    # Trend
    # -----------------------------------------------------

    if (
        ema20_value is not None
        and ema50_value is not None
    ):

        if (
            current
            > ema20_value
            > ema50_value
        ):

            trend = "BULLISH"

        elif (
            current
            < ema20_value
            < ema50_value
        ):

            trend = "BEARISH"

        else:

            trend = "NEUTRAL"

    else:

        trend = "NEUTRAL"


    # -----------------------------------------------------
    # Momentum
    # -----------------------------------------------------

    if current_rsi is None:

        momentum = "NEUTRAL"

    elif current_rsi >= 55:

        momentum = "BUYING"

    elif current_rsi <= 45:

        momentum = "SELLING"

    else:

        momentum = "NEUTRAL"


    # -----------------------------------------------------
    # Structure
    # -----------------------------------------------------

    recent = closes[-10:]

    if len(recent) >= 10:

        old_average = (
            sum(recent[:5])
            / 5
        )

        new_average = (
            sum(recent[-5:])
            / 5
        )

        if new_average > old_average:

            structure = "HIGHER"

        elif new_average < old_average:

            structure = "LOWER"

        else:

            structure = "RANGE"

    else:

        structure = "RANGE"


    # -----------------------------------------------------
    # Liquidity
    # -----------------------------------------------------

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


# =========================================================
# CONVERT DATA FOR SIGNAL ENGINE
# =========================================================

def convert_to_engine_candles(
    candles
):

    result = []

    for candle in candles:

        try:

            result.append(

                Candle(

                    open=float(
                        candle.get(
                            "open",
                            0
                        )
                    ),

                    high=float(
                        candle.get(
                            "high",
                            0
                        )
                    ),

                    low=float(
                        candle.get(
                            "low",
                            0
                        )
                    ),

                    close=float(
                        candle.get(
                            "close",
                            0
                        )
                    ),

                    volume=float(
                        candle.get(
                            "volume",
                            0
                        )
                        or 0
                    )
                )
            )

        except Exception:

            continue

    return result


# =========================================================
# MULTI-FACTOR AI ENGINE
# =========================================================

def build_ai_analysis(
    timeframes,
    live_price
):

    five_raw = (
        timeframes
        .get("5min", {})
        .get("candles", [])
    )

    fifteen_raw = (
        timeframes
        .get("15min", {})
        .get("candles", [])
    )

    one_hour_raw = (
        timeframes
        .get("1h", {})
        .get("candles", [])
    )


    five = convert_to_engine_candles(
        five_raw
    )

    fifteen = convert_to_engine_candles(
        fifteen_raw
    )

    one_hour = convert_to_engine_candles(
        one_hour_raw
    )


    # -----------------------------------------------------
    # Data quality gate
    # -----------------------------------------------------

    if len(five) < 60:

        return {

            "signal": "WAIT",

            "confidence": 25,

            "score": 0,

            "reasons": [
                "Waiting for enough 5m market data."
            ],

            "warnings": [
                "Multi-factor engine needs at least "
                "60 candles."
            ],

            "trend": "UNKNOWN",

            "momentum": "UNKNOWN",

            "structure": "UNKNOWN",

            "liquidity": "UNKNOWN",

            "breakout": "UNKNOWN"
        }


    # -----------------------------------------------------
    # 5m primary engine
    # -----------------------------------------------------

    result = generate_signal(

        candles=five,

        higher_timeframe_candles=(
            fifteen
            if len(fifteen) >= 60
            else None
        )
    )


    data = signal_to_dict(
        result
    )


    # -----------------------------------------------------
    # 1H confirmation
    # -----------------------------------------------------

    if len(one_hour) >= 60:

        one_hour_result = generate_signal(
            candles=one_hour
        )

        higher_trend = (
            one_hour_result.trend
        )


        if (
            data["decision"] == "BUY"
            and higher_trend == "BEARISH"
        ):

            data["decision"] = "WAIT"

            data["confidence"] = min(
                data["confidence"],
                60
            )

            data["warnings"].append(
                "1H trend conflicts with BUY setup."
            )


        elif (
            data["decision"] == "SELL"
            and higher_trend == "BULLISH"
        ):

            data["decision"] = "WAIT"

            data["confidence"] = min(
                data["confidence"],
                60
            )

            data["warnings"].append(
                "1H trend conflicts with SELL setup."
            )


    return {

        "signal":
            data["decision"],

        "confidence":
            data["confidence"],

        "score":
            data["score"],

        "reasons":
            data["reasons"],

        "warnings":
            data["warnings"],

        "trend":
            data["trend"],

        "momentum":
            data["momentum"],

        "structure":
            data["structure"],

        "liquidity":
            data["liquidity"],

        "breakout":
            data["breakout"]
    }


# =========================================================
# TRADE PLAN
# =========================================================

def build_trade_plan(
    timeframes,
    live_price,
    ai
):

    if (
        live_price is None
        or ai["signal"] == "WAIT"
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

            "reason":
                ai.get(
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
        or fifteen.get("support")
    )

    resistance = (
        five.get("resistance")
        or fifteen.get("resistance")
    )


    entry = float(
        live_price
    )


    if ai["signal"] == "SELL":

        stop = (
            resistance
            if (
                resistance
                and resistance > entry
            )
            else entry * 1.005
        )

        risk = max(
            stop - entry,
            entry * 0.001
        )

        t1 = entry - risk
        t2 = entry - 2 * risk
        t3 = entry - 3 * risk


    else:

        stop = (
            support
            if (
                support
                and support < entry
            )
            else entry * 0.995
        )

        risk = max(
            entry - stop,
            entry * 0.001
        )

        t1 = entry + risk
        t2 = entry + 2 * risk
        t3 = entry + 3 * risk


    return {

        "signal":
            ai["signal"],

        "entry":
            round(
                entry,
                3
            ),

        "stop_loss":
            round(
                stop,
                3
            ),

        "target_1":
            round(
                t1,
                3
            ),

        "target_2":
            round(
                t2,
                3
            ),

        "target_3":
            round(
                t3,
                3
            ),

        "risk_reward":
            "1:1 / 1:2 / 1:3",

        "invalidation":
            round(
                stop,
                3
            ),

        "reason":
            ai.get(
                "reasons",
                []
            )
    }


# =========================================================
# SIGNAL HISTORY
# =========================================================

def record_signal_history(
    ai,
    live_price
):

    with lock:

        history = (
            state["gold"]
            .setdefault(
                "signal_history",
                []
            )
        )


        previous = (
            history[-1]
            if history
            else None
        )


        if (
            previous
            and previous.get("score")
            == ai["score"]

            and previous.get("signal")
            == ai["signal"]
        ):

            return


        history.append(

            {

                "time":
                    now_text(),

                "price":
                    (
                        round(
                            float(
                                live_price
                            ),
                            3
                        )
                        if live_price is not None
                        else None
                    ),

                "score":
                    ai["score"],

                "signal":
                    ai["signal"],

                "confidence":
                    ai["confidence"],

                "reasons":
                    ai.get(
                        "reasons",
                        []
                    )[:8]
            }
        )


        state["gold"][
            "signal_history"
        ] = history[-50:]


# =========================================================
# UPDATE ANALYSIS FROM CACHE
# =========================================================

def build_cached_timeframes():

    timeframe_data = {}

    for interval in CANDLE_INTERVALS:

        cache = candle_cache[
            interval
        ]

        candles = cache.get(
            "candles",
            []
        )

        if not candles:
            continue

        result = analyze_timeframe(
            candles
        )

        if result:

            result["candles"] = (
                candles[-120:]
            )

            timeframe_data[
                interval
            ] = result

    return timeframe_data


# =========================================================
# GOLD ANALYSIS LOOP
# =========================================================

def gold_analysis_loop():

    while True:

        try:

            # -------------------------------------------------
            # Refresh only due timeframes.
            # -------------------------------------------------

            refreshed = False

            for interval in CANDLE_INTERVALS:

                try:

                    changed = (
                        refresh_one_timeframe(
                            interval
                        )
                    )

                    if changed:
                        refreshed = True

                except Exception as exc:

                    print(
                        "TIMEFRAME REFRESH ERROR:",
                        interval,
                        repr(exc)
                    )


            # -------------------------------------------------
            # Build analysis from cached data.
            # -------------------------------------------------

            timeframe_data = (
                build_cached_timeframes()
            )


            if timeframe_data:

                with lock:

                    state["gold"][
                        "timeframes"
                    ] = timeframe_data


                with lock:

                    live_price = (
                        state["gold"]["price"]
                    )


                if live_price is None:

                    live_price = (

                        timeframe_data
                        .get(
                            "5min",
                            {}
                        )
                        .get(
                            "price"
                        )
                    )


                # -------------------------------------------------
                # Need 5m data before producing AI signal.
                # -------------------------------------------------

                if (
                    "5min" not in timeframe_data
                    or len(
                        timeframe_data[
                            "5min"
                        ].get(
                            "candles",
                            []
                        )
                    ) < 60
                ):

                    with lock:

                        state["gold"][
                            "data_status"
                        ] = "LOADING"

                        state["gold"][
                            "data_message"
                        ] = (
                            "Waiting for enough "
                            "5m candles."
                        )

                        state["gold"][
                            "signal"
                        ] = "WAIT"

                        state["gold"][
                            "confidence"
                        ] = 25

                    broadcast()

                elif live_price is not None:

                    # -------------------------------------------------
                    # AI analysis
                    # -------------------------------------------------

                    ai = build_ai_analysis(

                        timeframe_data,

                        live_price
                    )


                    five = (
                        timeframe_data
                        .get(
                            "5min",
                            {}
                        )
                    )


                    trade_plan = (
                        build_trade_plan(

                            timeframe_data,

                            live_price,

                            ai
                        )
                    )


                    record_signal_history(
                        ai,
                        live_price
                    )


                    with lock:

                        state["gold"][
                            "candles"
                        ] = {

                            k: v.get(
                                "candles",
                                []
                            )

                            for k, v
                            in timeframe_data.items()
                        }


                        state["gold"][
                            "trade_plan"
                        ] = trade_plan


                        state["gold"].update(

                            {

                                "trend":
                                    five.get(
                                        "trend",
                                        "NEUTRAL"
                                    ),

                                "momentum":
                                    five.get(
                                        "momentum",
                                        "NEUTRAL"
                                    ),

                                "structure":
                                    five.get(
                                        "structure",
                                        "RANGE"
                                    ),

                                "rsi":
                                    five.get(
                                        "rsi"
                                    ),

                                "ema20":
                                    five.get(
                                        "ema20"
                                    ),

                                "ema50":
                                    five.get(
                                        "ema50"
                                    ),

                                "support":
                                    five.get(
                                        "support"
                                    ),

                                "resistance":
                                    five.get(
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

                                "sweep":
                                    five.get(
                                        "sweep",
                                        "NONE"
                                    ),

                                "signal":
                                    ai[
                                        "signal"
                                    ],

                                "confidence":
                                    ai[
                                        "confidence"
                                    ],

                                "score":
                                    ai[
                                        "score"
                                    ],

                                "ai_reasons":
                                    ai.get(
                                        "reasons",
                                        []
                                    ),

                                "ai_warnings":
                                    ai.get(
                                        "warnings",
                                        []
                                    ),

                                "breakout":
                                    ai.get(
                                        "breakout",
                                        "UNKNOWN"
                                    ),

                                "liquidity_analysis":
                                    ai.get(
                                        "liquidity",
                                        "UNKNOWN"
                                    ),

                                "data_status":
                                    "LIVE",

                                "data_message":
                                    (
                                        "Using cached "
                                        "multi-timeframe "
                                        "market data."
                                    ),

                                "updated":
                                    now_text(),

                                "error":
                                    None
                            }
                        )


                    print(

                        "AI UPDATE:",

                        f"price={live_price}",

                        f"score={ai['score']}",

                        f"decision={ai['signal']}",

                        f"confidence={ai['confidence']}%",

                        f"cached_timeframes={len(timeframe_data)}"
                    )


                    broadcast()


        except Exception as exc:

            print(
                "AI LOOP ERROR:",
                repr(exc)
            )

            with lock:

                state["gold"][
                    "error"
                ] = str(exc)

                state["gold"][
                    "data_status"
                ] = "DEGRADED"

                state["gold"][
                    "data_message"
                ] = (
                    "Using last valid market data."
                )

            broadcast()


        # -----------------------------------------------------
        # The loop itself is lightweight.
        # Actual HTTP requests are controlled by the
        # timeframe cache.
        # -----------------------------------------------------

        time.sleep(5)


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_websocket_loop():

    reconnect_delay = 5

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


            ws = websocket.create_connection(

                WS_URL
                + "?apikey="
                + API_KEY,

                timeout=20
            )


            subscribe = {

                "action":
                    "subscribe",

                "params": {

                    "symbols":
                        GOLD_SYMBOL
                }
            }


            ws.send(
                json.dumps(
                    subscribe
                )
            )


            print(
                "TWELVE DATA GOLD SUBSCRIBE SENT"
            )


            with lock:

                state["gold"][
                    "connection"
                ] = "CONNECTED"

                state["gold"][
                    "error"
                ] = None


            broadcast()


            while True:

                raw = ws.recv()


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


                print(
                    "GOLD WS MESSAGE:",
                    message
                )


                event = message.get(
                    "event"
                )


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
                        and price is not None
                    ):

                        update_gold_price(
                            price
                        )


                elif event == "subscribe-status":

                    print(
                        "GOLD SUBSCRIBE STATUS:",
                        message
                    )


                elif event == "heartbeat":

                    pass


                elif event == "error":

                    print(
                        "GOLD WS ERROR:",
                        message
                    )


        except Exception as exc:

            print(
                "GOLD WEBSOCKET ERROR:",
                repr(exc)
            )


            with lock:

                state["gold"][
                    "connection"
                ] = "RECONNECTING"

                state["gold"][
                    "error"
                ] = str(exc)


            broadcast()


        finally:

            try:

                if ws is not None:
                    ws.close()

            except Exception:
                pass


        print(
            f"Gold WebSocket reconnecting "
            f"in {reconnect_delay}s..."
        )


        time.sleep(
            reconnect_delay
        )


# =========================================================
# START WORKERS
# =========================================================

def start_workers():

    global workers_started

    with lock:

        if workers_started:
            return

        workers_started = True


    print(
        "Starting Trading-AI workers..."
    )


    ws_thread = threading.Thread(

        target=gold_websocket_loop,

        daemon=True,

        name="GoldWebSocket"
    )

    ws_thread.start()


    ai_thread = threading.Thread(

        target=gold_analysis_loop,

        daemon=True,

        name="GoldAnalysis"
    )

    ai_thread.start()


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    start_workers()

    return render_template_string(
        HTML
    )


# =========================================================
# API
# =========================================================

@app.route("/api/market")
def api_market():

    start_workers()

    with lock:

        data = json.loads(
            json.dumps(state)
        )

    return jsonify(data)


# =========================================================
# SSE STREAM
# =========================================================

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
                    )
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


        finally:

            with clients_lock:

                if client_queue in clients:

                    clients.remove(
                        client_queue
                    )


    return Response(

        generate(),

        mimetype=
            "text/event-stream",

        headers={

            "Cache-Control":
                "no-cache",

            "Connection":
                "keep-alive",

            "X-Accel-Buffering":
                "no"
        }
    )


# =========================================================
# HEALTH
# =========================================================

@app.route("/health")
def health():

    return jsonify(

        {

            "status":
                "ok",

            "service":
                "Trading-AI",

            "gold_connection":
                state["gold"][
                    "connection"
                ],

            "gold_price":
                state["gold"][
                    "price"
                ],

            "data_status":
                state["gold"][
                    "data_status"
                ],

            "time":
                now_text()
        }
    )


# =========================================================
# DASHBOARD
# =========================================================

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
font-family:Arial;
color:white;
background:
radial-gradient(
circle at top,
#183d75,
#08172f 45%,
#020817
)
}

.page{
width:94%;
max-width:1280px;
margin:auto;
padding:22px 0 45px
}

.header{
text-align:center;
margin-bottom:20px
}

.header h1{
margin:0;
font-size:30px
}

.header p{
margin:7px 0;
color:#7fa9e8
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
0 18px 45px rgba(0,0,0,.32)
}

.asset{
font-size:20px;
font-weight:700
}

.price{
margin-top:8px;
font-size:38px;
font-weight:700
}

.live{
margin-top:4px;
color:#2be48e;
font-size:13px;
font-weight:700
}

.connection{
margin-top:4px;
color:#719bd4;
font-size:11px
}

.metrics{
display:grid;
grid-template-columns:repeat(6,1fr);
gap:8px;
margin-top:18px
}

.metric{
padding:12px;
min-height:68px;
border-radius:11px;
background:rgba(11,35,70,.72);
border:1px solid rgba(100,150,220,.13)
}

.label{
color:#78a5e5;
font-size:10px;
margin-bottom:6px
}

.value{
font-size:15px;
font-weight:700
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
)
}

.analysis-label{
color:#82b0f5;
font-size:10px;
font-weight:700
}

.signal{
margin-top:4px;
font-size:27px;
font-weight:800
}

.confidence,
.score{
margin-top:5px;
font-size:14px;
font-weight:700
}

.toolbar{
display:flex;
gap:7px;
flex-wrap:wrap;
margin:15px 0 10px
}

.tf{
border:1px solid #38679e;
background:#092447;
color:#bcd8ff;
padding:8px 12px;
border-radius:8px;
cursor:pointer;
font-weight:700
}

.tf.active{
background:#1a64ad;
color:white
}

.chart{
height:430px;
border-radius:12px;
overflow:hidden;
border:1px solid rgba(100,150,220,.16)
}

.grid2{
display:grid;
grid-template-columns:1.3fr .7fr;
gap:14px;
margin-top:14px
}

.panel{
padding:16px;
border-radius:13px;
background:rgba(4,17,37,.55);
border:1px solid rgba(100,150,220,.13)
}

.panel h3{
margin:0 0 12px;
font-size:14px;
color:#9fc5f5
}

.tradegrid{
display:grid;
grid-template-columns:repeat(4,1fr);
gap:8px
}

.tradebox{
padding:11px;
border-radius:9px;
background:#081d39
}

.tradebox b{
display:block;
font-size:14px;
margin-top:4px
}

.history{
max-height:250px;
overflow:auto
}

.row{
display:grid;
grid-template-columns:
75px 80px 55px 70px 1fr;
gap:7px;
padding:8px 0;
border-bottom:
1px solid rgba(130,170,220,.1);
font-size:11px
}

.reason{
color:#8eafd8
}

.warning{
margin-top:8px;
padding:9px;
border-radius:8px;
background:rgba(150,90,20,.2);
color:#ffd48a;
font-size:11px
}

.error{
margin-top:12px;
color:#ffb3b3;
font-size:12px
}

.status{
margin-top:8px;
font-size:11px;
color:#78a5e5
}

.footer{
text-align:center;
color:#6489ba;
font-size:11px;
margin-top:15px
}

@media(max-width:900px){

.metrics{
grid-template-columns:repeat(3,1fr)
}

.grid2{
grid-template-columns:1fr
}

.tradegrid{
grid-template-columns:repeat(2,1fr)
}

}

@media(max-width:480px){

.page{
width:96%
}

.price{
font-size:31px
}

.metrics{
grid-template-columns:repeat(2,1fr)
}

.chart{
height:330px
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
Multi-Factor Market Analysis
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


function fmt(v,d=3){

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
"30min":"30m",
"1h":"1H",
"4h":"4H",
"1day":"1D",
"1week":"1W",
"1month":"1M"

}[x]||x;

}


function metric(a,b){

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


function box(a,b){

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

const el=
document.getElementById(
"price-chart"
);

if(!el)
return;

if(chart)
chart.remove();

chart=
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

const s=
chart.addCandlestickSeries(
{
upColor:"#20c997",
downColor:"#ff5c73",
borderVisible:false,
wickUpColor:"#20c997",
wickDownColor:"#ff5c73"
}
);

const d=
(c||[])
.map(
x=>({

time:
Math.floor(
new Date(
x.datetime||x.date
).getTime()/1000
),

open:+x.open,
high:+x.high,
low:+x.low,
close:+x.close

})
)
.filter(
x=>
Number.isFinite(x.time)
&&
[
x.open,
x.high,
x.low,
x.close
].every(
Number.isFinite
)
);

s.setData(d);

chart.timeScale().fitContent();

if(resizeObserver)
resizeObserver.disconnect();

resizeObserver=
new ResizeObserver(
()=>
chart.applyOptions(
{
width:
el.clientWidth
}
)
);

resizeObserver.observe(el);

}


function hist(h){

if(!h||!h.length)

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
${x.time}
</span>

<span>
${fmt(x.price)}
</span>

<b>
${x.score>0?"+":""}${x.score}
</b>

<span>
${x.signal}
</span>

<span class="reason">
${(x.reasons||[])
.slice(0,3)
.join(" • ")}
</span>

</div>

`
)
.join("");

}


function warningsHtml(warnings){

if(
!warnings ||
!warnings.length
)
return "";

return warnings
.map(
w=>
`
<div class="warning">
⚠ ${w}
</div>
`
)
.join("");

}


function render(data){

latestData=data;

let g=data.gold;

let p=
g.trade_plan||{};


document.getElementById(
"gold"
).innerHTML=

`

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
${g.connection||"CONNECTING"}
•
Updated
${g.updated||"—"}
</div>

<div class="status">
Data:
${g.data_status||"STARTING"}
•
${g.data_message||""}
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
g.sweep||"NONE"
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
${g.signal||"WAIT"}
</div>

<div class="confidence">
Confidence:
${g.confidence||0}%
</div>

<div class="score">
AI SCORE:
${g.score??0}
</div>

<div class="reason"
style="margin-top:10px">

<b>
Engine:
</b>

${g.breakout||"UNKNOWN"}

•

${g.liquidity_analysis||"UNKNOWN"}

</div>

${warningsHtml(
g.ai_warnings
)}

</div>


<div class="toolbar">

${
[
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
.map(
x=>
`
<button
class="tf ${activeTF===x?"active":""}"
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
p.risk_reward||"—"
)}

${box(
"INVALIDATION",
fmt(p.invalidation)
)}

${box(
"SETUP",
p.signal||"WAIT"
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


<div
class="reason"
style="margin-top:12px"
>

<b>
Multi-Factor Evidence:
</b>

${
(g.ai_reasons||[])
.join(" • ")
||
"Waiting for enough evidence."
}

</div>


${warningsHtml(
g.ai_warnings
)}

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
).innerHTML=

`

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
${data.oil.connection||"UNAVAILABLE"}
</div>

<div class="error">
${data.oil.error||""}
</div>

</div>

`;


draw(
(g.candles||{})[
activeTF
]||[]
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


const source=
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
});

</script>

</body>

</html>
"""


# =========================================================
# START APP
# =========================================================

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
