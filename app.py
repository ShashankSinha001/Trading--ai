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
# ============================================================

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# WTI intentionally disabled for the current Twelve Data plan.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

REST_PRICE_URL = "https://api.twelvedata.com/price"
REST_TIME_SERIES_URL = "https://api.twelvedata.com/time_series"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

# Keep the number of historical requests low.
# The Basic plan is limited, so we do NOT hammer all 9 timeframes
# continuously.
CANDLE_INTERVALS = [
    "1min",
    "5min",
    "15min",
    "1h",
    "4h",
    "1day"
]

# Minimum time between requests for each timeframe.
FETCH_INTERVALS = {
    "1min": 120,
    "5min": 180,
    "15min": 300,
    "1h": 600,
    "4h": 1200,
    "1day": 1800
}

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

api_backoff_until = 0
api_backoff_lock = threading.Lock()

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

        "data_source": "Twelve Data REST"
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
        return float(value)
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


def set_gold_price(price, source="Twelve Data REST"):
    price = number(price)

    if price is None:
        return

    with lock:
        state["gold"]["price"] = price
        state["gold"]["updated"] = now_text()
        state["gold"]["connection"] = "CONNECTED"
        state["gold"]["data_source"] = source
        state["gold"]["error"] = None

    broadcast()


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
# REST PRICE FALLBACK
# ============================================================

def get_live_price():

    global api_backoff_until

    if not API_KEY:
        print("ERROR: TWELVE_DATA_API_KEY missing")
        return None

    now = time.time()

    with api_backoff_lock:

        if now < api_backoff_until:
            return None

    # Price cache: do not request repeatedly.
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
                "apikey": API_KEY
            },
            timeout=15
        )

        if response.status_code == 429:

            print("PRICE 429 - Twelve Data rate limit")

            with api_backoff_lock:
                api_backoff_until = time.time() + 120

            return None

        if response.status_code != 200:

            print(
                "PRICE HTTP ERROR:",
                response.status_code
            )

            return None

        data = response.json()

        if data.get("status") == "error":

            print(
                "PRICE API ERROR:",
                data.get("message", "")
            )

            return None

        price = number(data.get("price"))

        if price is None:
            return None

        with price_cache_lock:

            price_cache["price"] = price
            price_cache["timestamp"] = time.time()

        return price

    except Exception as exc:

        print(
            "PRICE ERROR:",
            repr(exc)
        )

        return None


# ============================================================
# CANDLE DATA
# ============================================================

def get_candles(
    symbol,
    interval,
    outputsize=100
):

    global api_backoff_until

    if not API_KEY:

        print(
            "ERROR: TWELVE_DATA_API_KEY missing"
        )

        return []

    now = time.time()

    with api_backoff_lock:

        if now < api_backoff_until:
            return []

    cache_key = f"{symbol}:{interval}"

    with candle_cache_lock:

        cached = candle_cache.get(cache_key)

        if cached:

            timestamp = cached.get(
                "timestamp",
                0
            )

            values = cached.get(
                "values",
                []
            )

            refresh_after = FETCH_INTERVALS.get(
                interval,
                300
            )

            if (
                values
                and
                now - timestamp < refresh_after
            ):

                return values

    try:

        response = requests.get(
            REST_TIME_SERIES_URL,
            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": API_KEY
            },
            timeout=15
        )

        if response.status_code == 429:

            print(
                "TWELVE DATA 429 - using cache"
            )

            with api_backoff_lock:

                api_backoff_until = (
                    time.time() + 120
                )

            with candle_cache_lock:

                cached = candle_cache.get(
                    cache_key
                )

                if cached:
                    return cached.get(
                        "values",
                        []
                    )

            return []

        if response.status_code != 200:

            print(
                "CANDLE HTTP ERROR:",
                interval,
                response.status_code
            )

            with candle_cache_lock:

                cached = candle_cache.get(
                    cache_key
                )

                if cached:
                    return cached.get(
                        "values",
                        []
                    )

            return []

        data = response.json()

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

            with candle_cache_lock:

                cached = candle_cache.get(
                    cache_key
                )

                if cached:
                    return cached.get(
                        "values",
                        []
                    )

            return []

        values = data.get(
            "values",
            []
        )

        if not values:

            with candle_cache_lock:

                cached = candle_cache.get(
                    cache_key
                )

                if cached:
                    return cached.get(
                        "values",
                        []
                    )

            return []

        values = list(
            reversed(values)
        )

        with candle_cache_lock:

            candle_cache[cache_key] = {
                "timestamp": time.time(),
                "values": values
            }

        return values

    except Exception as exc:

        print(
            "CANDLE ERROR:",
            interval,
            repr(exc)
        )

        with candle_cache_lock:

            cached = candle_cache.get(
                cache_key
            )

            if cached:
                return cached.get(
                    "values",
                    []
                )

        return []


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

    # Trend
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

    # Momentum
    if current_rsi is None:

        momentum = "NEUTRAL"

    elif current_rsi >= 55:

        momentum = "BUYING"

    elif current_rsi <= 45:

        momentum = "SELLING"

    else:

        momentum = "NEUTRAL"

    # Structure
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

    # Liquidity sweep
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
            round(current_rsi, 2)
            if current_rsi is not None
            else None
        ),

        "ema20": (
            round(ema20_value, 3)
            if ema20_value is not None
            else None
        ),

        "ema50": (
            round(ema50_value, 3)
            if ema50_value is not None
            else None
        ),

        "support": (
            round(support, 3)
            if support is not None
            else None
        ),

        "resistance": (
            round(resistance, 3)
            if resistance is not None
            else None
        ),

        "liquidity_high": (
            round(liquidity_high, 3)
            if liquidity_high is not None
            else None
        ),

        "liquidity_low": (
            round(liquidity_low, 3)
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
            reasons.append(reason)

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

    score = max(
        -10,
        min(10, score)
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
            and fifteen.get("trend") == "BULLISH"
            and one_hour.get("trend") == "BULLISH"
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
            and fifteen.get("trend") == "BEARISH"
            and one_hour.get("trend") == "BEARISH"
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
        or fifteen.get("support")
    )

    resistance = (
        five.get("resistance")
        or fifteen.get("resistance")
    )

    entry = float(live_price)

    if ai["signal"] == "SELL":

        stop = (
            resistance
            if resistance and resistance > entry
            else entry * 1.005
        )

        risk = max(
            stop - entry,
            entry * 0.001
        )

        t1 = entry - risk
        t2 = entry - (2 * risk)
        t3 = entry - (3 * risk)

    else:

        stop = (
            support
            if support and support < entry
            else entry * 0.995
        )

        risk = max(
            entry - stop,
            entry * 0.001
        )

        t1 = entry + risk
        t2 = entry + (2 * risk)
        t3 = entry + (3 * risk)

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

        "risk_reward": "1:1 / 1:2 / 1:3",

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

    with lock:

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
            and previous.get("score") == ai["score"]
            and previous.get("signal") == ai["signal"]
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

            "confidence": ai["confidence"],

            "reasons": ai.get(
                "reasons",
                []
            )[:8]
        })

        state["gold"]["signal_history"] = (
            history[-50:]
        )


# ============================================================
# REST PRICE LOOP
# ============================================================

def gold_price_loop():

    while True:

        try:

            price = get_live_price()

            if price is not None:

                set_gold_price(
                    price,
                    "Twelve Data REST"
                )

                print(
                    "GOLD PRICE:",
                    price
                )

        except Exception as exc:

            print(
                "PRICE LOOP ERROR:",
                repr(exc)
            )

        # Avoid hammering the Basic plan.
        time.sleep(35)


# ============================================================
# ANALYSIS LOOP
# ============================================================

def gold_analysis_loop():

    while True:

        try:

            timeframe_data = {}

            for interval in CANDLE_INTERVALS:

                candles = get_candles(
                    GOLD_SYMBOL,
                    interval,
                    100
                )

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

                # Small pause between requests.
                time.sleep(1)

            if not timeframe_data:

                with lock:

                    timeframe_data = dict(
                        state["gold"].get(
                            "timeframes",
                            {}
                        )
                    )

            if timeframe_data:

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

                if live_price is not None:

                    ai = build_ai_analysis(
                        timeframe_data,
                        live_price
                    )

                    phase = build_market_phase(
                        timeframe_data,
                        ai
                    )

                    trade_plan = build_trade_plan(
                        timeframe_data,
                        live_price,
                        ai
                    )

                    five = timeframe_data.get(
                        "5min",
                        {}
                    )

                    with lock:

                        state["gold"]["timeframes"] = (
                            timeframe_data
                        )

                        state["gold"]["candles"] = {
                            k: v.get(
                                "candles",
                                []
                            )
                            for k, v
                            in timeframe_data.items()
                        }

                        state["gold"]["trade_plan"] = (
                            trade_plan
                        )

                        state["gold"]["market_phase"] = (
                            phase
                        )

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

                            "liquidity_high": five.get(
                                "liquidity_high"
                            ),

                            "liquidity_low": five.get(
                                "liquidity_low"
                            ),

                            "sweep": five.get(
                                "sweep",
                                "NONE"
                            ),

                            "signal": ai[
                                "signal"
                            ],

                            "confidence": ai[
                                "confidence"
                            ],

                            "score": ai[
                                "score"
                            ],

                            "updated": now_text(),

                            "error": None
                        })

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

        except Exception as exc:

            print(
                "AI LOOP ERROR:",
                repr(exc)
            )

            with lock:

                state["gold"]["error"] = str(
                    exc
                )

            broadcast()

        # Analysis every 3 minutes.
        time.sleep(180)


# ============================================================
# OPTIONAL WEBSOCKET
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
                "Trying Twelve Data Gold WebSocket..."
            )

            # IMPORTANT:
            # API key comes from Render environment.
            ws_url = (
                WS_URL
                + "?apikey="
                + API_KEY
            )

            ws = websocket.create_connection(
                ws_url,
                timeout=20
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
                "TWELVE DATA GOLD SUBSCRIBE SENT"
            )

            with lock:

                state["gold"]["connection"] = (
                    "CONNECTED"
                )

                state["gold"]["error"] = None

            broadcast()

            last_heartbeat = time.time()

            while True:

                # Twelve Data recommends heartbeat
                # messages to keep the connection alive.
                if (
                    time.time()
                    - last_heartbeat
                    > 10
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

                        pass

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

                        set_gold_price(
                            price,
                            "Twelve Data WebSocket"
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
                        "GOLD WS SERVER ERROR:",
                        message
                    )

        except Exception as exc:

            error_text = str(exc)

            print(
                "GOLD WEBSOCKET ERROR:",
                repr(exc)
            )

            # The REST price loop continues to provide
            # market data even when WS authentication fails.
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
        "Starting Trading-AI workers..."
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

        data = json.loads(
            json.dumps(state)
        )

    return jsonify(data)


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
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
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

    return jsonify({

        "status": "ok",

        "service": "Trading-AI",

        "gold_connection":
            gold_connection,

        "gold_price":
            gold_price,

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
