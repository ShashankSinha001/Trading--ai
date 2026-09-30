import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, Response


# ============================================================
# CONFIG
# ============================================================

APP_NAME = "Trading AI"

SYMBOL = "XAU/USD"

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

REST_URL = "https://api.twelvedata.com"
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

MAX_CANDLES = 500

REST_PRICE_INTERVAL = 600          # 10 minutes
HISTORY_REFRESH_INTERVAL = 1800    # 30 minutes
DAILY_REFRESH_INTERVAL = 21600     # 6 hours

WS_RECONNECT_DELAY = 60
WS_PROVIDER_ERROR_DELAY = 1800     # 30 minutes

HEARTBEAT_INTERVAL = 10


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# THREAD-SAFE STATE
# ============================================================

state_lock = threading.RLock()

state = {
    "symbol": SYMBOL,
    "price": None,
    "timestamp": None,

    "ws_connected": False,
    "ws_subscribed": False,
    "ws_error": None,

    "rest_available": True,
    "rest_cooldown_until": 0,

    "history_loaded": False,

    "candles": {
        "1min": deque(maxlen=MAX_CANDLES),
        "5min": deque(maxlen=MAX_CANDLES),
        "15min": deque(maxlen=MAX_CANDLES),
        "30min": deque(maxlen=MAX_CANDLES),
        "1h": deque(maxlen=MAX_CANDLES),
        "4h": deque(maxlen=MAX_CANDLES),
        "1day": deque(maxlen=MAX_CANDLES),
    },

    "analysis": {
        "trend": "WAIT",
        "momentum": "WAIT",
        "structure": "WAIT",
        "rsi": None,
        "ema20": None,
        "ema50": None,
        "atr": None,
        "liquidity_high": None,
        "liquidity_low": None,
        "sweep": "NONE",
        "signal": "WAIT",
        "signal_strength": 0,
        "entry": None,
        "stop_loss": None,
        "target_1": None,
        "target_2": None,
        "risk_reward": None,
        "updated": None,
    },

    "history": deque(maxlen=100),
}


# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_timestamp():
    return int(time.time())


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def candle_time(timestamp):
    return int(timestamp)


def add_history(message):
    with state_lock:
        state["history"].appendleft({
            "time": utc_now().isoformat(),
            "message": str(message),
        })


def set_ws_error(message):
    with state_lock:
        state["ws_error"] = str(message)


def clear_ws_error():
    with state_lock:
        state["ws_error"] = None


# ============================================================
# REST COOLDOWN
# ============================================================

def next_utc_midnight_timestamp():
    now = utc_now()
    tomorrow = now.date().fromordinal(now.date().toordinal() + 1)

    midnight = datetime(
        tomorrow.year,
        tomorrow.month,
        tomorrow.day,
        tzinfo=timezone.utc,
    )

    return midnight.timestamp()


def activate_rest_cooldown():
    cooldown = next_utc_midnight_timestamp()

    with state_lock:
        state["rest_available"] = False
        state["rest_cooldown_until"] = cooldown

    add_history("REST API paused until next UTC day because of rate limit.")


def check_rest_cooldown():

    with state_lock:
        available = state["rest_available"]
        cooldown_until = state["rest_cooldown_until"]

    if available:
        return True

    if time.time() >= cooldown_until:
        with state_lock:
            state["rest_available"] = True
            state["rest_cooldown_until"] = 0

        add_history("REST API cooldown ended.")
        return True

    return False


# ============================================================
# TWELVE DATA REST
# ============================================================

def td_get(endpoint, params=None):

    if not API_KEY:
        add_history("TWELVE_DATA_API_KEY is missing.")
        return None

    if not check_rest_cooldown():
        return None

    request_params = dict(params or {})
    request_params["apikey"] = API_KEY

    url = f"{REST_URL}/{endpoint}"

    try:

        response = requests.get(
            url,
            params=request_params,
            timeout=20,
        )

        if response.status_code == 429:
            add_history("Twelve Data REST 429 rate limit.")
            activate_rest_cooldown()
            return None

        response.raise_for_status()

        data = response.json()

        if isinstance(data, dict):

            if data.get("status") == "error":
                message = data.get("message", "Unknown Twelve Data error")

                add_history(
                    f"Twelve Data REST error: {message}"
                )

                message_lower = str(message).lower()

                if (
                    "limit" in message_lower
                    or "credit" in message_lower
                    or "quota" in message_lower
                    or "429" in message_lower
                ):
                    activate_rest_cooldown()

                return None

        return data

    except requests.RequestException as exc:

        add_history(
            f"REST request error: {type(exc).__name__}"
        )

        return None

    except ValueError:

        add_history("REST returned invalid JSON.")
        return None


# ============================================================
# CANDLE NORMALIZATION
# ============================================================

def normalize_rest_candle(item):

    if not isinstance(item, dict):
        return None

    timestamp = item.get("datetime")

    try:
        if isinstance(timestamp, str):

            try:
                dt = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

                timestamp = int(dt.timestamp())

            except ValueError:

                timestamp = int(float(timestamp))

        else:
            timestamp = int(float(timestamp))

    except (TypeError, ValueError):
        return None

    open_price = safe_float(item.get("open"))
    high_price = safe_float(item.get("high"))
    low_price = safe_float(item.get("low"))
    close_price = safe_float(item.get("close"))
    volume = safe_float(item.get("volume"))

    if None in (
        open_price,
        high_price,
        low_price,
        close_price,
    ):
        return None

    return {
        "time": timestamp,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume if volume is not None else 0,
    }


# ============================================================
# HISTORY LOAD
# ============================================================

def load_initial_history():

    if not API_KEY:
        add_history("Cannot load history: API key missing.")
        return

    add_history("Loading 5-minute market history...")

    data = td_get(
        "time_series",
        {
            "symbol": SYMBOL,
            "interval": "5min",
            "outputsize": 500,
            "timezone": "UTC",
        },
    )

    if not data:
        add_history("5-minute history unavailable.")
        return

    values = data.get("values", [])

    candles = []

    for item in values:

        candle = normalize_rest_candle(item)

        if candle:
            candles.append(candle)

    candles.sort(key=lambda x: x["time"])

    with state_lock:

        state["candles"]["5min"].clear()

        for candle in candles[-MAX_CANDLES:]:
            state["candles"]["5min"].append(candle)

    rebuild_all_timeframes()

    with state_lock:
        state["history_loaded"] = len(candles) > 0

    if candles:
        add_history(
            f"Loaded {len(candles)} five-minute candles."
        )
    else:
        add_history("No valid five-minute candles received.")


def load_daily_history():

    if not check_rest_cooldown():
        return

    data = td_get(
        "time_series",
        {
            "symbol": SYMBOL,
            "interval": "1day",
            "outputsize": 250,
            "timezone": "UTC",
        },
    )

    if not data:
        add_history("Daily history unavailable.")
        return

    values = data.get("values", [])

    candles = []

    for item in values:

        candle = normalize_rest_candle(item)

        if candle:
            candles.append(candle)

    candles.sort(key=lambda x: x["time"])

    if candles:

        with state_lock:

            state["candles"]["1day"].clear()

            for candle in candles[-MAX_CANDLES:]:
                state["candles"]["1day"].append(candle)

        add_history(
            f"Loaded {len(candles)} daily candles."
        )


# ============================================================
# CANDLE AGGREGATION
# ============================================================

def aggregate_candles(source, target_minutes):

    with state_lock:
        source_data = list(state["candles"][source])

    if not source_data:
        return []

    bucket_seconds = target_minutes * 60

    result = []

    current = None
    current_bucket = None

    for candle in source_data:

        timestamp = int(candle["time"])

        bucket = (
            timestamp // bucket_seconds
        ) * bucket_seconds

        if current_bucket != bucket:

            if current is not None:
                result.append(current)

            current_bucket = bucket

            current = {
                "time": bucket,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle.get("volume", 0),
            }

        else:

            current["high"] = max(
                current["high"],
                candle["high"],
            )

            current["low"] = min(
                current["low"],
                candle["low"],
            )

            current["close"] = candle["close"]

            current["volume"] += candle.get(
                "volume",
                0,
            )

    if current is not None:
        result.append(current)

    return result[-MAX_CANDLES:]


def rebuild_all_timeframes():

    mappings = [
        ("15min", 15),
        ("30min", 30),
        ("1h", 60),
        ("4h", 240),
    ]

    for target, minutes in mappings:

        candles = aggregate_candles(
            "5min",
            minutes,
        )

        with state_lock:

            state["candles"][target].clear()

            for candle in candles:
                state["candles"][target].append(candle)


# ============================================================
# LIVE TICK -> LOCAL CANDLES
# ============================================================

def update_live_candle(price, timestamp):

    if price is None:
        return

    timestamp = int(timestamp)

    bucket_1m = (timestamp // 60) * 60
    bucket_5m = (timestamp // 300) * 300

    new_5m_bucket = False

    with state_lock:

        # -----------------------------
        # 1 MINUTE
        # -----------------------------

        candles_1m = state["candles"]["1min"]

        if (
            not candles_1m
            or candles_1m[-1]["time"] != bucket_1m
        ):

            candles_1m.append({
                "time": bucket_1m,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0,
            })

        else:

            candle = candles_1m[-1]

            candle["high"] = max(
                candle["high"],
                price,
            )

            candle["low"] = min(
                candle["low"],
                price,
            )

            candle["close"] = price

        # -----------------------------
        # 5 MINUTE
        # -----------------------------

        candles_5m = state["candles"]["5min"]

        if (
            not candles_5m
            or candles_5m[-1]["time"] != bucket_5m
        ):

            candles_5m.append({
                "time": bucket_5m,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0,
            })

            new_5m_bucket = True

        else:

            candle = candles_5m[-1]

            candle["high"] = max(
                candle["high"],
                price,
            )

            candle["low"] = min(
                candle["low"],
                price,
            )

            candle["close"] = price

        state["price"] = price
        state["timestamp"] = timestamp

    if new_5m_bucket:
        rebuild_all_timeframes()


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def process_ws_message(raw):

    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return

    if not isinstance(data, dict):
        return

    event = data.get("event")

    # ------------------------------------------
    # SUBSCRIBE STATUS
    # ------------------------------------------

    if event == "subscribe-status":

        status = str(
            data.get("status", "")
        ).lower()

        if status in (
            "ok",
            "success",
            "subscribed",
        ):

            with state_lock:
                state["ws_subscribed"] = True
                state["ws_error"] = None

            add_history(
                f"WebSocket subscribed to {SYMBOL}."
            )

        else:

            message = (
                data.get("message")
                or data.get("error")
                or str(data)
            )

            with state_lock:
                state["ws_subscribed"] = False
                state["ws_error"] = str(message)

            add_history(
                f"WebSocket subscription rejected: {message}"
            )

        return

    # ------------------------------------------
    # PRICE
    # ------------------------------------------

    if event == "price":

        price = safe_float(data.get("price"))

        if price is None:
            return

        timestamp = data.get("timestamp")

        try:
            timestamp = int(float(timestamp))
        except (TypeError, ValueError):
            timestamp = utc_timestamp()

        with state_lock:
            state["ws_connected"] = True
            state["ws_subscribed"] = True
            state["price"] = price
            state["timestamp"] = timestamp

        clear_ws_error()

        update_live_candle(
            price,
            timestamp,
        )

        return

    # ------------------------------------------
    # ERROR
    # ------------------------------------------

    if event == "error":

        message = (
            data.get("message")
            or data.get("error")
            or str(data)
        )

        set_ws_error(message)

        add_history(
            f"WebSocket error: {message}"
        )


# ============================================================
# WEBSOCKET ERROR CLASSIFICATION
# ============================================================

def is_provider_access_error(message):

    text = str(message).lower()

    keywords = [
        "401",
        "apikey",
        "api key",
        "unauthorized",
        "permission",
        "not authorized",
        "not available",
        "symbol",
        "subscription",
        "plan",
        "trial",
        "websocket",
    ]

    return any(
        keyword in text
        for keyword in keywords
    )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker():

    if not API_KEY:

        add_history(
            "WebSocket disabled: TWELVE_DATA_API_KEY missing."
        )

        return

    while True:

        ws = None

        try:

            url = (
                f"{WS_URL}"
                f"?apikey={API_KEY}"
            )

            add_history(
                f"Connecting WebSocket for {SYMBOL}..."
            )

            ws = websocket.create_connection(
                url,
                timeout=2,
                enable_multithread=True,
            )

            with state_lock:
                state["ws_connected"] = True
                state["ws_subscribed"] = False
                state["ws_error"] = None

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": SYMBOL,
                },
            }

            ws.send(
                json.dumps(subscribe_message)
            )

            add_history(
                f"Subscription request sent for {SYMBOL}."
            )

            last_heartbeat = time.time()

            while True:

                now = time.time()

                # ----------------------------------
                # HEARTBEAT
                # ----------------------------------

                if (
                    now - last_heartbeat
                    >= HEARTBEAT_INTERVAL
                ):

                    heartbeat = {
                        "action": "heartbeat"
                    }

                    ws.send(
                        json.dumps(heartbeat)
                    )

                    last_heartbeat = now

                # ----------------------------------
                # RECEIVE
                # ----------------------------------

                try:

                    raw = ws.recv()

                    if raw is None:
                        raise ConnectionError(
                            "WebSocket connection closed."
                        )

                    process_ws_message(raw)

                except websocket.WebSocketTimeoutException:
                    continue

        except Exception as exc:

            message = (
                f"{type(exc).__name__}: {exc}"
            )

            set_ws_error(message)

            with state_lock:
                state["ws_connected"] = False
                state["ws_subscribed"] = False

            add_history(
                f"WebSocket disconnected: {message}"
            )

            if is_provider_access_error(message):

                add_history(
                    "WebSocket provider/authentication issue detected. "
                    "Waiting before retry."
                )

                time.sleep(
                    WS_PROVIDER_ERROR_DELAY
                )

            else:

                time.sleep(
                    WS_RECONNECT_DELAY
                )

        finally:

            with state_lock:
                state["ws_connected"] = False
                state["ws_subscribed"] = False

            if ws is not None:

                try:
                    ws.close()
                except Exception:
                    pass


# ============================================================
# REST PRICE FALLBACK
# ============================================================

def rest_price_worker():

    while True:

        try:

            with state_lock:
                ws_ok = (
                    state["ws_connected"]
                    and state["ws_subscribed"]
                )

            # WebSocket healthy -> do nothing.
            if ws_ok:

                time.sleep(REST_PRICE_INTERVAL)

                continue

            data = td_get(
                "price",
                {
                    "symbol": SYMBOL,
                },
            )

            if data:

                price = safe_float(
                    data.get("price")
                )

                if price is not None:

                    timestamp = utc_timestamp()

                    with state_lock:
                        state["price"] = price
                        state["timestamp"] = timestamp

                    update_live_candle(
                        price,
                        timestamp,
                    )

                    add_history(
                        f"REST fallback price: {price:.3f}"
                    )

            time.sleep(
                REST_PRICE_INTERVAL
            )

        except Exception as exc:

            add_history(
                f"REST fallback worker error: "
                f"{type(exc).__name__}"
            )

            time.sleep(
                REST_PRICE_INTERVAL
            )


# ============================================================
# HISTORY REFRESH WORKER
# ============================================================

def history_refresh_worker():

    # Initial wait allows the main startup to settle.
    time.sleep(30)

    while True:

        try:

            with state_lock:
                ws_ok = (
                    state["ws_connected"]
                    and state["ws_subscribed"]
                )

            # History is still useful periodically,
            # even when WS is healthy.
            # However, only if REST is available.
            if check_rest_cooldown():

                data = td_get(
                    "time_series",
                    {
                        "symbol": SYMBOL,
                        "interval": "5min",
                        "outputsize": 500,
                        "timezone": "UTC",
                    },
                )

                if data:

                    values = data.get(
                        "values",
                        [],
                    )

                    candles = []

                    for item in values:

                        candle = normalize_rest_candle(
                            item
                        )

                        if candle:
                            candles.append(candle)

                    candles.sort(
                        key=lambda x: x["time"]
                    )

                    if candles:

                        with state_lock:

                            state["candles"]["5min"].clear()

                            for candle in candles[
                                -MAX_CANDLES:
                            ]:
                                state["candles"]["5min"].append(
                                    candle
                                )

                        rebuild_all_timeframes()

                        add_history(
                            "5-minute history refreshed."
                        )

            time.sleep(
                HISTORY_REFRESH_INTERVAL
            )

        except Exception as exc:

            add_history(
                f"History worker error: "
                f"{type(exc).__name__}"
            )

            time.sleep(
                HISTORY_REFRESH_INTERVAL
            )


# ============================================================
# DAILY HISTORY WORKER
# ============================================================

def daily_history_worker():

    time.sleep(60)

    while True:

        try:

            if check_rest_cooldown():
                load_daily_history()

            time.sleep(
                DAILY_REFRESH_INTERVAL
            )

        except Exception as exc:

            add_history(
                f"Daily history worker error: "
                f"{type(exc).__name__}"
            )

            time.sleep(
                DAILY_REFRESH_INTERVAL
            )


# ============================================================
# INDICATORS
# ============================================================

def closes(candles):

    return [
        float(c["close"])
        for c in candles
        if c.get("close") is not None
    ]


def ema(values, period):

    if not values:
        return None

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(
        values[:period]
    ) / period

    for value in values[period:]:
        result = (
            (value - result) * multiplier
            + result
        )

    return result


def rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i] - values[i - 1]
        )

        if change >= 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

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
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


def atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(candles)):

        current = candles[i]
        previous = candles[i - 1]

        high = current["high"]
        low = current["low"]
        previous_close = previous["close"]

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    value = sum(
        true_ranges[:period]
    ) / period

    for tr in true_ranges[period:]:

        value = (
            (value * (period - 1))
            + tr
        ) / period

    return value


# ============================================================
# STRUCTURE
# ============================================================

def market_structure(candles):

    if len(candles) < 10:
        return "WAIT"

    recent = candles[-10:]

    highs = [
        c["high"]
        for c in recent
    ]

    lows = [
        c["low"]
        for c in recent
    ]

    if (
        highs[-1] > highs[-3]
        and lows[-1] > lows[-3]
    ):
        return "HIGHER"

    if (
        highs[-1] < highs[-3]
        and lows[-1] < lows[-3]
    ):
        return "LOWER"

    return "RANGE"


# ============================================================
# LIQUIDITY
# ============================================================

def liquidity_levels(candles, lookback=20):

    if len(candles) < lookback:
        return None, None

    recent = candles[-lookback:]

    high = max(
        c["high"]
        for c in recent
    )

    low = min(
        c["low"]
        for c in recent
    )

    return high, low


def detect_sweep(candles):

    if len(candles) < 20:
        return "NONE"

    previous = candles[-20:-1]
    current = candles[-1]

    previous_high = max(
        c["high"]
        for c in previous
    )

    previous_low = min(
        c["low"]
        for c in previous
    )

    if (
        current["high"] > previous_high
        and current["close"] < previous_high
    ):
        return "HIGH_SWEEP"

    if (
        current["low"] < previous_low
        and current["close"] > previous_low
    ):
        return "LOW_SWEEP"

    return "NONE"


# ============================================================
# ANALYSIS ENGINE
# ============================================================

def calculate_analysis():

    with state_lock:

        candles_5m = list(
            state["candles"]["5min"]
        )

        candles_15m = list(
            state["candles"]["15min"]
        )

        candles_1h = list(
            state["candles"]["1h"]
        )

        price = state["price"]

    if price is None or len(candles_5m) < 20:

        return {
            "trend": "WAIT",
            "momentum": "WAIT",
            "structure": "WAIT",
            "rsi": None,
            "ema20": None,
            "ema50": None,
            "atr": None,
            "liquidity_high": None,
            "liquidity_low": None,
            "sweep": "NONE",
            "signal": "WAIT",
            "signal_strength": 0,
            "entry": None,
            "stop_loss": None,
            "target_1": None,
            "target_2": None,
            "risk_reward": None,
            "updated": utc_now().isoformat(),
        }

    values = closes(candles_5m)

    ema20 = ema(values, 20)
    ema50 = ema(values, 50)
    rsi_value = rsi(values, 14)
    atr_value = atr(candles_5m, 14)

    structure_5m = market_structure(
        candles_5m
    )

    structure_15m = (
        market_structure(candles_15m)
        if len(candles_15m) >= 10
        else "WAIT"
    )

    structure_1h = (
        market_structure(candles_1h)
        if len(candles_1h) >= 10
        else "WAIT"
    )

    liquidity_high, liquidity_low = (
        liquidity_levels(
            candles_5m,
            20,
        )
    )

    sweep = detect_sweep(
        candles_5m
    )

    # ------------------------------------------
    # TREND
    # ------------------------------------------

    bullish_points = 0
    bearish_points = 0

    if ema20 is not None and ema50 is not None:

        if ema20 > ema50:
            bullish_points += 2

        elif ema20 < ema50:
            bearish_points += 2

    if price > ema20 if ema20 is not None else False:
        bullish_points += 1

    if price < ema20 if ema20 is not None else False:
        bearish_points += 1

    if structure_5m == "HIGHER":
        bullish_points += 1

    elif structure_5m == "LOWER":
        bearish_points += 1

    if structure_15m == "HIGHER":
        bullish_points += 2

    elif structure_15m == "LOWER":
        bearish_points += 2

    if structure_1h == "HIGHER":
        bullish_points += 2

    elif structure_1h == "LOWER":
        bearish_points += 2

    if bullish_points >= bearish_points + 2:
        trend = "BULLISH"

    elif bearish_points >= bullish_points + 2:
        trend = "BEARISH"

    else:
        trend = "RANGE"


    # ------------------------------------------
    # MOMENTUM
    # ------------------------------------------

    if rsi_value is None:
        momentum = "WAIT"

    elif rsi_value >= 60:
        momentum = "BUYING"

    elif rsi_value <= 40:
        momentum = "SELLING"

    else:
        momentum = "NEUTRAL"


    # ------------------------------------------
    # SIGNAL SCORE
    # ------------------------------------------

    buy_score = 0
    sell_score = 0

    if trend == "BULLISH":
        buy_score += 3

    elif trend == "BEARISH":
        sell_score += 3

    if momentum == "BUYING":
        buy_score += 2

    elif momentum == "SELLING":
        sell_score += 2

    if structure_5m == "HIGHER":
        buy_score += 2

    elif structure_5m == "LOWER":
        sell_score += 2

    if structure_15m == "HIGHER":
        buy_score += 2

    elif structure_15m == "LOWER":
        sell_score += 2

    if structure_1h == "HIGHER":
        buy_score += 2

    elif structure_1h == "LOWER":
        sell_score += 2

    if sweep == "LOW_SWEEP":
        buy_score += 2

    elif sweep == "HIGH_SWEEP":
        sell_score += 2


    # ------------------------------------------
    # FINAL SIGNAL
    # ------------------------------------------

    signal = "WAIT"

    if (
        buy_score >= 7
        and buy_score >= sell_score + 2
    ):
        signal = "BUY"

    elif (
        sell_score >= 7
        and sell_score >= buy_score + 2
    ):
        signal = "SELL"


    signal_strength = min(
        100,
        max(
            buy_score,
            sell_score,
        ) * 10,
        ),
    )


    # ------------------------------------------
    # TRADE LEVELS
    # ------------------------------------------

    entry = price
    stop_loss = None
    target_1 = None
    target_2 = None
    risk_reward = None

    if (
        signal in ("BUY", "SELL")
        and atr_value is not None
        and atr_value > 0
    ):

        if signal == "BUY":

            stop_loss = price - (
                atr_value * 1.2
            )

            target_1 = price + (
                atr_value * 1.5
            )

            target_2 = price + (
                atr_value * 2.5
            )

        else:

            stop_loss = price + (
                atr_value * 1.2
            )

            target_1 = price - (
                atr_value * 1.5
            )

            target_2 = price - (
                atr_value * 2.5
            )

        risk = abs(
            price - stop_loss
        )

        reward = abs(
            target_1 - price
        )

        if risk > 0:
            risk_reward = reward / risk


    return {
        "trend": trend,
        "momentum": momentum,
        "structure": structure_5m,
        "rsi": round(rsi_value, 2)
        if rsi_value is not None
        else None,
        "ema20": round(ema20, 3)
        if ema20 is not None
        else None,
        "ema50": round(ema50, 3)
        if ema50 is not None
        else None,
        "atr": round(atr_value, 3)
        if atr_value is not None
        else None,
        "liquidity_high": round(
            liquidity_high,
            3,
        )
        if liquidity_high is not None
        else None,
        "liquidity_low": round(
            liquidity_low,
            3,
        )
        if liquidity_low is not None
        else None,
        "sweep": sweep,
        "signal": signal,
        "signal_strength": signal_strength,
        "entry": round(entry, 3)
        if entry is not None
        else None,
        "stop_loss": round(stop_loss, 3)
        if stop_loss is not None
        else None,
        "target_1": round(target_1, 3)
        if target_1 is not None
        else None,
        "target_2": round(target_2, 3)
        if target_2 is not None
        else None,
        "risk_reward": round(
            risk_reward,
            2,
        )
        if risk_reward is not None
        else None,
        "updated": utc_now().isoformat(),
    }


# ============================================================
# ANALYSIS WORKER
# ============================================================

def analysis_worker():

    while True:

        try:

            result = calculate_analysis()

            with state_lock:
                state["analysis"] = result

            time.sleep(5)

        except Exception as exc:

            add_history(
                f"Analysis error: "
                f"{type(exc).__name__}"
            )

            time.sleep(5)


# ============================================================
# API SERIALIZATION
# ============================================================

def candle_to_dict(candle):

    return {
        "time": int(candle["time"]),
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
    }


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():

    return Response(
        HTML_PAGE,
        mimetype="text/html",
    )


@app.route("/health")
def health():

    with state_lock:

        return jsonify({
            "status": "ok",
            "symbol": state["symbol"],
            "price": state["price"],
            "websocket_connected": state["ws_connected"],
            "websocket_subscribed": state["ws_subscribed"],
            "history_loaded": state["history_loaded"],
            "rest_available": state["rest_available"],
            "timestamp": state["timestamp"],
        })


@app.route("/api/market")
def market():

    with state_lock:

        return jsonify({
            "app": APP_NAME,
            "symbol": state["symbol"],
            "price": state["price"],
            "timestamp": state["timestamp"],
            "websocket": {
                "connected": state["ws_connected"],
                "subscribed": state["ws_subscribed"],
                "error": state["ws_error"],
            },
            "analysis": state["analysis"],
            "history": list(
                state["history"]
            )[:20],
        })


@app.route("/api/candles")
def candles():

    with state_lock:

        data = {
            timeframe: [
                candle_to_dict(c)
                for c in values
            ]
            for timeframe, values
            in state["candles"].items()
        }

    return jsonify(data)


@app.route("/stream")
def stream():

    with state_lock:

        payload = {
            "symbol": state["symbol"],
            "price": state["price"],
            "timestamp": state["timestamp"],
            "analysis": state["analysis"],
            "websocket": {
                "connected": state["ws_connected"],
                "subscribed": state["ws_subscribed"],
            },
        }

    return jsonify(payload)


# ============================================================
# STARTUP
# ============================================================

startup_lock = threading.Lock()
startup_done = False


def start_background_workers():

    global startup_done

    with startup_lock:

        if startup_done:
            return

        startup_done = True

    if not API_KEY:

        add_history(
            "WARNING: TWELVE_DATA_API_KEY is not set."
        )

    else:

        # Load initial REST history once.
        load_initial_history()

        # Daily history is optional and may fail because of quota.
        load_daily_history()

    workers = [
        (
            "websocket-worker",
            websocket_worker,
        ),
        (
            "rest-price-worker",
            rest_price_worker,
        ),
        (
            "history-refresh-worker",
            history_refresh_worker,
        ),
        (
            "daily-history-worker",
            daily_history_worker,
        ),
        (
            "analysis-worker",
            analysis_worker,
        ),
    ]

    for name, target in workers:

        thread = threading.Thread(
            target=target,
            name=name,
            daemon=True,
        )

        thread.start()


# Start once when Gunicorn imports the app.
start_background_workers()


# ============================================================
# DASHBOARD
# ============================================================

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>Trading AI</title>

<script src="https://unpkg.com/lightweight-charts@5.2.1/dist/lightweight-charts.standalone.production.js"></script>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #07111f;
    color: #e8eef7;
    font-family:
        Arial,
        Helvetica,
        sans-serif;
}

.container {
    width: min(1450px, 96%);
    margin: 20px auto;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 20px;
    margin-bottom: 18px;
}

.title {
    font-size: 28px;
    font-weight: 700;
}

.subtitle {
    color: #8fa3ba;
    margin-top: 5px;
}

.status {
    padding: 9px 14px;
    border-radius: 20px;
    background: #132238;
    font-size: 13px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(4, minmax(0, 1fr));
    gap: 12px;
}

.card {
    background: #0d1a2b;
    border: 1px solid #1b3048;
    border-radius: 12px;
    padding: 16px;
}

.label {
    color: #8fa3ba;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.7px;
}

.value {
    font-size: 23px;
    font-weight: 700;
    margin-top: 8px;
}

.price {
    font-size: 34px;
}

.signal {
    font-size: 30px;
}

.chart-card {
    margin-top: 14px;
    padding: 12px;
}

#chart {
    width: 100%;
    height: 520px;
}

.metrics {
    margin-top: 14px;
    display: grid;
    grid-template-columns:
        repeat(6, minmax(0, 1fr));
    gap: 10px;
}

.small-card {
    background: #0d1a2b;
    border: 1px solid #1b3048;
    border-radius: 10px;
    padding: 13px;
}

.small-value {
    margin-top: 7px;
    font-size: 17px;
    font-weight: 600;
}

.buy {
    color: #46d391;
}

.sell {
    color: #ff6b78;
}

.wait {
    color: #ffd166;
}

.footer {
    margin-top: 14px;
    color: #71869e;
    font-size: 12px;
}

@media (max-width: 1000px) {

    .grid {
        grid-template-columns:
            repeat(2, minmax(0, 1fr));
    }

    .metrics {
        grid-template-columns:
            repeat(3, minmax(0, 1fr));
    }

}

@media (max-width: 600px) {

    .grid {
        grid-template-columns: 1fr;
    }

    .metrics {
        grid-template-columns:
            repeat(2, minmax(0, 1fr));
    }

    #chart {
        height: 400px;
    }

}

</style>

</head>


<body>

<div class="container">

    <div class="header">

        <div>
            <div class="title">
                Trading AI
            </div>

            <div class="subtitle">
                Real-Time Market Intelligence
            </div>
        </div>

        <div
            id="connection"
            class="status"
        >
            CONNECTING
        </div>

    </div>


    <div class="grid">

        <div class="card">

            <div class="label">
                Gold — XAU/USD
            </div>

            <div
                id="price"
                class="value price"
            >
                --
            </div>

        </div>


        <div class="card">

            <div class="label">
                Signal
            </div>

            <div
                id="signal"
                class="value signal"
            >
                WAIT
            </div>

        </div>


        <div class="card">

            <div class="label">
                Trend
            </div>

            <div
                id="trend"
                class="value"
            >
                --
            </div>

        </div>


        <div class="card">

            <div class="label">
                Signal Strength
            </div>

            <div
                id="strength"
                class="value"
            >
                --
            </div>

        </div>

    </div>


    <div class="chart-card card">

        <div id="chart"></div>

    </div>


    <div class="metrics">

        <div class="small-card">
            <div class="label">Momentum</div>
            <div
                id="momentum"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Structure</div>
            <div
                id="structure"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">RSI</div>
            <div
                id="rsi"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">EMA 20</div>
            <div
                id="ema20"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">EMA 50</div>
            <div
                id="ema50"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">ATR</div>
            <div
                id="atr"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Liquidity High</div>
            <div
                id="liqHigh"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Liquidity Low</div>
            <div
                id="liqLow"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Sweep</div>
            <div
                id="sweep"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Entry</div>
            <div
                id="entry"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Stop Loss</div>
            <div
                id="sl"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Target 1</div>
            <div
                id="target1"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Target 2</div>
            <div
                id="target2"
                class="small-value"
            >
                --
            </div>
        </div>

        <div class="small-card">
            <div class="label">Risk / Reward</div>
            <div
                id="rr"
                class="small-value"
            >
                --
            </div>
        </div>

    </div>


    <div class="footer">
        Trading analysis is informational and does not guarantee
        trading results.
    </div>

</div>


<script>

let chart = null;
let candleSeries = null;


function setText(id, value) {

    const element =
        document.getElementById(id);

    if (element) {
        element.textContent =
            value === null ||
            value === undefined
                ? "--"
                : value;
    }
}


function setSignalClass(element, value) {

    if (!element) {
        return;
    }

    element.classList.remove(
        "buy",
        "sell",
        "wait"
    );

    if (value === "BUY") {
        element.classList.add("buy");
    }
    else if (value === "SELL") {
        element.classList.add("sell");
    }
    else {
        element.classList.add("wait");
    }
}


function initializeChart() {

    const container =
        document.getElementById("chart");

    chart =
        LightweightCharts.createChart(
            container,
            {
                layout: {
                    background: {
                        color: "#07111f"
                    },
                    textColor: "#b8c6d8"
                },

                grid: {
                    vertLines: {
                        color: "#14253a"
                    },
                    horzLines: {
                        color: "#14253a"
                    }
                },

                rightPriceScale: {
                    borderColor: "#263c55"
                },

                timeScale: {
                    borderColor: "#263c55",
                    timeVisible: true,
                    secondsVisible: false
                }
            }
        );


    // Lightweight Charts v5 API
    candleSeries =
        chart.addSeries(
            LightweightCharts.CandlestickSeries
        );


    candleSeries.applyOptions({

        upColor: "#46d391",
        downColor: "#ff6b78",

        borderUpColor: "#46d391",
        borderDownColor: "#ff6b78",

        wickUpColor: "#46d391",
        wickDownColor: "#ff6b78"

    });


    window.addEventListener(
        "resize",
        () => {

            chart.resize(
                container.clientWidth,
                container.clientHeight
            );

        }
    );
}


async function loadCandles() {

    try {

        const response =
            await fetch(
                "/api/candles",
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            return;
        }

        const data =
            await response.json();

        if (
            data &&
            data["5min"] &&
            candleSeries
        ) {

            candleSeries.setData(
                data["5min"]
            );

            chart.timeScale()
                .fitContent();
        }

    }
    catch (error) {

        console.error(
            "Candle load error:",
            error
        );

    }
}


function formatNumber(value, digits = 3) {

    if (
        value === null ||
        value === undefined ||
        Number.isNaN(Number(value))
    ) {
        return "--";
    }

    return Number(value)
        .toFixed(digits);
}


async function loadMarket() {

    try {

        const response =
            await fetch(
                "/api/market",
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            return;
        }

        const data =
            await response.json();

        const analysis =
            data.analysis || {};

        const websocket =
            data.websocket || {};


        // -----------------------------
        // PRICE
        // -----------------------------

        setText(
            "price",
            formatNumber(
                data.price,
                3
            )
        );


        // -----------------------------
        // SIGNAL
        // -----------------------------

        const signalElement =
            document.getElementById(
                "signal"
            );

        setText(
            "signal",
            analysis.signal || "WAIT"
        );

        setSignalClass(
            signalElement,
            analysis.signal || "WAIT"
        );


        // -----------------------------
        // TREND
        // -----------------------------

        setText(
            "trend",
            analysis.trend || "--"
        );


        // -----------------------------
        // STRENGTH
        // -----------------------------

        setText(
            "strength",
            analysis.signal_strength !== null &&
            analysis.signal_strength !== undefined
                ? analysis.signal_strength + "%"
                : "--"
        );


        // -----------------------------
        // METRICS
        // -----------------------------

        setText(
            "momentum",
            analysis.momentum || "--"
        );

        setText(
            "structure",
            analysis.structure || "--"
        );

        setText(
            "rsi",
            analysis.rsi !== null
                ? formatNumber(
                    analysis.rsi,
                    2
                )
                : "--"
        );

        setText(
            "ema20",
            formatNumber(
                analysis.ema20,
                3
            )
        );

        setText(
            "ema50",
            formatNumber(
                analysis.ema50,
                3
            )
        );

        setText(
            "atr",
            formatNumber(
                analysis.atr,
                3
            )
        );

        setText(
            "liqHigh",
            formatNumber(
                analysis.liquidity_high,
                3
            )
        );

        setText(
            "liqLow",
            formatNumber(
                analysis.liquidity_low,
                3
            )
        );

        setText(
            "sweep",
            analysis.sweep || "--"
        );

        setText(
            "entry",
            formatNumber(
                analysis.entry,
                3
            )
        );

        setText(
            "sl",
            formatNumber(
                analysis.stop_loss,
                3
            )
        );

        setText(
            "target1",
            formatNumber(
                analysis.target_1,
                3
            )
        );

        setText(
            "target2",
            formatNumber(
                analysis.target_2,
                3
            )
        );

        setText(
            "rr",
            analysis.risk_reward !== null &&
            analysis.risk_reward !== undefined
                ? "1:" +
                    Number(
                        analysis.risk_reward
                    ).toFixed(2)
                : "--"
        );


        // -----------------------------
        // CONNECTION
        // -----------------------------

        const connection =
            document.getElementById(
                "connection"
            );

        if (
            websocket.connected &&
            websocket.subscribed
        ) {

            connection.textContent =
                "● LIVE";

        }
        else if (
            websocket.connected
        ) {

            connection.textContent =
                "CONNECTED";

        }
        else {

            connection.textContent =
                "RECONNECTING";

        }

    }
    catch (error) {

        console.error(
            "Market load error:",
            error
        );

        const connection =
            document.getElementById(
                "connection"
            );

        connection.textContent =
            "CONNECTION ERROR";
    }
}


async function refreshAll() {

    await loadMarket();

}


initializeChart();

loadCandles();

loadMarket();


// Market status every second.
setInterval(
    loadMarket,
    1000
);


// Candles every 10 seconds.
setInterval(
    loadCandles,
    10000
);

</script>

</body>

</html>
"""


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                "10000"
            )
        ),
        debug=False,
        use_reloader=False,
    )
