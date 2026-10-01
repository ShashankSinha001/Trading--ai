import os
import json
import time
import threading
import queue
import requests
import websocket

from flask import Flask, Response, jsonify, render_template_string


# ============================================================
# TRADING AI
# REAL-TIME MARKET INTELLIGENCE
# GOLD XAU/USD
# ============================================================

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()

GOLD_SYMBOL = "XAU/USD"

# WTI intentionally disabled because current Twelve Data plan
# does not provide WTI/USD.
OIL_ENABLED = False
OIL_SYMBOL = "WTI/USD"

WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

PORT = int(os.environ.get("PORT", "10000"))


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAMES = [
    "1min",
    "5min",
    "15min",
    "30min",
    "1h",
    "4h",
    "1day",
    "1week",
    "1month",
]

DISPLAY_TF = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1h": "1H",
    "4h": "4H",
    "1day": "1D",
    "1week": "1W",
    "1month": "1M",
}


# ============================================================
# API REQUEST CONTROL
#
# Important:
# Do NOT continuously hit Twelve Data after 429.
# ============================================================

REFRESH_SECONDS = {
    "1min": 300,
    "5min": 600,
    "15min": 900,
    "30min": 1800,
    "1h": 3600,
    "4h": 14400,
    "1day": 21600,
    "1week": 43200,
    "1month": 86400,
}

OUTPUT_SIZE = {
    "1min": 300,
    "5min": 250,
    "15min": 200,
    "30min": 180,
    "1h": 160,
    "4h": 140,
    "1day": 120,
    "1week": 100,
    "1month": 60,
}


# ============================================================
# STATE
# ============================================================

state = {
    "gold": {
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
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,
            "invalidation": None,
            "setup": "WAIT",
            "why": "Waiting for sufficient candle data.",
        },

        "data_status": "WAITING FOR CANDLE DATA",
        "data_error": None,
    },

    "oil": {
        "price": None,
        "connection": "PLAN LIMIT",
        "updated": None,
    },
}


# ============================================================
# CANDLE CACHE
# ============================================================

candle_cache = {}

last_fetch_time = {}

fetch_locks = {
    tf: threading.Lock()
    for tf in TIMEFRAMES
}


# Global API cooldown.
# When Twelve Data returns 429, no candle request is made
# until this timestamp.
api_cooldown_until = 0

# How long to stay quiet after a 429.
# 30 minutes prevents request spam.
API_COOLDOWN_SECONDS = 1800


# ============================================================
# THREADING
# ============================================================

state_lock = threading.RLock()

stream_clients = []

workers_started = False


# ============================================================
# UTILITY
# ============================================================

def now_string():
    return time.strftime("%H:%M:%S")


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def calculate_ema(values, period):
    if not values:
        return None

    if len(values) < period:
        period = len(values)

    if period <= 0:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(values[:period]) / period

    for price in values[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


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

    recent_gains = gains[-period:]
    recent_losses = losses[-period:]

    avg_gain = sum(recent_gains) / period
    avg_loss = sum(recent_losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def fmt_price(value):
    if value is None:
        return "—"

    try:
        return f"{float(value):.3f}"
    except Exception:
        return "—"


# ============================================================
# TWELVE DATA COOLDOWN
# ============================================================

def api_is_in_cooldown():
    global api_cooldown_until

    return time.time() < api_cooldown_until


def activate_api_cooldown():
    global api_cooldown_until

    api_cooldown_until = time.time() + API_COOLDOWN_SECONDS

    remaining = int(API_COOLDOWN_SECONDS / 60)

    print(
        f"CANDLE API COOLDOWN ACTIVE: {remaining} minutes"
    )


# ============================================================
# CANDLE NORMALIZATION
# ============================================================

def normalize_candles(values):
    candles = []

    if not isinstance(values, list):
        return candles

    for item in values:
        try:
            dt = item.get("datetime")

            o = safe_float(item.get("open"))
            h = safe_float(item.get("high"))
            l = safe_float(item.get("low"))
            c = safe_float(item.get("close"))

            if not dt:
                continue

            if None in (o, h, l, c):
                continue

            candles.append(
                {
                    "time": dt,
                    "datetime": dt,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                }
            )

        except Exception:
            continue

    candles.reverse()

    return candles


# ============================================================
# GET CANDLES
# ============================================================

def get_candles(interval):
    """
    Fetch candles only when needed.

    IMPORTANT:
    - Successful response updates cache.
    - 429 activates global cooldown.
    - Failed request never deletes old cache.
    """

    if not API_KEY:
        print("CANDLE ERROR: TWELVE_DATA_API_KEY missing")
        return candle_cache.get(interval, [])

    # Global cooldown after 429.
    if api_is_in_cooldown():
        return candle_cache.get(interval, [])

    lock = fetch_locks[interval]

    if not lock.acquire(blocking=False):
        return candle_cache.get(interval, [])

    try:

        url = "https://api.twelvedata.com/time_series"

        params = {
            "symbol": GOLD_SYMBOL,
            "interval": interval,
            "outputsize": OUTPUT_SIZE.get(interval, 100),
            "apikey": API_KEY,
        }

        response = requests.get(
            url,
            params=params,
            timeout=15,
        )

        if response.status_code == 429:

            print(
                f"CANDLE HTTP ERROR: {interval} 429"
            )

            activate_api_cooldown()

            return candle_cache.get(interval, [])

        if response.status_code != 200:

            print(
                f"CANDLE HTTP ERROR: "
                f"{interval} {response.status_code}"
            )

            return candle_cache.get(interval, [])

        data = response.json()

        if "values" not in data:

            print(
                f"CANDLE API ERROR: "
                f"{interval} {data}"
            )

            return candle_cache.get(interval, [])

        candles = normalize_candles(
            data["values"]
        )

        if not candles:

            print(
                f"CANDLE EMPTY: {interval}"
            )

            return candle_cache.get(interval, [])

        # SUCCESS
        candle_cache[interval] = candles

        last_fetch_time[interval] = time.time()

        print(
            f"CANDLE SUCCESS: "
            f"{interval} {len(candles)} candles"
        )

        return candles

    except Exception as exc:

        print(
            f"CANDLE EXCEPTION: "
            f"{interval} {repr(exc)}"
        )

        return candle_cache.get(interval, [])

    finally:
        lock.release()


# ============================================================
# SHOULD REFRESH
# ============================================================

def should_refresh(interval):

    if interval not in candle_cache:
        return True

    last = last_fetch_time.get(interval)

    if last is None:
        return True

    return (
        time.time() - last
        >= REFRESH_SECONDS.get(interval, 3600)
    )


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def analyze_timeframe(candles):

    if not candles or len(candles) < 20:
        return {
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
        }

    closes = [
        float(c["close"])
        for c in candles
    ]

    highs = [
        float(c["high"])
        for c in candles
    ]

    lows = [
        float(c["low"])
        for c in candles
    ]

    current = closes[-1]

    ema20 = calculate_ema(
        closes,
        20,
    )

    ema50 = calculate_ema(
        closes,
        50,
    )

    rsi = calculate_rsi(
        closes,
        14,
    )

    recent20_high = max(
        highs[-20:]
    )

    recent20_low = min(
        lows[-20:]
    )

    recent10_high = max(
        highs[-10:]
    )

    recent10_low = min(
        lows[-10:]
    )

    # -------------------------
    # TREND
    # -------------------------

    if ema20 is None or ema50 is None:
        trend = "WAITING"

    elif current > ema20 > ema50:
        trend = "BULLISH"

    elif current < ema20 < ema50:
        trend = "BEARISH"

    else:
        trend = "NEUTRAL"


    # -------------------------
    # MOMENTUM
    # -------------------------

    if rsi is None:
        momentum = "WAITING"

    elif rsi >= 60:
        momentum = "BULLISH"

    elif rsi <= 40:
        momentum = "BEARISH"

    else:
        momentum = "NEUTRAL"


    # -------------------------
    # STRUCTURE
    # -------------------------

    previous = closes[-10]

    if current > previous:
        structure = "BULLISH"

    elif current < previous:
        structure = "BEARISH"

    else:
        structure = "NEUTRAL"


    # -------------------------
    # SWEEP
    # -------------------------

    sweep = "NONE"

    if highs[-1] > recent10_high:
        sweep = "HIGH SWEEP"

    elif lows[-1] < recent10_low:
        sweep = "LOW SWEEP"


    return {
        "trend": trend,
        "momentum": momentum,
        "structure": structure,

        "rsi": rsi,

        "ema20": ema20,
        "ema50": ema50,

        "support": recent20_low,
        "resistance": recent20_high,

        "liquidity_high": recent10_high,
        "liquidity_low": recent10_low,

        "sweep": sweep,
    }


# ============================================================
# AI ANALYSIS
# ============================================================

def build_ai_analysis(timeframe_data):

    required = [
        "5min",
        "15min",
        "1h",
        "4h",
    ]

    available = [
        tf
        for tf in required
        if tf in timeframe_data
        and timeframe_data[tf]
    ]

    if len(available) < 2:

        return {
            "signal": "WAIT",
            "confidence": 50,
            "score": 0,
            "reason": "Waiting for sufficient candle data.",
        }


    score = 0

    weights = {
        "5min": 1,
        "15min": 2,
        "1h": 3,
        "4h": 3,
    }


    for tf in available:

        analysis = timeframe_data[tf]

        weight = weights.get(tf, 1)

        if analysis["trend"] == "BULLISH":
            score += weight

        elif analysis["trend"] == "BEARISH":
            score -= weight


        if analysis["momentum"] == "BULLISH":
            score += weight

        elif analysis["momentum"] == "BEARISH":
            score -= weight


        if analysis["structure"] == "BULLISH":
            score += weight

        elif analysis["structure"] == "BEARISH":
            score -= weight


        if analysis["sweep"] == "HIGH SWEEP":
            score -= 1

        elif analysis["sweep"] == "LOW SWEEP":
            score += 1


    score = max(
        -10,
        min(10, score)
    )


    if score >= 6:
        signal = "BUY"

    elif score <= -6:
        signal = "SELL"

    else:
        signal = "WAIT"


    confidence = min(
        95,
        50 + abs(score) * 5
    )


    return {
        "signal": signal,
        "confidence": confidence,
        "score": score,
        "reason": (
            f"Multi-timeframe analysis "
            f"based on {len(available)} timeframes."
        ),
    }


# ============================================================
# TRADE PLAN
# ============================================================

def build_trade_plan(
    signal,
    analysis,
):

    if signal == "WAIT":

        return {
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,
            "invalidation": None,
            "setup": "WAIT",
            "why": "Waiting for sufficient confirmation.",
        }


    entry = analysis.get("ema20")

    support = analysis.get("support")

    resistance = analysis.get("resistance")

    if entry is None:
        return {
            "entry": None,
            "stop_loss": None,
            "target1": None,
            "target2": None,
            "target3": None,
            "rr": None,
            "invalidation": None,
            "setup": "WAIT",
            "why": "EMA data unavailable.",
        }


    if signal == "BUY":

        stop = support

        if stop is None or stop >= entry:
            return {
                "entry": entry,
                "stop_loss": None,
                "target1": None,
                "target2": None,
                "target3": None,
                "rr": None,
                "invalidation": "Below confirmed support",
                "setup": "BUY",
                "why": "Buy bias detected, waiting for valid risk level.",
            }

        risk = entry - stop

        target1 = entry + risk
        target2 = entry + risk * 2
        target3 = entry + risk * 3

        return {
            "entry": entry,
            "stop_loss": stop,
            "target1": target1,
            "target2": target2,
            "target3": target3,
            "rr": "1:1 / 1:2 / 1:3",
            "invalidation": f"Below {fmt_price(stop)}",
            "setup": "BUY",
            "why": "Multi-timeframe bullish confirmation.",
        }


    if signal == "SELL":

        stop = resistance

        if stop is None or stop <= entry:
            return {
                "entry": entry,
                "stop_loss": None,
                "target1": None,
                "target2": None,
                "target3": None,
                "rr": None,
                "invalidation": "Above confirmed resistance",
                "setup": "SELL",
                "why": "Sell bias detected, waiting for valid risk level.",
            }

        risk = stop - entry

        target1 = entry - risk
        target2 = entry - risk * 2
        target3 = entry - risk * 3

        return {
            "entry": entry,
            "stop_loss": stop,
            "target1": target1,
            "target2": target2,
            "target3": target3,
            "rr": "1:1 / 1:2 / 1:3",
            "invalidation": f"Above {fmt_price(stop)}",
            "setup": "SELL",
            "why": "Multi-timeframe bearish confirmation.",
        }


    return {
        "entry": None,
        "stop_loss": None,
        "target1": None,
        "target2": None,
        "target3": None,
        "rr": None,
        "invalidation": None,
        "setup": "WAIT",
        "why": "Waiting for confirmation.",
    }


# ============================================================
# SIGNAL HISTORY
# ============================================================

def record_signal_history(signal, confidence):

    history = state["gold"]["signal_history"]

    if history:

        if history[-1]["signal"] == signal:
            return

    history.append(
        {
            "time": now_string(),
            "signal": signal,
            "confidence": confidence,
        }
    )

    if len(history) > 20:
        del history[:-20]


# ============================================================
# ANALYSIS ENGINE
# ============================================================

def run_gold_analysis():

    timeframe_data = {}

    candle_data = {}

    any_candles = False


    for interval in TIMEFRAMES:

        try:

            # Only request when necessary.
            if should_refresh(interval):

                candles = get_candles(interval)

            else:

                candles = candle_cache.get(
                    interval,
                    []
                )


            # ALWAYS preserve cached data.
            if candles:

                candle_data[interval] = candles

                timeframe_data[interval] = (
                    analyze_timeframe(candles)
                )

                any_candles = True

        except Exception as exc:

            print(
                f"ANALYSIS ERROR "
                f"{interval}: {repr(exc)}"
            )


    # --------------------------------------------------------
    # Update state without destroying old data
    # --------------------------------------------------------

    with state_lock:

        gold = state["gold"]


        if candle_data:

            gold["candles"].update(
                candle_data
            )

            gold["timeframes"].update(
                timeframe_data
            )


        # ----------------------------------------------------
        # Determine best primary timeframe
        # ----------------------------------------------------

        primary = None

        for preferred in [
            "5min",
            "15min",
            "1h",
            "4h",
        ]:

            if preferred in timeframe_data:

                primary = timeframe_data[
                    preferred
                ]

                break


        if primary:

            gold["trend"] = primary.get(
                "trend",
                "WAITING"
            )

            gold["momentum"] = primary.get(
                "momentum",
                "WAITING"
            )

            gold["structure"] = primary.get(
                "structure",
                "WAITING"
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
                "NONE"
            )


        # ----------------------------------------------------
        # AI
        # ----------------------------------------------------

        if any_candles:

            ai = build_ai_analysis(
                gold["timeframes"]
            )

            gold["signal"] = ai[
                "signal"
            ]

            gold["confidence"] = ai[
                "confidence"
            ]

            gold["score"] = ai[
                "score"
            ]

            gold["data_status"] = (
                "LIVE CANDLE DATA"
            )

            gold["data_error"] = None


            # Use 5m analysis for trade plan.
            plan_analysis = gold[
                "timeframes"
            ].get(
                "5min",
                primary or {}
            )


            gold["trade_plan"] = (
                build_trade_plan(
                    gold["signal"],
                    plan_analysis,
                )
            )


            record_signal_history(
                gold["signal"],
                gold["confidence"],
            )

        else:

            # IMPORTANT:
            # Do not pretend we have analysis.
            gold["signal"] = "WAIT"
            gold["confidence"] = 50
            gold["score"] = 0

            gold["data_status"] = (
                "WAITING FOR CANDLE DATA"
            )

            if api_is_in_cooldown():

                remaining = max(
                    0,
                    int(
                        api_cooldown_until
                        - time.time()
                    )
                )

                gold["data_error"] = (
                    "Twelve Data candle API "
                    f"cooldown active "
                    f"({remaining // 60}m remaining)."
                )

            else:

                gold["data_error"] = (
                    "Waiting for Twelve Data candle data."
                )

            gold["trade_plan"] = {
                "entry": None,
                "stop_loss": None,
                "target1": None,
                "target2": None,
                "target3": None,
                "rr": None,
                "invalidation": None,
                "setup": "WAIT",
                "why": (
                    "Waiting for sufficient "
                    "candle data."
                ),
            }


# ============================================================
# ANALYSIS WORKER
# ============================================================

def gold_analysis_loop():

    print(
        "Gold analysis worker started."
    )

    while True:

        try:

            run_gold_analysis()

        except Exception as exc:

            print(
                "GOLD ANALYSIS LOOP ERROR:",
                repr(exc),
            )

        # Keep worker alive.
        time.sleep(30)


# ============================================================
# GOLD WEBSOCKET
# ============================================================

def gold_websocket_loop():

    while True:

        ws = None

        try:

            print(
                "Connecting Twelve Data Gold WebSocket..."
            )

            ws = websocket.create_connection(
                WS_URL,
                timeout=20,
            )

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL,
                },
            }

            ws.send(
                json.dumps(
                    subscribe_message
                )
            )

            with state_lock:
                state["gold"][
                    "connection"
                ] = "CONNECTED"


            while True:

                raw = ws.recv()

                if not raw:
                    continue

                try:

                    message = json.loads(raw)

                except Exception:

                    continue


                print(
                    "GOLD WS MESSAGE:",
                    message
                )


                if (
                    message.get("event")
                    == "price"
                ):

                    price = safe_float(
                        message.get("price")
                    )

                    if price is not None:

                        with state_lock:

                            state["gold"][
                                "price"
                            ] = price

                            state["gold"][
                                "updated"
                            ] = now_string()


                        print(
                            "STATE UPDATED: "
                            f"GOLD = {price}"
                        )


                elif message.get(
                    "event"
                ) == "heartbeat":

                    continue


        except Exception as exc:

            print(
                "GOLD WS ERROR:",
                repr(exc)
            )

            with state_lock:
                state["gold"][
                    "connection"
                ] = "RECONNECTING"


        finally:

            try:

                if ws:
                    ws.close()

            except Exception:

                pass


        time.sleep(5)


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    global workers_started

    if workers_started:
        return

    workers_started = True

    print(
        "Starting Trading-AI workers..."
    )


    threading.Thread(
        target=gold_websocket_loop,
        daemon=True,
    ).start()


    threading.Thread(
        target=gold_analysis_loop,
        daemon=True,
    ).start()


# ============================================================
# HTML
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

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #05070b;
    color: #f4f7fb;
    font-family: Arial, Helvetica, sans-serif;
}

.container {
    max-width: 1500px;
    margin: auto;
    padding: 28px;
}

.header {
    margin-bottom: 22px;
}

.title {
    font-size: 34px;
    font-weight: 800;
}

.subtitle {
    margin-top: 5px;
    color: #8f9aaa;
    font-size: 15px;
}

.card {
    background: #0b0f16;
    border: 1px solid #1b2230;
    border-radius: 16px;
    padding: 20px;
    margin-bottom: 18px;
}

.asset-title {
    font-size: 22px;
    font-weight: 800;
}

.price {
    font-size: 40px;
    font-weight: 800;
    margin-top: 10px;
}

.live {
    color: #31d17c;
    font-size: 14px;
    font-weight: 700;
}

.connection {
    color: #8993a4;
    margin-top: 7px;
}

.data-status {
    margin-top: 10px;
    color: #e6b84c;
    font-size: 13px;
    font-weight: 700;
}

.grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-top: 20px;
}

.metric {
    background: #080c12;
    border: 1px solid #1b2230;
    border-radius: 12px;
    padding: 14px;
}

.metric-name {
    color: #778194;
    font-size: 12px;
    font-weight: 700;
}

.metric-value {
    margin-top: 7px;
    font-size: 17px;
    font-weight: 800;
}

.section-title {
    color: #aeb7c6;
    font-size: 12px;
    font-weight: 800;
    letter-spacing: .08em;
    margin-bottom: 10px;
}

.signal {
    font-size: 32px;
    font-weight: 900;
}

.confidence {
    color: #9ca7b8;
    margin-top: 5px;
}

.score {
    margin-top: 6px;
    color: #c7d0dc;
}

.timeframes {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 16px 0;
}

.tf {
    background: #111722;
    border: 1px solid #263043;
    color: #bdc6d4;
    border-radius: 8px;
    padding: 8px 12px;
    cursor: pointer;
}

.tf.active {
    background: #202b3d;
    color: white;
    border-color: #4b5c78;
}

#chart {
    width: 100%;
    height: 460px;
}

.chart-empty {
    height: 460px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #7e899a;
    border: 1px dashed #252d3b;
    border-radius: 12px;
}

.setup-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
}

.setup-item {
    background: #080c12;
    border: 1px solid #1b2230;
    border-radius: 12px;
    padding: 15px;
}

.setup-label {
    color: #778194;
    font-size: 12px;
}

.setup-value {
    font-size: 18px;
    font-weight: 800;
    margin-top: 7px;
}

.history {
    color: #8993a4;
}

.oil {
    opacity: .95;
}

@media(max-width: 900px) {

    .grid {
        grid-template-columns: repeat(2, 1fr);
    }

    .setup-grid {
        grid-template-columns: repeat(2, 1fr);
    }
}

@media(max-width: 600px) {

    .container {
        padding: 14px;
    }

    .title {
        font-size: 27px;
    }

    .grid {
        grid-template-columns: 1fr 1fr;
    }

    #chart {
        height: 360px;
    }

    .setup-grid {
        grid-template-columns: 1fr;
    }
}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <div class="title">
            Trading AI
        </div>

        <div class="subtitle">
            Real-Time Market Intelligence • Explainable Multi-Timeframe Analysis
        </div>

    </div>


    <div class="card">

        <div class="asset-title">
            🥇 Gold — XAU/USD
        </div>

        <div
            id="price"
            class="price"
        >
            —
        </div>

        <div class="live">
            ● LIVE
        </div>

        <div
            id="connection"
            class="connection"
        >
            Connection: —
        </div>

        <div
            id="dataStatus"
            class="data-status"
        >
            Data: —
        </div>


        <div class="grid">

            <div class="metric">
                <div class="metric-name">
                    TREND
                </div>
                <div
                    id="trend"
                    class="metric-value"
                >
                    WAITING
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    MOMENTUM
                </div>
                <div
                    id="momentum"
                    class="metric-value"
                >
                    WAITING
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    STRUCTURE
                </div>
                <div
                    id="structure"
                    class="metric-value"
                >
                    WAITING
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    RSI
                </div>
                <div
                    id="rsi"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    EMA 20
                </div>
                <div
                    id="ema20"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    EMA 50
                </div>
                <div
                    id="ema50"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    SUPPORT
                </div>
                <div
                    id="support"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    RESISTANCE
                </div>
                <div
                    id="resistance"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    LIQUIDITY HIGH
                </div>
                <div
                    id="liquidityHigh"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    LIQUIDITY LOW
                </div>
                <div
                    id="liquidityLow"
                    class="metric-value"
                >
                    —
                </div>
            </div>


            <div class="metric">
                <div class="metric-name">
                    SWEEP
                </div>
                <div
                    id="sweep"
                    class="metric-value"
                >
                    NONE
                </div>
            </div>

        </div>

    </div>


    <div class="card">

        <div class="section-title">
            TRADING-AI CONCLUSION
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
            Confidence: 50%
        </div>

        <div
            id="score"
            class="score"
        >
            AI SCORE: 0
        </div>


        <div
            id="timeframes"
            class="timeframes"
        ></div>


        <div
            id="chart"
        ></div>

    </div>


    <div class="card">

        <div class="section-title">
            🎯 AI TRADE SETUP
        </div>

        <div class="setup-grid">

            <div class="setup-item">
                <div class="setup-label">
                    ENTRY
                </div>
                <div
                    id="entry"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    STOP LOSS
                </div>
                <div
                    id="stop"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    TARGET 1
                </div>
                <div
                    id="target1"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    TARGET 2
                </div>
                <div
                    id="target2"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    TARGET 3
                </div>
                <div
                    id="target3"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    R:R
                </div>
                <div
                    id="rr"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    INVALIDATION
                </div>
                <div
                    id="invalidation"
                    class="setup-value"
                >
                    —
                </div>
            </div>


            <div class="setup-item">
                <div class="setup-label">
                    SETUP
                </div>
                <div
                    id="setup"
                    class="setup-value"
                >
                    WAIT
                </div>
            </div>

        </div>


        <div
            id="why"
            style="margin-top:15px;color:#8993a4;"
        >
            Why: Waiting for sufficient candle data.
        </div>

    </div>


    <div class="card">

        <div class="section-title">
            🕐 AI SIGNAL HISTORY
        </div>

        <div
            id="history"
            class="history"
        >
            No signal changes recorded yet.
        </div>

    </div>


    <div class="card oil">

        <div class="asset-title">
            🛢️ Crude Oil — WTI
        </div>

        <div
            style="font-size:32px;font-weight:800;margin-top:10px;"
        >
            —
        </div>

        <div
            style="color:#e2b54d;font-weight:800;margin-top:5px;"
        >
            ● PLAN LIMIT
        </div>

        <div class="connection">
            Connection: PLAN LIMIT
        </div>

        <div
            style="color:#8993a4;margin-top:8px;"
        >
            WTI/USD is not available on the current Twelve Data plan.
        </div>

    </div>


    <div
        style="text-align:center;color:#687386;font-size:12px;padding:15px;"
    >
        LIVE MARKET DATA • TRADING-AI
    </div>

</div>


<script>

let currentTF = "5min";

let chart = null;
let candleSeries = null;


function fmt(value) {

    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "—";
    }

    const n = Number(value);

    if (!Number.isFinite(n)) {
        return "—";
    }

    return n.toFixed(3);
}


function formatTime(value) {

    if (!value) {
        return null;
    }

    const date = new Date(
        String(value).replace(" ", "T") + "Z"
    );

    if (
        Number.isNaN(
            date.getTime()
        )
    ) {
        return null;
    }

    return Math.floor(
        date.getTime() / 1000
    );
}


function getChartData(candles) {

    if (!Array.isArray(candles)) {
        return [];
    }

    const output = [];

    for (const c of candles) {

        const t = formatTime(
            c.datetime || c.time
        );

        if (!t) {
            continue;
        }

        const open = Number(c.open);
        const high = Number(c.high);
        const low = Number(c.low);
        const close = Number(c.close);

        if (
            !Number.isFinite(open) ||
            !Number.isFinite(high) ||
            !Number.isFinite(low) ||
            !Number.isFinite(close)
        ) {
            continue;
        }

        output.push({
            time: t,
            open: open,
            high: high,
            low: low,
            close: close
        });
    }

    // Remove duplicate timestamps.
    const map = new Map();

    for (const c of output) {
        map.set(c.time, c);
    }

    return Array.from(
        map.values()
    ).sort(
        (a, b) => a.time - b.time
    );
}


function renderChart(candles) {

    const container =
        document.getElementById("chart");

    container.innerHTML = "";

    const data = getChartData(
        candles
    );


    if (!data.length) {

        const empty =
            document.createElement("div");

        empty.className =
            "chart-empty";

        empty.innerText =
            "No candle data available yet. Waiting for Twelve Data candle data...";

        container.appendChild(
            empty
        );

        chart = null;
        candleSeries = null;

        return;
    }


    chart =
        LightweightCharts.createChart(
            container,
            {
                layout: {
                    background: {
                        color: "#0b0f16"
                    },
                    textColor: "#8f9aaa"
                },

                grid: {
                    vertLines: {
                        color: "#151b25"
                    },

                    horzLines: {
                        color: "#151b25"
                    }
                },

                width:
                    container.clientWidth,

                height:
                    container.clientHeight,

                timeScale: {
                    borderColor:
                        "#252d3b"
                },

                rightPriceScale: {
                    borderColor:
                        "#252d3b"
                }
            }
        );


    candleSeries =
        chart.addCandlestickSeries(
            {
                upColor: "#31d17c",
                downColor: "#ef596f",
                borderUpColor: "#31d17c",
                borderDownColor: "#ef596f",
                wickUpColor: "#31d17c",
                wickDownColor: "#ef596f"
            }
        );


    candleSeries.setData(
        data
    );


    chart.timeScale().fitContent();
}


function setText(id, value) {

    const element =
        document.getElementById(id);

    if (element) {
        element.innerText =
            value;
    }
}


function renderTimeframes() {

    const box =
        document.getElementById(
            "timeframes"
        );

    box.innerHTML = "";


    const tfs = [
        ["1min", "1m"],
        ["5min", "5m"],
        ["15min", "15m"],
        ["30min", "30m"],
        ["1h", "1H"],
        ["4h", "4H"],
        ["1day", "1D"],
        ["1week", "1W"],
        ["1month", "1M"]
    ];


    for (const [key, label] of tfs) {

        const button =
            document.createElement(
                "button"
            );

        button.className =
            "tf" +
            (
                currentTF === key
                    ? " active"
                    : ""
            );

        button.innerText =
            label;

        button.onclick = () => {

            currentTF = key;

            renderTimeframes();

            // If data exists, immediately draw it.
            if (
                window.lastMarketData &&
                window.lastMarketData.gold
            ) {

                renderChart(
                    (
                        window.lastMarketData
                            .gold
                            .candles || {}
                    )[currentTF] || []
                );

            }

        };

        box.appendChild(
            button
        );
    }
}


function render(data) {

    window.lastMarketData =
        data;


    const g = data.gold || {};

    const plan =
        g.trade_plan || {};


    setText(
        "price",
        fmt(g.price)
    );


    setText(
        "connection",
        "Connection: " +
        (g.connection || "—") +
        " • Updated " +
        (g.updated || "—")
    );


    setText(
        "dataStatus",
        "Data: " +
        (
            g.data_status ||
            "WAITING FOR CANDLE DATA"
        )
    );


    setText(
        "trend",
        g.trend || "WAITING"
    );

    setText(
        "momentum",
        g.momentum || "WAITING"
    );

    setText(
        "structure",
        g.structure || "WAITING"
    );


    setText(
        "rsi",
        g.rsi == null
            ? "—"
            : Number(g.rsi).toFixed(2)
    );


    setText(
        "ema20",
        fmt(g.ema20)
    );

    setText(
        "ema50",
        fmt(g.ema50)
    );

    setText(
        "support",
        fmt(g.support)
    );

    setText(
        "resistance",
        fmt(g.resistance)
    );

    setText(
        "liquidityHigh",
        fmt(g.liquidity_high)
    );

    setText(
        "liquidityLow",
        fmt(g.liquidity_low)
    );

    setText(
        "sweep",
        g.sweep || "NONE"
    );


    setText(
        "signal",
        g.signal || "WAIT"
    );


    setText(
        "confidence",
        "Confidence: " +
        (
            g.confidence ?? 50
        ) +
        "%"
    );


    setText(
        "score",
        "AI SCORE: " +
        (
            g.score ?? 0
        )
    );


    setText(
        "entry",
        fmt(plan.entry)
    );

    setText(
        "stop",
        fmt(plan.stop_loss)
    );

    setText(
        "target1",
        fmt(plan.target1)
    );

    setText(
        "target2",
        fmt(plan.target2)
    );

    setText(
        "target3",
        fmt(plan.target3)
    );


    setText(
        "rr",
        plan.rr || "—"
    );


    setText(
        "invalidation",
        plan.invalidation || "—"
    );


    setText(
        "setup",
        plan.setup || "WAIT"
    );


    setText(
        "why",
        "Why: " +
        (
            plan.why ||
            "Waiting for sufficient candle data."
        )
    );


    // History
    const history =
        g.signal_history || [];


    if (!history.length) {

        setText(
            "history",
            "No signal changes recorded yet."
        );

    } else {

        setText(
            "history",
            history
                .slice()
                .reverse()
                .map(
                    h =>
                        `${h.time} — ${h.signal} — ${h.confidence}%`
                )
                .join("\n")
        );
    }


    renderChart(
        (
            g.candles || {}
        )[currentTF] || []
    );
}


async function fetchMarket() {

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


        render(data);

    } catch (error) {

        console.error(
            "MARKET FETCH ERROR",
            error
        );

    }

}


renderTimeframes();

fetchMarket();

setInterval(
    fetchMarket,
    5000
);


window.addEventListener(
    "resize",
    () => {

        if (
            chart &&
            candleSeries
        ) {

            const container =
                document.getElementById(
                    "chart"
                );

            chart.resize(
                container.clientWidth,
                container.clientHeight
            );

        }

    }
);

</script>

</body>
</html>
"""


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

    with state_lock:

        payload = json.loads(
            json.dumps(
                state
            )
        )

    return jsonify(
        payload
    )


@app.route("/stream")
def stream():

    start_workers()

    def event_stream():

        while True:

            with state_lock:

                payload = json.dumps(
                    state
                )

            yield (
                "data: "
                + payload
                + "\n\n"
            )

            time.sleep(2)


    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/health")
def health():

    with state_lock:

        return jsonify(
            {
                "status": "ok",
                "gold_websocket":
                    state["gold"]["connection"],
                "gold_price":
                    state["gold"]["price"],
                "candle_intervals":
                    list(
                        candle_cache.keys()
                    ),
                "api_cooldown":
                    api_is_in_cooldown(),
            }
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    start_workers()

    app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True,
        debug=False,
    )
