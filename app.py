from flask import Flask, render_template_string, Response
import os
import json
import time
import threading
import queue
import websocket
import requests

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

latest = {
    "gold": {
        "price": None,
        "status": "CONNECTING"
    },
    "oil": {
        "price": None,
        "status": "DATA SOURCE REQUIRED"
    }
}

history = []

clients = []
clients_lock = threading.Lock()
state_lock = threading.Lock()
engine_lock = threading.Lock()
candle_lock = threading.Lock()

engine_started = False


# =========================================================
# CANDLE DATA
# =========================================================

CANDLE_INTERVALS = {
    "5M": "5min",
    "15M": "15min",
    "1H": "1h",
    "1D": "1day"
}

candle_data = {
    "5M": [],
    "15M": [],
    "1H": [],
    "1D": []
}


# =========================================================
# BROADCAST
# =========================================================

def broadcast(data):

    message = json.dumps(
        data,
        separators=(",", ":")
    )

    with clients_lock:

        for client_queue in list(clients):

            try:
                client_queue.put_nowait(message)

            except Exception:
                pass


# =========================================================
# EMA
# =========================================================

def calculate_ema(values, period):

    if not values:
        return None

    if len(values) < period:
        return sum(values) / len(values)

    multiplier = 2 / (period + 1)

    value = sum(values[:period]) / period

    for price in values[period:]:
        value = (
            (price - value) * multiplier
            + value
        )

    return value


# =========================================================
# RSI
# =========================================================

def calculate_rsi(values):

    if len(values) < 2:
        return None

    changes = []

    for i in range(1, len(values)):
        changes.append(
            values[i] - values[i - 1]
        )

    gains = [
        x for x in changes
        if x > 0
    ]

    losses = [
        abs(x) for x in changes
        if x < 0
    ]

    avg_gain = (
        sum(gains) / len(gains)
        if gains else 0
    )

    avg_loss = (
        sum(losses) / len(losses)
        if losses else 0
    )

    if avg_loss == 0:

        if avg_gain == 0:
            return 50

        return 100

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


# =========================================================
# FETCH CANDLES
# =========================================================

def fetch_candles(interval, outputsize=200):

    if not API_KEY:
        return []

    try:

        url = (
            "https://api.twelvedata.com/time_series"
        )

        params = {
            "symbol": "XAU/USD",
            "interval": interval,
            "outputsize": outputsize,
            "order": "asc",
            "timezone": "UTC",
            "apikey": API_KEY
        }

        response = requests.get(
            url,
            params=params,
            timeout=15
        )

        data = response.json()

        if data.get("status") == "error":

            print(
                "CANDLE API ERROR:",
                data,
                flush=True
            )

            return []

        values = data.get(
            "values",
            []
        )

        candles = []

        for row in values:

            try:

                candles.append({

                    "datetime":
                        row.get("datetime"),

                    "open":
                        float(row.get("open")),

                    "high":
                        float(row.get("high")),

                    "low":
                        float(row.get("low")),

                    "close":
                        float(row.get("close"))

                })

            except Exception:
                continue

        return candles

    except Exception as error:

        print(
            "CANDLE FETCH ERROR:",
            error,
            flush=True
        )

        return []


# =========================================================
# UPDATE CANDLES
# =========================================================

def update_all_candles():

    for name, interval in CANDLE_INTERVALS.items():

        candles = fetch_candles(
            interval,
            200
        )

        if candles:

            with candle_lock:

                candle_data[name] = candles

            print(
                "CANDLES:",
                name,
                len(candles),
                flush=True
            )


# =========================================================
# CANDLE WORKER
# =========================================================

def candle_worker():

    print(
        "CANDLE ENGINE STARTING",
        flush=True
    )

    while True:

        try:

            update_all_candles()

            time.sleep(60)

        except Exception as error:

            print(
                "CANDLE ENGINE ERROR:",
                error,
                flush=True
            )

            time.sleep(10)


# =========================================================
# SWING DETECTION
# =========================================================

def find_swing_highs(candles, lookback=2):

    highs = []

    if len(candles) < (
        lookback * 2 + 1
    ):
        return highs

    for i in range(
        lookback,
        len(candles) - lookback
    ):

        current = candles[i]["high"]

        left = [
            candles[j]["high"]
            for j in range(
                i - lookback,
                i
            )
        ]

        right = [
            candles[j]["high"]
            for j in range(
                i + 1,
                i + lookback + 1
            )
        ]

        if (
            current >= max(left)
            and
            current >= max(right)
        ):

            highs.append({
                "price": current,
                "datetime":
                    candles[i]["datetime"]
            })

    return highs


def find_swing_lows(candles, lookback=2):

    lows = []

    if len(candles) < (
        lookback * 2 + 1
    ):
        return lows

    for i in range(
        lookback,
        len(candles) - lookback
    ):

        current = candles[i]["low"]

        left = [
            candles[j]["low"]
            for j in range(
                i - lookback,
                i
            )
        ]

        right = [
            candles[j]["low"]
            for j in range(
                i + 1,
                i + lookback + 1
            )
        ]

        if (
            current <= min(left)
            and
            current <= min(right)
        ):

            lows.append({
                "price": current,
                "datetime":
                    candles[i]["datetime"]
            })

    return lows


# =========================================================
# EQUAL HIGH / LOW DETECTION
# =========================================================

def detect_equal_levels(
    levels,
    tolerance=0.15
):

    groups = []

    for level in levels:

        price = level["price"]

        matched = False

        for group in groups:

            if abs(
                price - group["price"]
            ) <= tolerance:

                group["members"].append(
                    level
                )

                group["price"] = (
                    sum(
                        x["price"]
                        for x in group["members"]
                    )
                    /
                    len(group["members"])
                )

                matched = True

                break

        if not matched:

            groups.append({

                "price": price,

                "members": [level]

            })

    result = []

    for group in groups:

        if len(group["members"]) >= 2:

            result.append({

                "price":
                    round(
                        group["price"],
                        3
                    ),

                "strength":
                    min(
                        100,
                        50 +
                        (
                            len(
                                group["members"]
                            ) * 15
                        )
                    ),

                "type":
                    "EQUAL LEVEL"

            })

    return result


# =========================================================
# LIQUIDITY ANALYSIS
# =========================================================

def calculate_liquidity():

    with candle_lock:

        candles = {
            key: list(value)
            for key, value
            in candle_data.items()
        }

    with state_lock:

        current = latest["gold"]["price"]

    result = {

        "current_price": current,

        "1D": {
            "buy_side": [],
            "sell_side": [],
            "previous_day_high": None,
            "previous_day_low": None
        },

        "1H": {
            "buy_side": [],
            "sell_side": []
        },

        "15M": {
            "buy_side": [],
            "sell_side": []
        },

        "5M": {
            "buy_side": [],
            "sell_side": []
        },

        "nearest_above": None,
        "nearest_below": None,

        "nearest_above_distance": None,
        "nearest_below_distance": None
    }

    if not current:
        return result

    # -----------------------------------------------------
    # 1D PREVIOUS DAY HIGH / LOW
    # -----------------------------------------------------

    daily = candles.get(
        "1D",
        []
    )

    if len(daily) >= 2:

        previous_day = daily[-2]

        pdh = previous_day["high"]
        pdl = previous_day["low"]

        result["1D"][
            "previous_day_high"
        ] = round(pdh, 3)

        result["1D"][
            "previous_day_low"
        ] = round(pdl, 3)

        if pdh > current:

            result["1D"][
                "buy_side"
            ].append({

                "price": round(
                    pdh,
                    3
                ),

                "type":
                    "PREVIOUS DAY HIGH",

                "strength": 90

            })

        if pdl < current:

            result["1D"][
                "sell_side"
            ].append({

                "price": round(
                    pdl,
                    3
                ),

                "type":
                    "PREVIOUS DAY LOW",

                "strength": 90

            })

    # -----------------------------------------------------
    # TIMEFRAME SWINGS
    # -----------------------------------------------------

    for timeframe in [
        "1H",
        "15M",
        "5M"
    ]:

        data = candles.get(
            timeframe,
            []
        )

        if len(data) < 7:
            continue

        recent = data[-100:]

        swing_highs = find_swing_highs(
            recent,
            2
        )

        swing_lows = find_swing_lows(
            recent,
            2
        )

        # -------------------------
        # BUY SIDE
        # -------------------------

        for level in swing_highs[-10:]:

            price = level["price"]

            if price > current:

                result[timeframe][
                    "buy_side"
                ].append({

                    "price":
                        round(
                            price,
                            3
                        ),

                    "type":
                        "SWING HIGH",

                    "strength":
                        70

                })

        # -------------------------
        # SELL SIDE
        # -------------------------

        for level in swing_lows[-10:]:

            price = level["price"]

            if price < current:

                result[timeframe][
                    "sell_side"
                ].append({

                    "price":
                        round(
                            price,
                            3
                        ),

                    "type":
                        "SWING LOW",

                    "strength":
                        70

                })

        # -------------------------
        # EQUAL HIGHS
        # -------------------------

        equal_highs = detect_equal_levels(
            swing_highs,
            0.15
        )

        for level in equal_highs:

            if level["price"] > current:

                result[timeframe][
                    "buy_side"
                ].append({

                    "price":
                        level["price"],

                    "type":
                        "EQUAL HIGHS",

                    "strength":
                        level["strength"]

                })

        # -------------------------
        # EQUAL LOWS
        # -------------------------

        equal_lows = detect_equal_levels(
            swing_lows,
            0.15
        )

        for level in equal_lows:

            if level["price"] < current:

                result[timeframe][
                    "sell_side"
                ].append({

                    "price":
                        level["price"],

                    "type":
                        "EQUAL LOWS",

                    "strength":
                        level["strength"]

                })

    # -----------------------------------------------------
    # SORT LEVELS
    # -----------------------------------------------------

    for timeframe in result:

        if timeframe in [
            "1D",
            "1H",
            "15M",
            "5M"
        ]:

            result[timeframe][
                "buy_side"
            ] = sorted(
                result[timeframe][
                    "buy_side"
                ],
                key=lambda x: x["price"]
            )

            result[timeframe][
                "sell_side"
            ] = sorted(
                result[timeframe][
                    "sell_side"
                ],
                key=lambda x: x["price"],
                reverse=True
            )

    # -----------------------------------------------------
    # NEAREST LIQUIDITY
    # -----------------------------------------------------

    above = []
    below = []

    for timeframe in [
        "1D",
        "1H",
        "15M",
        "5M"
    ]:

        for level in result[
            timeframe
        ]["buy_side"]:

            if level["price"] > current:

                above.append(
                    level["price"]
                )

        for level in result[
            timeframe
        ]["sell_side"]:

            if level["price"] < current:

                below.append(
                    level["price"]
                )

    if above:

        nearest = min(above)

        result["nearest_above"] = nearest

        result[
            "nearest_above_distance"
        ] = round(
            nearest - current,
            3
        )

    if below:

        nearest = max(below)

        result["nearest_below"] = nearest

        result[
            "nearest_below_distance"
        ] = round(
            current - nearest,
            3
        )

    return result


# =========================================================
# LIQUIDITY SWEEP DETECTION
# =========================================================

def detect_liquidity_sweeps():

    with candle_lock:

        candles = {
            key: list(value)
            for key, value
            in candle_data.items()
        }

    result = {
        "5M": "NONE",
        "15M": "NONE",
        "1H": "NONE",
        "1D": "NONE"
    }

    for timeframe in [
        "5M",
        "15M",
        "1H",
        "1D"
    ]:

        data = candles.get(
            timeframe,
            []
        )

        if len(data) < 5:
            continue

        recent = data[-5:]

        previous = data[-2]

        latest_candle = data[-1]

        previous_high = previous["high"]
        previous_low = previous["low"]

        latest_high = latest_candle["high"]
        latest_low = latest_candle["low"]
        latest_close = latest_candle["close"]

        if (
            latest_high > previous_high
            and
            latest_close < previous_high
        ):

            result[timeframe] = (
                "BUY-SIDE LIQUIDITY SWEPT"
            )

        elif (
            latest_low < previous_low
            and
            latest_close > previous_low
        ):

            result[timeframe] = (
                "SELL-SIDE LIQUIDITY SWEPT"
            )

    return result


# =========================================================
# ANALYSIS ENGINE
# =========================================================

def calculate_analysis():

    with state_lock:

        prices = list(history)

    if len(prices) < 10:

        return {

            "trend": "CALCULATING",

            "momentum": "CALCULATING",

            "structure": "BUILDING DATA",

            "support": None,

            "resistance": None,

            "ema20": None,

            "ema50": None,

            "rsi": None,

            "signal": "WAIT",

            "confidence": 0,

            "summary":
                "Collecting market data..."

        }

    recent = prices[-100:]

    ema20 = calculate_ema(
        recent,
        20
    )

    ema50 = calculate_ema(
        recent,
        50
    )

    rsi = calculate_rsi(
        recent
    )

    support = min(
        recent
    )

    resistance = max(
        recent
    )

    current = recent[-1]

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

    if current > ema20 > ema50:

        trend = "BULLISH"

    elif current < ema20 < ema50:

        trend = "BEARISH"

    else:

        trend = "SIDEWAYS"

    # -----------------------------------------------------
    # MOMENTUM
    # -----------------------------------------------------

    if rsi >= 60:

        momentum = "STRONG BUYING"

    elif rsi >= 52:

        momentum = "BUYING"

    elif rsi <= 40:

        momentum = "STRONG SELLING"

    elif rsi <= 48:

        momentum = "SELLING"

    else:

        momentum = "NEUTRAL"

    # -----------------------------------------------------
    # STRUCTURE
    # -----------------------------------------------------

    if len(recent) >= 10:

        old = sum(
            recent[-10:-5]
        ) / 5

        new = sum(
            recent[-5:]
        ) / 5

        if new > old:

            structure = "HIGHER"

        elif new < old:

            structure = "LOWER"

        else:

            structure = "RANGE"

    else:

        structure = "BUILDING"

    # -----------------------------------------------------
    # LIQUIDITY
    # -----------------------------------------------------

    liquidity = calculate_liquidity()

    sweeps = detect_liquidity_sweeps()

    # -----------------------------------------------------
    # SCORE
    # -----------------------------------------------------

    score = 0

    if trend == "BULLISH":
        score += 2

    elif trend == "BEARISH":
        score -= 2

    if momentum == "STRONG BUYING":
        score += 2

    elif momentum == "BUYING":
        score += 1

    elif momentum == "STRONG SELLING":
        score -= 2

    elif momentum == "SELLING":
        score -= 1

    if structure == "HIGHER":
        score += 1

    elif structure == "LOWER":
        score -= 1

    # -----------------------------------------------------
    # SIGNAL
    # -----------------------------------------------------

    if score >= 4:

        signal = "BUY"

        confidence = min(
            90,
            60 + score * 5
        )

    elif score <= -4:

        signal = "SELL"

        confidence = min(
            90,
            60 + abs(score) * 5
        )

    else:

        signal = "WAIT"

        confidence = 50

    # -----------------------------------------------------
    # SUMMARY
    # -----------------------------------------------------

    if signal == "BUY":

        summary = (
            "Buying pressure is stronger. "
            "Wait for liquidity and structure "
            "confirmation before acting."
        )

    elif signal == "SELL":

        summary = (
            "Selling pressure is stronger. "
            "Wait for liquidity and structure "
            "confirmation before acting."
        )

    else:

        summary = (
            "Market conditions are mixed. "
            "Waiting for stronger confirmation."
        )

    return {

        "trend":
            trend,

        "momentum":
            momentum,

        "structure":
            structure,

        "support":
            round(
                support,
                3
            ),

        "resistance":
            round(
                resistance,
                3
            ),

        "ema20":
            round(
                ema20,
                3
            ),

        "ema50":
            round(
                ema50,
                3
            ),

        "rsi":
            round(
                rsi,
                2
            ),

        "signal":
            signal,

        "confidence":
            confidence,

        "summary":
            summary,

        "liquidity":
            liquidity,

        "sweeps":
            sweeps
    }


# =========================================================
# UPDATE GOLD
# =========================================================

def update_gold(price):

    with state_lock:

        latest["gold"]["price"] = price

        latest["gold"]["status"] = "LIVE"

        history.append(price)

        if len(history) > 500:

            del history[:-500]

    analysis = calculate_analysis()

    broadcast({

        "type":
            "gold_update",

        "price":
            price,

        "analysis":
            analysis,

        "timestamp":
            time.time()

    })


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_worker():

    print(
        "GOLD ENGINE STARTING",
        flush=True
    )

    if not API_KEY:

        print(
            "TWELVE_DATA_API_KEY MISSING",
            flush=True
        )

        return

    while True:

        try:

            url = (
                "wss://ws.twelvedata.com/v1/quotes/price?apikey="
                + API_KEY
            )

            def on_open(ws):

                print(
                    "TWELVE DATA GOLD CONNECTED",
                    flush=True
                )

                subscribe = {

                    "action":
                        "subscribe",

                    "params": {
                        "symbols":
                            "XAU/USD"
                    }

                }

                ws.send(
                    json.dumps(
                        subscribe
                    )
                )

            def on_message(
                ws,
                message
            ):

                try:

                    data = json.loads(
                        message
                    )

                    if (
                        data.get("event")
                        ==
                        "price"
                    ):

                        price = data.get(
                            "price"
                        )

                        if price is not None:

                            print(
                                "GOLD:",
                                price,
                                flush=True
                            )

                            update_gold(
                                float(price)
                            )

                    elif (
                        data.get("event")
                        ==
                        "subscribe-status"
                    ):

                        print(
                            "GOLD SUBSCRIPTION:",
                            data,
                            flush=True
                        )

                except Exception as error:

                    print(
                        "GOLD MESSAGE ERROR:",
                        error,
                        flush=True
                    )

            def on_error(
                ws,
                error
            ):

                print(
                    "GOLD WEBSOCKET ERROR:",
                    error,
                    flush=True
                )

            def on_close(
                ws,
                code,
                message
            ):

                print(
                    "GOLD WEBSOCKET CLOSED:",
                    code,
                    message,
                    flush=True
                )

            ws = websocket.WebSocketApp(

                url,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            ws.run_forever(

                ping_interval=20,

                ping_timeout=10

            )

        except Exception as error:

            print(
                "GOLD ENGINE ERROR:",
                error,
                flush=True
            )

        time.sleep(5)


# =========================================================
# OIL PLACEHOLDER
# =========================================================

def oil_worker():

    print(
        "OIL ENGINE STARTING",
        flush=True
    )


# =========================================================
# START ENGINE
# =========================================================

def start_live_engine():

    global engine_started

    with engine_lock:

        if engine_started:
            return

        engine_started = True

        threading.Thread(
            target=gold_worker,
            daemon=True
        ).start()

        threading.Thread(
            target=oil_worker,
            daemon=True
        ).start()

        threading.Thread(
            target=candle_worker,
            daemon=True
        ).start()

        print(
            "LIVE ENGINE STARTED",
            flush=True
        )


@app.before_request
def start_engine():

    start_live_engine()


# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1">

<title>Trading-AI</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background: #05070b;

    color: white;

    font-family: Arial, sans-serif;
}

.header {

    text-align: center;

    padding: 25px;

    border-bottom:
    1px solid #202631;
}

.logo {

    font-size: 30px;

    font-weight: bold;
}

.subtitle {

    color: #8d96a5;

    margin-top: 7px;
}

.container {

    max-width: 1150px;

    margin: auto;

    padding: 25px;
}

.price-card {

    background: #0d1118;

    border:
    1px solid #202631;

    border-radius: 18px;

    padding: 25px;

    margin-bottom: 20px;
}

.asset {

    font-size: 22px;

    font-weight: bold;
}

.price {

    font-size: 42px;

    font-weight: bold;

    margin-top: 15px;
}

.live {

    color: #45e08b;

    margin-top: 10px;
}

.analysis {

    display: grid;

    grid-template-columns:
    repeat(
        auto-fit,
        minmax(180px, 1fr)
    );

    gap: 15px;

    margin-top: 20px;
}

.box {

    background: #111722;

    border:
    1px solid #202631;

    border-radius: 14px;

    padding: 18px;
}

.label {

    color: #8d96a5;

    font-size: 13px;

    text-transform: uppercase;
}

.value {

    font-size: 20px;

    font-weight: bold;

    margin-top: 8px;
}

.signal {

    margin-top: 20px;

    padding: 20px;

    border:
    1px solid #202631;

    border-radius: 15px;

    background: #111722;
}

.signal-title {

    color: #8d96a5;

    font-size: 13px;
}

.signal-value {

    font-size: 30px;

    font-weight: bold;

    margin-top: 8px;
}

.summary {

    margin-top: 10px;

    color: #c5cbd5;

    line-height: 1.5;
}

.section-title {

    font-size: 22px;

    font-weight: bold;

    margin-top: 30px;

    margin-bottom: 15px;
}

.liquidity-grid {

    display: grid;

    grid-template-columns:
    repeat(
        auto-fit,
        minmax(220px, 1fr)
    );

    gap: 15px;
}

.liquidity-card {

    background: #111722;

    border:
    1px solid #202631;

    border-radius: 14px;

    padding: 18px;
}

.liquidity-card h3 {

    margin-top: 0;

    margin-bottom: 15px;

    font-size: 18px;
}

.liquidity-row {

    display: flex;

    justify-content: space-between;

    gap: 10px;

    padding: 8px 0;

    border-bottom:
    1px solid #202631;
}

.liquidity-row:last-child {

    border-bottom: none;
}

.liquidity-label {

    color: #8d96a5;

    font-size: 12px;
}

.liquidity-price {

    font-weight: bold;

    font-size: 15px;
}

.buy {

    color: #ff6b6b;
}

.sell {

    color: #45e08b;
}

.nearest {

    margin-top: 20px;

    display: grid;

    grid-template-columns:
    repeat(
        auto-fit,
        minmax(220px, 1fr)
    );

    gap: 15px;
}

.nearest-box {

    background: #0b1018;

    border:
    1px solid #202631;

    border-radius: 14px;

    padding: 18px;
}

.sweep {

    margin-top: 20px;

    padding: 18px;

    background: #111722;

    border:
    1px solid #202631;

    border-radius: 14px;
}

.small {

    color: #8d96a5;

    font-size: 13px;

    margin-top: 5px;
}

</style>

</head>

<body>

<div class="header">

<div class="logo">
Trading-AI
</div>

<div class="subtitle">
Real-Time Market Intelligence
</div>

</div>

<div class="container">

<div class="price-card">

<div class="asset">
🥇 Gold — XAU/USD
</div>

<div
id="goldPrice"
class="price">
Waiting...
</div>

<div
id="goldStatus"
class="live">
Connecting...
</div>

<div class="analysis">

<div class="box">
<div class="label">Trend</div>
<div id="trend" class="value">--</div>
</div>

<div class="box">
<div class="label">Momentum</div>
<div id="momentum" class="value">--</div>
</div>

<div class="box">
<div class="label">Structure</div>
<div id="structure" class="value">--</div>
</div>

<div class="box">
<div class="label">RSI</div>
<div id="rsi" class="value">--</div>
</div>

<div class="box">
<div class="label">EMA 20</div>
<div id="ema20" class="value">--</div>
</div>

<div class="box">
<div class="label">EMA 50</div>
<div id="ema50" class="value">--</div>
</div>

<div class="box">
<div class="label">Support</div>
<div id="support" class="value">--</div>
</div>

<div class="box">
<div class="label">Resistance</div>
<div id="resistance" class="value">--</div>
</div>

</div>

<div class="signal">

<div class="signal-title">
TRADING-AI ANALYSIS
</div>

<div
id="signal"
class="signal-value">
WAIT
</div>

<div
id="confidence"
class="value">
Confidence: --
</div>

<div
id="summary"
class="summary">
Collecting market data...
</div>

</div>

</div>


<!-- =====================================================
     LIQUIDITY MAP
===================================================== -->

<div class="section-title">
💧 Liquidity Map
</div>

<div class="liquidity-grid">

<div class="liquidity-card">

<h3>1D — Daily</h3>

<div class="liquidity-row">

<span class="liquidity-label">
BUY-SIDE
</span>

<span
id="dBuy"
class="liquidity-price buy">
--
</span>

</div>

<div class="liquidity-row">

<span class="liquidity-label">
SELL-SIDE
</span>

<span
id="dSell"
class="liquidity-price sell">
--
</span>

</div>

<div class="small">
Previous Day High / Low
</div>

</div>


<div class="liquidity-card">

<h3>1H — Hourly</h3>

<div class="liquidity-row">

<span class="liquidity-label">
BUY-SIDE
</span>

<span
id="hBuy"
class="liquidity-price buy">
--
</span>

</div>

<div class="liquidity-row">

<span class="liquidity-label">
SELL-SIDE
</span>

<span
id="hSell"
class="liquidity-price sell">
--
</span>

</div>

</div>


<div class="liquidity-card">

<h3>15M</h3>

<div class="liquidity-row">

<span class="liquidity-label">
BUY-SIDE
</span>

<span
id="m15Buy"
class="liquidity-price buy">
--
</span>

</div>

<div class="liquidity-row">

<span class="liquidity-label">
SELL-SIDE
</span>

<span
id="m15Sell"
class="liquidity-price sell">
--
</span>

</div>

</div>


<div class="liquidity-card">

<h3>5M</h3>

<div class="liquidity-row">

<span class="liquidity-label">
BUY-SIDE
</span>

<span
id="m5Buy"
class="liquidity-price buy">
--
</span>

</div>

<div class="liquidity-row">

<span class="liquidity-label">
SELL-SIDE
</span>

<span
id="m5Sell"
class="liquidity-price sell">
--
</span>

</div>

</div>

</div>


<div class="nearest">

<div class="nearest-box">

<div class="label">
NEAREST BUY-SIDE LIQUIDITY
</div>

<div
id="nearestAbove"
class="value">
--
</div>

<div
id="aboveDistance"
class="small">
--
</div>

</div>

<div class="nearest-box">

<div class="label">
NEAREST SELL-SIDE LIQUIDITY
</div>

<div
id="nearestBelow"
class="value">
--
</div>

<div
id="belowDistance"
class="small">
--
</div>

</div>

</div>


<div class="sweep">

<div class="label">
LIQUIDITY SWEEP STATUS
</div>

<div
id="sweep5"
class="value">
5M: --
</div>

<div
id="sweep15"
class="value">
15M: --
</div>

<div
id="sweep1h"
class="value">
1H: --
</div>

<div
id="sweep1d"
class="value">
1D: --
</div>

</div>


<div class="price-card">

<div class="asset">
🛢️ Crude Oil — WTI
</div>

<div class="price">
--
</div>

<div class="live">
DATA SOURCE REQUIRED
</div>

</div>

</div>


<script>

const goldPrice =
document.getElementById(
    "goldPrice"
);

const goldStatus =
document.getElementById(
    "goldStatus"
);

const trend =
document.getElementById(
    "trend"
);

const momentum =
document.getElementById(
    "momentum"
);

const structure =
document.getElementById(
    "structure"
);

const rsi =
document.getElementById(
    "rsi"
);

const ema20 =
document.getElementById(
    "ema20"
);

const ema50 =
document.getElementById(
    "ema50"
);

const support =
document.getElementById(
    "support"
);

const resistance =
document.getElementById(
    "resistance"
);

const signal =
document.getElementById(
    "signal"
);

const confidence =
document.getElementById(
    "confidence"
);

const summary =
document.getElementById(
    "summary"
);


function firstLevel(
    levels
) {

    if (
        !levels ||
        levels.length === 0
    ) {

        return "--";
    }

    return (
        Number(
            levels[0].price
        ).toFixed(3)
        +
        " "
        +
        "("
        +
        levels[0].type
        +
        ")"
    );
}


function updateLiquidity(
    liquidity,
    sweeps
) {

    if (!liquidity) {
        return;
    }

    document.getElementById(
        "dBuy"
    ).innerText =
        firstLevel(
            liquidity["1D"].buy_side
        );

    document.getElementById(
        "dSell"
    ).innerText =
        firstLevel(
            liquidity["1D"].sell_side
        );

    document.getElementById(
        "hBuy"
    ).innerText =
        firstLevel(
            liquidity["1H"].buy_side
        );

    document.getElementById(
        "hSell"
    ).innerText =
        firstLevel(
            liquidity["1H"].sell_side
        );

    document.getElementById(
        "m15Buy"
    ).innerText =
        firstLevel(
            liquidity["15M"].buy_side
        );

    document.getElementById(
        "m15Sell"
    ).innerText =
        firstLevel(
            liquidity["15M"].sell_side
        );

    document.getElementById(
        "m5Buy"
    ).innerText =
        firstLevel(
            liquidity["5M"].buy_side
        );

    document.getElementById(
        "m5Sell"
    ).innerText =
        firstLevel(
            liquidity["5M"].sell_side
        );


    document.getElementById(
        "nearestAbove"
    ).innerText =
        liquidity.nearest_above === null
        ? "--"
        : Number(
            liquidity.nearest_above
        ).toFixed(3);


    document.getElementById(
        "nearestBelow"
    ).innerText =
        liquidity.nearest_below === null
        ? "--"
        : Number(
            liquidity.nearest_below
        ).toFixed(3);


    document.getElementById(
        "aboveDistance"
    ).innerText =
        liquidity.nearest_above_distance === null
        ? "--"
        :
        "Distance: "
        +
        Number(
            liquidity.nearest_above_distance
        ).toFixed(3);


    document.getElementById(
        "belowDistance"
    ).innerText =
        liquidity.nearest_below_distance === null
        ? "--"
        :
        "Distance: "
        +
        Number(
            liquidity.nearest_below_distance
        ).toFixed(3);


    if (sweeps) {

        document.getElementById(
            "sweep5"
        ).innerText =
            "5M: "
            +
            sweeps["5M"];

        document.getElementById(
            "sweep15"
        ).innerText =
            "15M: "
            +
            sweeps["15M"];

        document.getElementById(
            "sweep1h"
        ).innerText =
            "1H: "
            +
            sweeps["1H"];

        document.getElementById(
            "sweep1d"
        ).innerText =
            "1D: "
            +
            sweeps["1D"];
    }
}


function updateAnalysis(
    data
) {

    goldPrice.innerText =
        Number(
            data.price
        ).toFixed(3);

    goldStatus.innerText =
        "LIVE";

    const a =
        data.analysis;

    trend.innerText =
        a.trend;

    momentum.innerText =
        a.momentum;

    structure.innerText =
        a.structure;

    rsi.innerText =
        a.rsi === null
        ? "--"
        : a.rsi;

    ema20.innerText =
        a.ema20 === null
        ? "--"
        : a.ema20;

    ema50.innerText =
        a.ema50 === null
        ? "--"
        : a.ema50;

    support.innerText =
        a.support === null
        ? "--"
        : a.support;

    resistance.innerText =
        a.resistance === null
        ? "--"
        : a.resistance;

    signal.innerText =
        a.signal;

    confidence.innerText =
        "Confidence: "
        +
        a.confidence
        +
        "%";

    summary.innerText =
        a.summary;

    updateLiquidity(
        a.liquidity,
        a.sweeps
    );
}


const stream =
new EventSource(
    "/stream"
);


stream.onopen =
function() {

    console.log(
        "TRADING-AI STREAM CONNECTED"
    );

};


stream.onmessage =
function(event) {

    try {

        const data =
            JSON.parse(
                event.data
            );

        if (
            data.type ===
            "gold_update"
        ) {

            updateAnalysis(
                data
            );

        }

    }

    catch(error) {

        console.log(
            "STREAM ERROR:",
            error
        );

    }

};

</script>

</body>

</html>
"""


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    return render_template_string(
        HTML
    )


# =========================================================
# STREAM
# =========================================================

@app.route("/stream")
def stream():

    client_queue = queue.Queue()

    with clients_lock:

        clients.append(
            client_queue
        )

    def generate():

        try:

            while True:

                try:

                    message = (
                        client_queue.get(
                            timeout=15
                        )
                    )

                    yield (
                        "data: "
                        +
                        message
                        +
                        "\n\n"
                    )

                except queue.Empty:

                    yield (
                        ": keepalive\n\n"
                    )

        finally:

            with clients_lock:

                if (
                    client_queue
                    in clients
                ):

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

            "X-Accel-Buffering":
                "no",

            "Connection":
                "keep-alive"

        }

    )


# =========================================================
# HEALTH
# =========================================================

@app.route("/health")
def health():

    with candle_lock:

        candle_counts = {
            key: len(value)
            for key, value
            in candle_data.items()
        }

    return {

        "status":
            "ok",

        "live_engine":
            engine_started,

        "api_key":
            bool(API_KEY),

        "gold_history":
            len(history),

        "candles":
            candle_counts

    }


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.environ.get(
                "PORT",
                10000
            )
        ),

        threaded=True

    )
