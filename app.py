from flask import Flask, render_template_string, Response
import os
import json
import time
import threading
import queue
import websocket
import requests
from collections import deque

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

GOLD_SYMBOL = "XAU/USD"

# =========================================================
# LIVE STATE
# =========================================================

latest = {
    "gold": {
        "price": None,
        "history": deque(maxlen=500),
        "analysis": {},
        "timestamp": None
    },
    "oil": {
        "price": None,
        "history": deque(maxlen=500),
        "analysis": {},
        "timestamp": None
    }
}

clients = []
clients_lock = threading.Lock()
state_lock = threading.Lock()

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
# BASIC HELPERS
# =========================================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for price in values[period:]:
        result = ((price - result) * multiplier) + result

    return result


def rsi(values, period=14):
    if len(values) <= period:
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
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# =========================================================
# CANDLE FETCH
# =========================================================

def fetch_candles(interval, outputsize=200):

    if not API_KEY:
        print("TWELVE DATA API KEY MISSING")
        return []

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": GOLD_SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "timezone": "UTC"
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=20
        )

        data = response.json()

        if "values" not in data:
            print("CANDLE ERROR:", data)
            return []

        candles = []

        for row in reversed(data["values"]):

            try:
                candles.append({
                    "datetime": row["datetime"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0))
                })

            except Exception:
                continue

        print(
            f"CANDLES: {interval.upper()} {len(candles)}"
        )

        return candles

    except Exception as e:

        print(
            f"CANDLE FETCH ERROR {interval}:",
            e
        )

        return []


def update_all_candles():

    for key, interval in CANDLE_INTERVALS.items():

        candles = fetch_candles(
            interval,
            200
        )

        if candles:

            with state_lock:
                candle_data[key] = candles


def candle_worker():

    print("CANDLE ENGINE STARTING")

    while True:

        try:
            update_all_candles()

        except Exception as e:
            print("CANDLE ENGINE ERROR:", e)

        time.sleep(60)


# =========================================================
# LIQUIDITY ENGINE
# =========================================================

def swing_highs(candles):

    levels = []

    if len(candles) < 5:
        return levels

    for i in range(2, len(candles) - 2):

        h = candles[i]["high"]

        if (
            h > candles[i - 1]["high"]
            and h > candles[i - 2]["high"]
            and h > candles[i + 1]["high"]
            and h > candles[i + 2]["high"]
        ):
            levels.append(h)

    return levels


def swing_lows(candles):

    levels = []

    if len(candles) < 5:
        return levels

    for i in range(2, len(candles) - 2):

        l = candles[i]["low"]

        if (
            l < candles[i - 1]["low"]
            and l < candles[i - 2]["low"]
            and l < candles[i + 1]["low"]
            and l < candles[i + 2]["low"]
        ):
            levels.append(l)

    return levels


def group_levels(levels, tolerance=0.15):

    if not levels:
        return []

    levels = sorted(levels)

    groups = []
    current = [levels[0]]

    for level in levels[1:]:

        if abs(level - current[-1]) <= tolerance:
            current.append(level)

        else:
            groups.append(current)
            current = [level]

    groups.append(current)

    result = []

    for group in groups:

        result.append(
            round(sum(group) / len(group), 3)
        )

    return result


def calculate_liquidity(price):

    result = {
        "1D": {},
        "1H": {},
        "15M": {},
        "5M": {},
        "nearest_buy": None,
        "nearest_sell": None,
        "buy_distance": None,
        "sell_distance": None
    }

    all_buy = []
    all_sell = []

    # -----------------------------------------------------
    # DAILY
    # -----------------------------------------------------

    daily = candle_data.get("1D", [])

    if len(daily) >= 2:

        previous_day = daily[-2]

        result["1D"] = {
            "buy_side": [
                round(previous_day["high"], 3)
            ],
            "sell_side": [
                round(previous_day["low"], 3)
            ]
        }

    # -----------------------------------------------------
    # OTHER TIMEFRAMES
    # -----------------------------------------------------

    for tf in ["1H", "15M", "5M"]:

        candles = candle_data.get(tf, [])

        highs = swing_highs(candles)
        lows = swing_lows(candles)

        highs = group_levels(highs)
        lows = group_levels(lows)

        buy_levels = [
            x for x in highs
            if price is not None and x > price
        ]

        sell_levels = [
            x for x in lows
            if price is not None and x < price
        ]

        result[tf] = {
            "buy_side": sorted(buy_levels),
            "sell_side": sorted(
                sell_levels,
                reverse=True
            )
        }

        all_buy.extend(buy_levels)
        all_sell.extend(sell_levels)

    # Daily liquidity
    if "1D" in result:

        for x in result["1D"].get("buy_side", []):
            if price is not None and x > price:
                all_buy.append(x)

        for x in result["1D"].get("sell_side", []):
            if price is not None and x < price:
                all_sell.append(x)

    if price is not None:

        above = sorted(
            [x for x in all_buy if x > price]
        )

        below = sorted(
            [x for x in all_sell if x < price],
            reverse=True
        )

        if above:

            result["nearest_buy"] = above[0]

            result["buy_distance"] = round(
                above[0] - price,
                3
            )

        if below:

            result["nearest_sell"] = below[0]

            result["sell_distance"] = round(
                price - below[0],
                3
            )

    return result


# =========================================================
# LIQUIDITY SWEEP
# =========================================================

def detect_liquidity_sweeps(price):

    sweeps = {}

    for tf in ["1D", "1H", "15M", "5M"]:

        candles = candle_data.get(tf, [])

        status = "NONE"

        if len(candles) >= 3:

            previous = candles[-2]
            current = candles[-1]

            # Buy-side liquidity sweep
            if (
                current["high"] > previous["high"]
                and current["close"] < previous["high"]
            ):
                status = "BUY-SIDE LIQUIDITY SWEPT"

            # Sell-side liquidity sweep
            elif (
                current["low"] < previous["low"]
                and current["close"] > previous["low"]
            ):
                status = "SELL-SIDE LIQUIDITY SWEPT"

        sweeps[tf] = status

    return sweeps


# =========================================================
# TIMEFRAME STRUCTURE
# =========================================================

def timeframe_bias(tf):

    candles = candle_data.get(tf, [])

    if len(candles) < 30:
        return {
            "bias": "UNKNOWN",
            "strength": 0
        }

    closes = [
        x["close"]
        for x in candles
    ]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)

    if ema20 is None or ema50 is None:
        return {
            "bias": "UNKNOWN",
            "strength": 0
        }

    recent = candles[-10:]

    recent_high = max(
        x["high"] for x in recent
    )

    previous_high = max(
        x["high"]
        for x in candles[-20:-10]
    )

    recent_low = min(
        x["low"] for x in recent
    )

    previous_low = min(
        x["low"]
        for x in candles[-20:-10]
    )

    bullish = 0
    bearish = 0

    if ema20 > ema50:
        bullish += 1

    elif ema20 < ema50:
        bearish += 1

    if recent_high > previous_high:
        bullish += 1

    elif recent_high < previous_high:
        bearish += 1

    if recent_low > previous_low:
        bullish += 1

    elif recent_low < previous_low:
        bearish += 1

    if bullish > bearish:

        return {
            "bias": "BULLISH",
            "strength": bullish
        }

    if bearish > bullish:

        return {
            "bias": "BEARISH",
            "strength": bearish
        }

    return {
        "bias": "NEUTRAL",
        "strength": 1
    }


# =========================================================
# AI CONCLUSION ENGINE
# =========================================================

def generate_ai_conclusion(
    price,
    liquidity,
    sweeps
):

    score = 0
    reasons = []

    # -----------------------------------------------------
    # MULTI TIMEFRAME STRUCTURE
    # -----------------------------------------------------

    timeframe_results = {}

    for tf in ["1D", "1H", "15M", "5M"]:

        timeframe_results[tf] = timeframe_bias(tf)

    # Higher timeframe weight
    weights = {
        "1D": 3,
        "1H": 3,
        "15M": 2,
        "5M": 2
    }

    for tf, weight in weights.items():

        bias = timeframe_results[tf]["bias"]

        if bias == "BULLISH":
            score += weight

        elif bias == "BEARISH":
            score -= weight

    # -----------------------------------------------------
    # SWEEP CONFIRMATION
    # -----------------------------------------------------

    bullish_sweep = 0
    bearish_sweep = 0

    for tf, sweep in sweeps.items():

        if sweep == "SELL-SIDE LIQUIDITY SWEPT":
            bullish_sweep += 1

        elif sweep == "BUY-SIDE LIQUIDITY SWEPT":
            bearish_sweep += 1

    score += bullish_sweep * 2
    score -= bearish_sweep * 2

    # -----------------------------------------------------
    # LIQUIDITY DISTANCE
    # -----------------------------------------------------

    nearest_buy = liquidity.get(
        "nearest_buy"
    )

    nearest_sell = liquidity.get(
        "nearest_sell"
    )

    if (
        nearest_buy is not None
        and nearest_sell is not None
        and price is not None
    ):

        buy_distance = abs(
            nearest_buy - price
        )

        sell_distance = abs(
            price - nearest_sell
        )

        if buy_distance < sell_distance:
            score += 1
            reasons.append(
                "Nearest liquidity is above price."
            )

        elif sell_distance < buy_distance:
            score -= 1
            reasons.append(
                "Nearest liquidity is below price."
            )

    # -----------------------------------------------------
    # REASONS
    # -----------------------------------------------------

    bullish_count = sum(
        1
        for x in timeframe_results.values()
        if x["bias"] == "BULLISH"
    )

    bearish_count = sum(
        1
        for x in timeframe_results.values()
        if x["bias"] == "BEARISH"
    )

    if bullish_count >= 3:

        reasons.append(
            "Multiple timeframes show bullish structure."
        )

    elif bearish_count >= 3:

        reasons.append(
            "Multiple timeframes show bearish structure."
        )

    if bullish_sweep:

        reasons.append(
            "Sell-side liquidity sweep detected."
        )

    if bearish_sweep:

        reasons.append(
            "Buy-side liquidity sweep detected."
        )

    # -----------------------------------------------------
    # FINAL DECISION
    # -----------------------------------------------------

    if score >= 6:

        decision = "BUY SIDE"
        action = "BUY SETUP"

    elif score <= -6:

        decision = "SELL SIDE"
        action = "SELL SETUP"

    else:

        decision = "WAIT"
        action = "NO TRADE"

    # -----------------------------------------------------
    # CONFIDENCE
    # -----------------------------------------------------

    raw_confidence = 50 + (
        min(abs(score), 10) * 4
    )

    confidence = min(
        90,
        max(
            35,
            raw_confidence
        )
    )

    # Conflicting higher timeframe data
    if (
        timeframe_results["1D"]["bias"]
        != "UNKNOWN"
        and timeframe_results["1H"]["bias"]
        != "UNKNOWN"
        and
        timeframe_results["1D"]["bias"]
        != timeframe_results["1H"]["bias"]
    ):

        confidence = min(
            confidence,
            60
        )

        reasons.append(
            "Higher timeframes are conflicting."
        )

        decision = "WAIT"
        action = "WAIT FOR CONFIRMATION"

    # -----------------------------------------------------
    # ENTRY / INVALIDATION / TARGET
    # -----------------------------------------------------

    entry_zone = None
    invalidation = None
    target = None

    if decision == "BUY SIDE":

        entry_zone = (
            round(price, 3)
            if price is not None
            else None
        )

        invalidation = nearest_sell
        target = nearest_buy

    elif decision == "SELL SIDE":

        entry_zone = (
            round(price, 3)
            if price is not None
            else None
        )

        invalidation = nearest_buy
        target = nearest_sell

    else:

        if bullish_count > bearish_count:
            action = "WAIT FOR BUY CONFIRMATION"

        elif bearish_count > bullish_count:
            action = "WAIT FOR SELL CONFIRMATION"

        else:
            action = "WAIT"

    if not reasons:

        reasons.append(
            "Market evidence is currently insufficient."
        )

    return {
        "decision": decision,
        "action": action,
        "confidence": confidence,
        "score": score,
        "reasons": reasons,
        "entry_zone": entry_zone,
        "invalidation": invalidation,
        "target_liquidity": target,
        "timeframes": timeframe_results
    }


# =========================================================
# GOLD ANALYSIS
# =========================================================

def calculate_analysis(price):

    history = list(
        latest["gold"]["history"]
    )

    if len(history) < 20:

        return {
            "trend": "WAITING",
            "momentum": "WAITING",
            "structure": "WAITING",
            "rsi": None,
            "ema20": None,
            "ema50": None,
            "support": None,
            "resistance": None,
            "signal": "WAIT",
            "confidence": 0,
            "liquidity": {},
            "sweeps": {},
            "ai_conclusion": {
                "decision": "WAIT",
                "action": "WAIT FOR DATA",
                "confidence": 0,
                "score": 0,
                "reasons": [
                    "Waiting for sufficient live market data."
                ],
                "entry_zone": None,
                "invalidation": None,
                "target_liquidity": None,
                "timeframes": {}
            }
        }

    ema20 = ema(
        history,
        20
    )

    ema50 = ema(
        history,
        50
    )

    current_rsi = rsi(
        history,
        14
    )

    support = min(
        history[-20:]
    )

    resistance = max(
        history[-20:]
    )

    if ema20 is not None and ema50 is not None:

        if ema20 > ema50:

            trend = "BULLISH"

        elif ema20 < ema50:

            trend = "BEARISH"

        else:

            trend = "NEUTRAL"

    else:

        trend = "WAITING"

    if len(history) >= 5:

        movement = (
            history[-1]
            - history[-5]
        )

        if movement > 0:
            momentum = "BUYING"

        elif movement < 0:
            momentum = "SELLING"

        else:
            momentum = "FLAT"

    else:

        momentum = "WAITING"

    if len(history) >= 10:

        if history[-1] > history[-10]:
            structure = "HIGHER"

        elif history[-1] < history[-10]:
            structure = "LOWER"

        else:
            structure = "SIDEWAYS"

    else:

        structure = "WAITING"

    # -----------------------------------------------------
    # LIQUIDITY
    # -----------------------------------------------------

    liquidity = calculate_liquidity(
        price
    )

    sweeps = detect_liquidity_sweeps(
        price
    )

    # -----------------------------------------------------
    # AI CONCLUSION
    # -----------------------------------------------------

    ai_conclusion = generate_ai_conclusion(
        price,
        liquidity,
        sweeps
    )

    return {
        "trend": trend,
        "momentum": momentum,
        "structure": structure,
        "rsi": round(current_rsi, 2)
        if current_rsi is not None
        else None,
        "ema20": round(ema20, 3)
        if ema20 is not None
        else None,
        "ema50": round(ema50, 3)
        if ema50 is not None
        else None,
        "support": round(support, 3),
        "resistance": round(resistance, 3),
        "signal": ai_conclusion["decision"],
        "confidence": ai_conclusion["confidence"],
        "liquidity": liquidity,
        "sweeps": sweeps,
        "ai_conclusion": ai_conclusion
    }


# =========================================================
# BROADCAST
# =========================================================

def broadcast(data):

    message = (
        json.dumps(data)
        + "\n"
    )

    dead = []

    with clients_lock:

        for client in clients:

            try:
                client.put_nowait(
                    message
                )

            except Exception:
                dead.append(client)

        for client in dead:

            if client in clients:
                clients.remove(client)


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_worker():

    print("GOLD ENGINE STARTING")

    while True:

        try:

            ws = websocket.create_connection(
                "wss://ws.twelvedata.com/v1/quotes/price",
                timeout=20
            )

            print(
                "TWELVE DATA GOLD CONNECTED"
            )

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(
                json.dumps(
                    subscribe_message
                )
            )

            while True:

                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(raw)

                if (
                    data.get("event")
                    == "subscribe-status"
                ):

                    print(
                        "GOLD SUBSCRIPTION:",
                        data
                    )

                    continue

                price = None

                if "price" in data:

                    try:
                        price = float(
                            data["price"]
                        )

                    except Exception:
                        pass

                if price is None:
                    continue

                print(
                    f"GOLD: {price}"
                )

                with state_lock:

                    latest["gold"]["price"] = price

                    latest["gold"]["history"].append(
                        price
                    )

                    latest["gold"]["timestamp"] = time.time()

                    analysis = calculate_analysis(
                        price
                    )

                    latest["gold"]["analysis"] = analysis

                broadcast({
                    "type": "gold_update",
                    "price": price,
                    "analysis": analysis,
                    "timestamp": time.time()
                })

        except Exception as e:

            print(
                "GOLD WEBSOCKET ERROR:",
                e
            )

            try:
                ws.close()

            except Exception:
                pass

            time.sleep(3)


# =========================================================
# OIL PLACEHOLDER
# =========================================================

def oil_worker():

    print("OIL ENGINE STARTING")

    while True:

        time.sleep(10)


# =========================================================
# START ENGINE
# =========================================================

def start_live_engine():

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
        "LIVE ENGINE STARTED"
    )


# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>

<html>

<head>

<meta name="viewport"
content="width=device-width, initial-scale=1">

<title>Trading-AI</title>

<style>

body {
    background:#07111f;
    color:#ffffff;
    font-family:Arial,sans-serif;
    margin:0;
    padding:20px;
}

.container {
    max-width:1000px;
    margin:auto;
}

h1 {
    margin-bottom:5px;
}

.subtitle {
    color:#8ea2bd;
    margin-bottom:20px;
}

.card {
    background:#0d1b2d;
    border:1px solid #20334d;
    border-radius:14px;
    padding:20px;
    margin-bottom:18px;
}

.price {
    font-size:38px;
    font-weight:bold;
}

.live {
    color:#4ade80;
    font-size:13px;
}

.grid {
    display:grid;
    grid-template-columns:
    repeat(auto-fit,minmax(180px,1fr));
    gap:12px;
}

.box {
    background:#081525;
    padding:14px;
    border-radius:10px;
}

.label {
    color:#8ea2bd;
    font-size:12px;
    margin-bottom:6px;
}

.value {
    font-size:18px;
    font-weight:bold;
}

.conclusion {
    border:2px solid #334155;
    background:#101f33;
    border-radius:16px;
    padding:22px;
}

.conclusion-title {
    font-size:13px;
    color:#94a3b8;
    letter-spacing:1px;
}

.decision {
    font-size:32px;
    font-weight:bold;
    margin:8px 0;
}

.reason {
    margin-top:8px;
    color:#cbd5e1;
}

.small {
    color:#94a3b8;
    font-size:13px;
}

.liquidity-row {
    margin-top:10px;
    padding:10px;
    background:#081525;
    border-radius:8px;
}

</style>

</head>

<body>

<div class="container">

<h1>Trading-AI</h1>

<div class="subtitle">
Real-time market intelligence
</div>


<div class="card">

<div class="live">
● LIVE GOLD STREAM
</div>

<div class="price"
id="goldPrice">
Waiting...
</div>

</div>


<!-- AI CONCLUSION -->

<div class="conclusion">

<div class="conclusion-title">
🧠 AI MARKET CONCLUSION
</div>

<div class="decision"
id="aiDecision">
WAIT
</div>

<div id="aiAction"
class="small">
Waiting for market data...
</div>

<br>

<div class="grid">

<div class="box">

<div class="label">
CONFIDENCE
</div>

<div class="value"
id="aiConfidence">
--
</div>

</div>


<div class="box">

<div class="label">
AI SCORE
</div>

<div class="value"
id="aiScore">
--
</div>

</div>


<div class="box">

<div class="label">
ENTRY ZONE
</div>

<div class="value"
id="aiEntry">
--
</div>

</div>


<div class="box">

<div class="label">
INVALIDATION
</div>

<div class="value"
id="aiInvalidation">
--
</div>

</div>


<div class="box">

<div class="label">
TARGET LIQUIDITY
</div>

<div class="value"
id="aiTarget">
--
</div>

</div>

</div>


<div style="margin-top:20px">

<div class="label">
WHY?
</div>

<div id="aiReasons"
class="reason">
Waiting...
</div>

</div>

</div>


<!-- MARKET ANALYSIS -->

<div class="card">

<h2>Market Analysis</h2>

<div class="grid">

<div class="box">
<div class="label">TREND</div>
<div class="value" id="trend">--</div>
</div>

<div class="box">
<div class="label">MOMENTUM</div>
<div class="value" id="momentum">--</div>
</div>

<div class="box">
<div class="label">STRUCTURE</div>
<div class="value" id="structure">--</div>
</div>

<div class="box">
<div class="label">RSI</div>
<div class="value" id="rsi">--</div>
</div>

<div class="box">
<div class="label">EMA 20</div>
<div class="value" id="ema20">--</div>
</div>

<div class="box">
<div class="label">EMA 50</div>
<div class="value" id="ema50">--</div>
</div>

</div>

</div>


<!-- LIQUIDITY -->

<div class="card">

<h2>💧 Liquidity Map</h2>

<div id="liquidity">
Waiting for liquidity data...
</div>

</div>


<div class="card">

<h2>Gold XAU/USD</h2>

<div class="small">
Data source: Twelve Data
</div>

</div>


</div>


<script>

const eventSource =
new EventSource("/stream");


eventSource.onmessage =
function(event) {

    const data =
    JSON.parse(event.data);


    if (data.type !== "gold_update")
        return;


    const price =
    data.price;

    const analysis =
    data.analysis;


    document.getElementById(
        "goldPrice"
    ).innerText =
        Number(price).toFixed(3);


    document.getElementById(
        "trend"
    ).innerText =
        analysis.trend || "--";


    document.getElementById(
        "momentum"
    ).innerText =
        analysis.momentum || "--";


    document.getElementById(
        "structure"
    ).innerText =
        analysis.structure || "--";


    document.getElementById(
        "rsi"
    ).innerText =
        analysis.rsi ?? "--";


    document.getElementById(
        "ema20"
    ).innerText =
        analysis.ema20 ?? "--";


    document.getElementById(
        "ema50"
    ).innerText =
        analysis.ema50 ?? "--";


    // =========================================
    // AI CONCLUSION
    // =========================================

    const ai =
    analysis.ai_conclusion;


    if (ai) {

        document.getElementById(
            "aiDecision"
        ).innerText =
            ai.decision || "WAIT";


        document.getElementById(
            "aiAction"
        ).innerText =
            ai.action || "--";


        document.getElementById(
            "aiConfidence"
        ).innerText =
            (ai.confidence ?? "--")
            + "%";


        document.getElementById(
            "aiScore"
        ).innerText =
            ai.score ?? "--";


        document.getElementById(
            "aiEntry"
        ).innerText =
            ai.entry_zone ?? "--";


        document.getElementById(
            "aiInvalidation"
        ).innerText =
            ai.invalidation ?? "--";


        document.getElementById(
            "aiTarget"
        ).innerText =
            ai.target_liquidity ?? "--";


        const reasons =
            ai.reasons || [];


        document.getElementById(
            "aiReasons"
        ).innerHTML =
            reasons
            .map(
                x => "• " + x
            )
            .join("<br>");

    }


    // =========================================
    // LIQUIDITY MAP
    // =========================================

    const liquidity =
    analysis.liquidity;


    if (liquidity) {

        let html = "";


        [
            "1D",
            "1H",
            "15M",
            "5M"
        ].forEach(tf => {

            const item =
                liquidity[tf] || {};


            html += `
                <div class="liquidity-row">

                <b>${tf}</b><br>

                <span class="small">
                Buy-side:
                ${
                    (item.buy_side || [])
                    .join(", ") || "--"
                }
                </span>

                <br>

                <span class="small">
                Sell-side:
                ${
                    (item.sell_side || [])
                    .join(", ") || "--"
                }
                </span>

                </div>
            `;

        });


        html += `
            <div class="grid"
                 style="margin-top:12px">

                <div class="box">

                    <div class="label">
                    NEAREST BUY-SIDE
                    </div>

                    <div class="value">
                    ${
                        liquidity.nearest_buy
                        ?? "--"
                    }
                    </div>

                </div>


                <div class="box">

                    <div class="label">
                    NEAREST SELL-SIDE
                    </div>

                    <div class="value">
                    ${
                        liquidity.nearest_sell
                        ?? "--"
                    }
                    </div>

                </div>


                <div class="box">

                    <div class="label">
                    BUY DISTANCE
                    </div>

                    <div class="value">
                    ${
                        liquidity.buy_distance
                        ?? "--"
                    }
                    </div>

                </div>


                <div class="box">

                    <div class="label">
                    SELL DISTANCE
                    </div>

                    <div class="value">
                    ${
                        liquidity.sell_distance
                        ?? "--"
                    }
                    </div>

                </div>

            </div>
        `;


        document.getElementById(
            "liquidity"
        ).innerHTML = html;

    }

};


eventSource.onerror =
function() {

    console.log(
        "Stream reconnecting..."
    );

};

</script>

</body>

</html>
"""


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def home():

    return render_template_string(
        HTML
    )


@app.route("/stream")
def stream():

    q = queue.Queue(
        maxsize=20
    )

    with clients_lock:
        clients.append(q)

    def generate():

        try:

            while True:

                try:

                    message = q.get(
                        timeout=30
                    )

                    yield (
                        "data: "
                        + message
                        + "\n\n"
                    )

                except queue.Empty:

                    yield ": keepalive\n\n"

        finally:

            with clients_lock:

                if q in clients:
                    clients.remove(q)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@app.route("/health")
def health():

    with state_lock:

        return {
            "status": "ok",
            "gold_price":
                latest["gold"]["price"],
            "candles": {
                tf: len(
                    candle_data.get(
                        tf,
                        []
                    )
                )
                for tf in CANDLE_INTERVALS
            }
        }


# =========================================================
# START
# =========================================================

start_live_engine()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                5000
            )
        ),
        threaded=True
    )
