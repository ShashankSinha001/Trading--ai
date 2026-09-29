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

latest = {
    "gold": {
        "price": None,
        "status": "Connecting...",
        "analysis": None,
        "updated": None
    },
    "oil": {
        "price": None,
        "status": "DATA SOURCE REQUIRED",
        "analysis": None,
        "updated": None
    }
}

history = {
    "gold": deque(maxlen=500),
    "oil": deque(maxlen=500)
}

clients = []
clients_lock = threading.Lock()
state_lock = threading.Lock()

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
    message = json.dumps(data)

    with clients_lock:
        dead = []

        for q in clients:
            try:
                q.put_nowait(message)
            except Exception:
                dead.append(q)

        for q in dead:
            if q in clients:
                clients.remove(q)


# =========================================================
# TWELVE DATA CANDLES
# =========================================================

def fetch_candles(interval, outputsize=200):
    if not API_KEY:
        print("CANDLE ERROR: TWELVE_DATA_API_KEY missing", flush=True)
        return []

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": GOLD_SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "timezone": "UTC",
        "apikey": API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=15)

        print(
            f"CANDLE REQUEST {interval}: HTTP {r.status_code}",
            flush=True
        )

        data = r.json()

        if "values" not in data:
            print(
                f"CANDLE ERROR {interval}: {data}",
                flush=True
            )
            return []

        values = list(reversed(data["values"]))

        candles = []

        for x in values:
            try:
                candles.append({
                    "datetime": x.get("datetime"),
                    "open": float(x["open"]),
                    "high": float(x["high"]),
                    "low": float(x["low"]),
                    "close": float(x["close"])
                })
            except Exception:
                continue

        print(
            f"CANDLES {interval}: {len(candles)}",
            flush=True
        )

        return candles

    except Exception as e:
        print(
            f"CANDLE REQUEST ERROR {interval}: {e}",
            flush=True
        )
        return []


def update_all_candles():
    for name, interval in CANDLE_INTERVALS.items():
        candles = fetch_candles(interval)

        if candles:
            candle_data[name] = candles


def candle_worker():
    print("CANDLE ENGINE STARTING", flush=True)

    # Initial fetch immediately
    update_all_candles()

    while True:
        try:
            time.sleep(60)
            update_all_candles()
        except Exception as e:
            print(
                f"CANDLE WORKER ERROR: {e}",
                flush=True
            )
            time.sleep(10)


# =========================================================
# INDICATORS
# =========================================================

def ema(values, period):
    if not values:
        return None

    if len(values) < period:
        period = len(values)

    if period <= 0:
        return None

    multiplier = 2 / (period + 1)

    result = values[0]

    for value in values[1:]:
        result = (value - result) * multiplier + result

    return result


def rsi(values, period=14):
    if len(values) < 2:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    if len(gains) < period:
        period = len(gains)

    if period <= 0:
        return None

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# =========================================================
# SWINGS
# =========================================================

def swing_highs(candles, lookback=2):
    result = []

    if len(candles) < lookback * 2 + 1:
        return result

    for i in range(lookback, len(candles) - lookback):
        h = candles[i]["high"]

        left = [
            candles[j]["high"]
            for j in range(i - lookback, i)
        ]

        right = [
            candles[j]["high"]
            for j in range(i + 1, i + lookback + 1)
        ]

        if h > max(left) and h > max(right):
            result.append(h)

    return result


def swing_lows(candles, lookback=2):
    result = []

    if len(candles) < lookback * 2 + 1:
        return result

    for i in range(lookback, len(candles) - lookback):
        l = candles[i]["low"]

        left = [
            candles[j]["low"]
            for j in range(i - lookback, i)
        ]

        right = [
            candles[j]["low"]
            for j in range(i + 1, i + lookback + 1)
        ]

        if l < min(left) and l < min(right):
            result.append(l)

    return result


# =========================================================
# LIQUIDITY
# =========================================================

def unique_levels(levels, tolerance=0.15):
    levels = sorted(levels)

    result = []

    for level in levels:
        if not result:
            result.append(level)
            continue

        if abs(level - result[-1]) > tolerance:
            result.append(level)

    return result


def calculate_liquidity(price):
    buy_side = []
    sell_side = []

    # 1D previous candle
    d1 = candle_data.get("1D", [])

    if len(d1) >= 2:
        previous = d1[-2]

        buy_side.append({
            "timeframe": "1D",
            "type": "Previous Day High",
            "price": previous["high"]
        })

        sell_side.append({
            "timeframe": "1D",
            "type": "Previous Day Low",
            "price": previous["low"]
        })

    # Other timeframes
    for tf in ["1H", "15M", "5M"]:
        candles = candle_data.get(tf, [])

        highs = swing_highs(candles)
        lows = swing_lows(candles)

        highs = unique_levels(highs)
        lows = unique_levels(lows)

        for level in highs[-15:]:
            buy_side.append({
                "timeframe": tf,
                "type": "Swing High",
                "price": level
            })

        for level in lows[-15:]:
            sell_side.append({
                "timeframe": tf,
                "type": "Swing Low",
                "price": level
            })

    buy_side = [
        x for x in buy_side
        if x["price"] > price
    ]

    sell_side = [
        x for x in sell_side
        if x["price"] < price
    ]

    buy_side.sort(key=lambda x: x["price"])
    sell_side.sort(
        key=lambda x: x["price"],
        reverse=True
    )

    nearest_buy = buy_side[0] if buy_side else None
    nearest_sell = sell_side[0] if sell_side else None

    return {
        "buy_side": buy_side,
        "sell_side": sell_side,
        "nearest_buy": nearest_buy,
        "nearest_sell": nearest_sell
    }


# =========================================================
# LIQUIDITY SWEEPS
# =========================================================

def detect_liquidity_sweeps():
    sweeps = []

    for tf in ["5M", "15M", "1H", "1D"]:
        candles = candle_data.get(tf, [])

        if len(candles) < 3:
            continue

        previous = candles[-2]
        current = candles[-1]

        # Buy-side liquidity sweep:
        # price takes previous high but closes below it
        if (
            current["high"] > previous["high"]
            and current["close"] < previous["high"]
        ):
            sweeps.append({
                "timeframe": tf,
                "side": "BUY-SIDE",
                "price": current["high"]
            })

        # Sell-side liquidity sweep:
        # price takes previous low but closes above it
        if (
            current["low"] < previous["low"]
            and current["close"] > previous["low"]
        ):
            sweeps.append({
                "timeframe": tf,
                "side": "SELL-SIDE",
                "price": current["low"]
            })

    return sweeps


# =========================================================
# TIMEFRAME BIAS
# =========================================================

def timeframe_bias(tf):
    candles = candle_data.get(tf, [])

    if len(candles) < 20:
        return "UNKNOWN"

    closes = [x["close"] for x in candles]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)

    recent = candles[-6:]

    recent_high = max(x["high"] for x in recent)
    previous_high = max(
        x["high"] for x in candles[-12:-6]
    )

    recent_low = min(x["low"] for x in recent)
    previous_low = min(
        x["low"] for x in candles[-12:-6]
    )

    last = closes[-1]

    bullish = 0
    bearish = 0

    if ema20 is not None and ema50 is not None:
        if ema20 > ema50:
            bullish += 1
        elif ema20 < ema50:
            bearish += 1

    if last > ema20:
        bullish += 1
    elif last < ema20:
        bearish += 1

    if recent_high > previous_high:
        bullish += 1

    if recent_low > previous_low:
        bullish += 1

    if recent_high < previous_high:
        bearish += 1

    if recent_low < previous_low:
        bearish += 1

    if bullish >= 3:
        return "BULLISH"

    if bearish >= 3:
        return "BEARISH"

    return "NEUTRAL"


# =========================================================
# AI MARKET CONCLUSION
# =========================================================

def generate_ai_conclusion(price, liquidity, sweeps):
    score = 0
    reasons = []

    weights = {
        "1D": 3,
        "1H": 3,
        "15M": 2,
        "5M": 2
    }

    biases = {}

    for tf, weight in weights.items():
        bias = timeframe_bias(tf)
        biases[tf] = bias

        if bias == "BULLISH":
            score += weight
            reasons.append(
                f"{tf} structure is bullish"
            )

        elif bias == "BEARISH":
            score -= weight
            reasons.append(
                f"{tf} structure is bearish"
            )

    # Sweep logic
    for sweep in sweeps:
        if sweep["side"] == "SELL-SIDE":
            score += 2
            reasons.append(
                f'{sweep["timeframe"]} sell-side liquidity sweep'
            )

        elif sweep["side"] == "BUY-SIDE":
            score -= 2
            reasons.append(
                f'{sweep["timeframe"]} buy-side liquidity sweep'
            )

    nearest_buy = liquidity.get("nearest_buy")
    nearest_sell = liquidity.get("nearest_sell")

    if nearest_buy and nearest_sell:
        buy_distance = abs(
            nearest_buy["price"] - price
        )

        sell_distance = abs(
            price - nearest_sell["price"]
        )

        if buy_distance < sell_distance:
            score += 1
            reasons.append(
                "Nearest liquidity is above price"
            )
        else:
            score -= 1
            reasons.append(
                "Nearest liquidity is below price"
            )

    if score >= 6:
        decision = "BUY SIDE"
        action = "LOOK FOR LONG SETUPS"

    elif score <= -6:
        decision = "SELL SIDE"
        action = "LOOK FOR SHORT SETUPS"

    else:
        decision = "WAIT"
        action = "NO CLEAR EDGE"

    confidence = 50 + min(abs(score), 10) * 4

    # Higher timeframe conflict protection
    if (
        biases.get("1D") in ["BULLISH", "BEARISH"]
        and biases.get("1H") in ["BULLISH", "BEARISH"]
        and biases["1D"] != biases["1H"]
    ):
        confidence = min(confidence, 60)
        decision = "WAIT"
        action = "HIGHER TIMEFRAME CONFLICT"

        reasons.append(
            "1D and 1H direction are conflicting"
        )

    if not reasons:
        reasons.append(
            "Waiting for enough market structure data"
        )

    # Entry / invalidation / target
    entry_zone = "--"
    invalidation = "--"
    target = "--"

    if decision == "BUY SIDE":
        if nearest_sell:
            entry_zone = (
                f'{nearest_sell["price"]:.3f} - '
                f'{price:.3f}'
            )

        if nearest_sell:
            invalidation = (
                f'Below {nearest_sell["price"]:.3f}'
            )

        if nearest_buy:
            target = (
                f'{nearest_buy["price"]:.3f} '
                f'({nearest_buy["timeframe"]})'
            )

    elif decision == "SELL SIDE":
        if nearest_buy:
            entry_zone = (
                f'{price:.3f} - '
                f'{nearest_buy["price"]:.3f}'
            )

        if nearest_buy:
            invalidation = (
                f'Above {nearest_buy["price"]:.3f}'
            )

        if nearest_sell:
            target = (
                f'{nearest_sell["price"]:.3f} '
                f'({nearest_sell["timeframe"]})'
            )

    else:
        if nearest_buy and nearest_sell:
            entry_zone = (
                f'Wait: {nearest_sell["price"]:.3f} / '
                f'{nearest_buy["price"]:.3f}'
            )

            invalidation = "Wait for confirmation"

            target = (
                f'Buy-side {nearest_buy["price"]:.3f} / '
                f'Sell-side {nearest_sell["price"]:.3f}'
            )

    return {
        "decision": decision,
        "action": action,
        "confidence": confidence,
        "score": score,
        "entry_zone": entry_zone,
        "invalidation": invalidation,
        "target_liquidity": target,
        "reasons": reasons,
        "biases": biases
    }


# =========================================================
# BASIC MARKET ANALYSIS
# =========================================================

def calculate_analysis(price):
    values = list(history["gold"])

    if len(values) < 5:
        return None

    ema20 = ema(values[-100:], 20)
    ema50 = ema(values[-100:], 50)
    current_rsi = rsi(values[-100:], 14)

    if len(values) >= 10:
        if values[-1] > values[-10]:
            trend = "BULLISH"
        elif values[-1] < values[-10]:
            trend = "BEARISH"
        else:
            trend = "NEUTRAL"
    else:
        trend = "NEUTRAL"

    if len(values) >= 5:
        if values[-1] > values[-5]:
            momentum = "BUYING"
        elif values[-1] < values[-5]:
            momentum = "SELLING"
        else:
            momentum = "NEUTRAL"
    else:
        momentum = "NEUTRAL"

    if len(values) >= 20:
        recent_high = max(values[-20:])
        recent_low = min(values[-20:])

        if price >= recent_high:
            structure = "BREAKING HIGH"

        elif price <= recent_low:
            structure = "BREAKING LOW"

        else:
            structure = "RANGE"

        support = recent_low
        resistance = recent_high

    else:
        structure = "BUILDING"
        support = min(values)
        resistance = max(values)

    liquidity = calculate_liquidity(price)
    sweeps = detect_liquidity_sweeps()

    ai = generate_ai_conclusion(
        price,
        liquidity,
        sweeps
    )

    return {
        "trend": trend,
        "momentum": momentum,
        "structure": structure,
        "rsi": round(current_rsi, 2)
        if current_rsi is not None else None,
        "ema20": round(ema20, 3)
        if ema20 is not None else None,
        "ema50": round(ema50, 3)
        if ema50 is not None else None,
        "support": round(support, 3),
        "resistance": round(resistance, 3),
        "liquidity": liquidity,
        "sweeps": sweeps,
        "ai": ai
    }


# =========================================================
# GOLD WEBSOCKET
# =========================================================

def gold_worker():
    print("GOLD ENGINE STARTING", flush=True)

    if not API_KEY:
        print(
            "GOLD ERROR: TWELVE_DATA_API_KEY NOT FOUND",
            flush=True
        )

        with state_lock:
            latest["gold"]["status"] = "API KEY MISSING"

        return

    ws_url = "wss://ws.twelvedata.com/v1/quotes/price"

    while True:
        ws = None

        try:
            print(
                "GOLD WS CONNECT ATTEMPT",
                flush=True
            )

            ws = websocket.create_connection(
                ws_url,
                timeout=15
            )

            print(
                "TWELVE DATA GOLD CONNECTED",
                flush=True
            )

            subscribe_message = {
                "action": "subscribe",
                "params": {
                    "symbols": GOLD_SYMBOL
                }
            }

            ws.send(
                json.dumps(subscribe_message)
            )

            print(
                f"GOLD SUBSCRIBE SENT: {GOLD_SYMBOL}",
                flush=True
            )

            with state_lock:
                latest["gold"]["status"] = "Connected"

            while True:
                raw = ws.recv()

                if not raw:
                    raise Exception(
                        "Empty WebSocket response"
                    )

                try:
                    data = json.loads(raw)
                except Exception:
                    print(
                        f"GOLD RAW MESSAGE: {raw}",
                        flush=True
                    )
                    continue

                print(
                    f"GOLD WS: {data}",
                    flush=True
                )

                # Subscription status
                if data.get("event") == "subscribe-status":
                    print(
                        f"GOLD SUBSCRIPTION: {data}",
                        flush=True
                    )
                    continue

                # Error
                if data.get("status") == "error":
                    print(
                        f"GOLD API ERROR: {data}",
                        flush=True
                    )
                    continue

                # Price message
                price = data.get("price")

                if price is None:
                    continue

                try:
                    price = float(price)
                except Exception:
                    continue

                history["gold"].append(price)

                analysis = calculate_analysis(price)

                now = time.strftime(
                    "%Y-%m-%d %H:%M:%S UTC",
                    time.gmtime()
                )

                with state_lock:
                    latest["gold"] = {
                        "price": price,
                        "status": "LIVE",
                        "analysis": analysis,
                        "updated": now
                    }

                print(
                    f"GOLD PRICE: {price}",
                    flush=True
                )

                broadcast({
                    "type": "gold",
                    "gold": latest["gold"]
                })

        except websocket.WebSocketTimeoutException:
            print(
                "GOLD WS TIMEOUT - RECONNECTING",
                flush=True
            )

        except websocket.WebSocketConnectionClosedException:
            print(
                "GOLD WS CLOSED - RECONNECTING",
                flush=True
            )

        except Exception as e:
            print(
                f"GOLD WEBSOCKET ERROR: {e}",
                flush=True
            )

        finally:
            with state_lock:
                if latest["gold"]["price"] is None:
                    latest["gold"]["status"] = "Reconnecting..."

            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

        time.sleep(3)


# =========================================================
# OIL PLACEHOLDER
# =========================================================

def oil_worker():
    print("OIL ENGINE STARTING", flush=True)

    with state_lock:
        latest["oil"]["status"] = (
            "DATA SOURCE REQUIRED"
        )

    while True:
        time.sleep(60)


# =========================================================
# ENGINE
# =========================================================

def start_live_engine():
    print("LIVE ENGINE STARTING", flush=True)

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
    margin: 0;
    background: #0b0f14;
    color: #f5f5f5;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 1100px;
    margin: auto;
    padding: 20px;
}

h1 {
    margin-bottom: 5px;
}

.subtitle {
    color: #8b949e;
    margin-bottom: 25px;
}

.card {
    background: #111820;
    border: 1px solid #26313d;
    border-radius: 14px;
    padding: 20px;
    margin-bottom: 18px;
}

.price {
    font-size: 42px;
    font-weight: bold;
    margin: 12px 0;
}

.live {
    color: #35d07f;
    font-weight: bold;
}

.waiting {
    color: #f0b429;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
}

.metric {
    background: #0d131a;
    padding: 14px;
    border-radius: 10px;
    border: 1px solid #202a35;
}

.label {
    color: #8b949e;
    font-size: 12px;
    margin-bottom: 6px;
}

.value {
    font-size: 18px;
    font-weight: bold;
}

.conclusion {
    text-align: center;
    padding: 25px;
}

.decision {
    font-size: 38px;
    font-weight: bold;
    margin: 10px 0;
}

.why {
    text-align: left;
    margin-top: 18px;
}

.why div {
    padding: 6px 0;
    color: #c9d1d9;
}

.liquidity-row {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(220px, 1fr));
    gap: 12px;
}

.liquidity-box {
    background: #0d131a;
    padding: 14px;
    border-radius: 10px;
}

.small {
    color: #8b949e;
    font-size: 13px;
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

<div id="goldStatus" class="waiting">
Connecting...
</div>

<div id="goldPrice" class="price">
Waiting...
</div>

<div class="small">
XAU/USD · Twelve Data
</div>

</div>


<div class="card conclusion">

<h2>🧠 AI MARKET CONCLUSION</h2>

<div id="decision"
     class="decision">
WAIT
</div>

<div id="action">
Waiting for market data...
</div>

<br>

<div class="grid">

<div class="metric">
<div class="label">CONFIDENCE</div>
<div id="confidence"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">AI SCORE</div>
<div id="score"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">ENTRY ZONE</div>
<div id="entry"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">INVALIDATION</div>
<div id="invalidation"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">TARGET LIQUIDITY</div>
<div id="target"
     class="value">--</div>
</div>

</div>

<div class="why">

<h3>WHY?</h3>

<div id="why">
Waiting...
</div>

</div>

</div>


<div class="card">

<h2>Market Analysis</h2>

<div class="grid">

<div class="metric">
<div class="label">TREND</div>
<div id="trend"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">MOMENTUM</div>
<div id="momentum"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">STRUCTURE</div>
<div id="structure"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">RSI</div>
<div id="rsi"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">EMA 20</div>
<div id="ema20"
     class="value">--</div>
</div>

<div class="metric">
<div class="label">EMA 50</div>
<div id="ema50"
     class="value">--</div>
</div>

</div>

</div>


<div class="card">

<h2>💧 Liquidity Map</h2>

<div class="liquidity-row">

<div class="liquidity-box">
<h3>BUY-SIDE</h3>
<div id="buyLiquidity">
Waiting...
</div>
</div>

<div class="liquidity-box">
<h3>SELL-SIDE</h3>
<div id="sellLiquidity">
Waiting...
</div>
</div>

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

function setText(id, value) {
    const el = document.getElementById(id);

    if (el) {
        el.innerText =
            value === null ||
            value === undefined
            ? "--"
            : value;
    }
}


function formatLiquidity(items) {

    if (!items || items.length === 0) {
        return "No nearby liquidity";
    }

    return items.slice(0, 8).map(x =>
        `${x.timeframe} · ${x.type} · ${Number(x.price).toFixed(3)}`
    ).join("\\n");

}


function updateGold(gold) {

    if (!gold) return;

    setText(
        "goldStatus",
        gold.status || "Connecting..."
    );

    if (gold.price !== null) {
        setText(
            "goldPrice",
            Number(gold.price).toFixed(3)
        );
    }

    const a = gold.analysis;

    if (!a) return;

    setText("trend", a.trend);
    setText("momentum", a.momentum);
    setText("structure", a.structure);
    setText("rsi", a.rsi);
    setText("ema20", a.ema20);
    setText("ema50", a.ema50);

    const ai = a.ai;

    if (!ai) return;

    setText("decision", ai.decision);
    setText("action", ai.action);
    setText(
        "confidence",
        ai.confidence + "%"
    );
    setText("score", ai.score);
    setText("entry", ai.entry_zone);
    setText(
        "invalidation",
        ai.invalidation
    );
    setText(
        "target",
        ai.target_liquidity
    );

    const why = document.getElementById("why");

    if (why) {
        why.innerHTML =
            ai.reasons.map(
                x => `<div>• ${x}</div>`
            ).join("");
    }

    setText(
        "buyLiquidity",
        formatLiquidity(
            a.liquidity?.buy_side
        )
    );

    setText(
        "sellLiquidity",
        formatLiquidity(
            a.liquidity?.sell_side
        )
    );
}


// =====================================================
// SSE
// =====================================================

function connectStream() {

    const source = new EventSource("/stream");

    source.onopen = function() {
        console.log(
            "Trading-AI stream connected"
        );
    };

    source.onmessage = function(event) {

        try {

            const data =
                JSON.parse(event.data);

            if (data.type === "gold") {
                updateGold(data.gold);
            }

        } catch (error) {

            console.error(
                "Stream parse error:",
                error
            );

        }

    };

    source.onerror = function() {

        console.log(
            "Stream disconnected. Browser will retry."
        );

    };

}

connectStream();

</script>

</body>
</html>
"""


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/stream")
def stream():

    q = queue.Queue(
        maxsize=100
    )

    with clients_lock:
        clients.append(q)

    # IMPORTANT:
    # Send current snapshot immediately.
    with state_lock:
        current_gold = latest["gold"].copy()

    try:
        q.put_nowait(
            json.dumps({
                "type": "gold",
                "gold": current_gold
            })
        )
    except Exception:
        pass

    def generate():

        try:

            while True:

                try:
                    message = q.get(
                        timeout=30
                    )

                    yield (
                        f"data: {message}\n\n"
                    )

                except queue.Empty:

                    yield ": heartbeat\n\n"

        finally:

            with clients_lock:

                if q in clients:
                    clients.remove(q)

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

    with state_lock:
        gold = latest["gold"].copy()

    return {
        "status": "ok",
        "gold_status": gold["status"],
        "gold_price": gold["price"],
        "candles": {
            tf: len(candle_data[tf])
            for tf in candle_data
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
