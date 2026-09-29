from flask import Flask, jsonify, render_template_string
import os
import json
import time
import threading
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
        "source": "Twelve Data",
        "updated": None,
        "decision": "WAIT",
        "confidence": 0,
        "score": 0,
        "entry": "--",
        "invalidation": "--",
        "target": "--",
        "why": "Waiting for market data...",
        "trend_5m": "WAIT",
        "trend_15m": "WAIT",
        "trend_1h": "WAIT",
        "trend_1d": "WAIT",
        "rsi": None,
        "ema20": None,
        "ema50": None,
        "candles_5m": 0,
        "candles_15m": 0,
        "candles_1h": 0,
        "candles_1d": 0
    }
}

gold_prices = deque(maxlen=300)

candles = {
    "5min": [],
    "15min": [],
    "1h": [],
    "1day": []
}

lock = threading.Lock()


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def ema(values, period):
    if not values:
        return None

    values = [safe_float(v) for v in values]
    values = [v for v in values if v is not None]

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for price in values[period:]:
        result = (price - result) * multiplier + result

    return result


def calculate_rsi(values, period=14):
    values = [safe_float(v) for v in values]
    values = [v for v in values if v is not None]

    if len(values) < period + 1:
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

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def trend_from_candles(data):
    if not data or len(data) < 5:
        return "WAIT"

    closes = []

    for candle in data:
        close = safe_float(candle.get("close"))

        if close is not None:
            closes.append(close)

    if len(closes) < 5:
        return "WAIT"

    short = sum(closes[-3:]) / 3
    long = sum(closes[-5:]) / 5

    if short > long:
        return "BULLISH"

    if short < long:
        return "BEARISH"

    return "WAIT"


def liquidity_levels(data):
    if not data:
        return None, None

    highs = []
    lows = []

    for candle in data[-30:]:
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))

        if high is not None:
            highs.append(high)

        if low is not None:
            lows.append(low)

    if not highs or not lows:
        return None, None

    return max(highs), min(lows)


def calculate_analysis(price=None):
    with lock:
        price = price if price is not None else latest["gold"]["price"]

        if price is None:
            latest["gold"]["decision"] = "WAIT"
            latest["gold"]["confidence"] = 0
            latest["gold"]["score"] = 0
            latest["gold"]["why"] = "Waiting for live gold price..."
            return

        live_history = list(gold_prices)

        if len(live_history) < 5:
            latest["gold"]["decision"] = "WAIT"
            latest["gold"]["confidence"] = 0
            latest["gold"]["score"] = 0
            latest["gold"]["why"] = "Collecting live market data..."
            latest["gold"]["price"] = price
            return

        trend_5m = trend_from_candles(candles["5min"])
        trend_15m = trend_from_candles(candles["15min"])
        trend_1h = trend_from_candles(candles["1h"])
        trend_1d = trend_from_candles(candles["1day"])

        closes_5m = []

        for candle in candles["5min"]:
            close = safe_float(candle.get("close"))
            if close is not None:
                closes_5m.append(close)

        combined_prices = closes_5m[-80:] + live_history[-80:]

        ema20 = ema(combined_prices, 20)
        ema50 = ema(combined_prices, 50)
        rsi = calculate_rsi(combined_prices)

        score = 0
        reasons = []

        weights = {
            "5m": 2,
            "15m": 2,
            "1h": 3,
            "1d": 3
        }

        trends = {
            "5m": trend_5m,
            "15m": trend_15m,
            "1h": trend_1h,
            "1d": trend_1d
        }

        for tf, trend in trends.items():
            weight = weights[tf]

            if trend == "BULLISH":
                score += weight
                reasons.append(f"{tf} trend bullish")

            elif trend == "BEARISH":
                score -= weight
                reasons.append(f"{tf} trend bearish")

        if ema20 is not None:
            if price > ema20:
                score += 1
                reasons.append("Price above EMA20")
            else:
                score -= 1
                reasons.append("Price below EMA20")

        if ema50 is not None:
            if price > ema50:
                score += 1
                reasons.append("Price above EMA50")
            else:
                score -= 1
                reasons.append("Price below EMA50")

        if rsi is not None:
            if 55 <= rsi <= 75:
                score += 1
                reasons.append(f"RSI bullish zone ({rsi:.1f})")

            elif 25 <= rsi <= 45:
                score -= 1
                reasons.append(f"RSI bearish zone ({rsi:.1f})")

            elif rsi > 75:
                reasons.append(f"RSI overbought ({rsi:.1f})")

            elif rsi < 25:
                reasons.append(f"RSI oversold ({rsi:.1f})")

        liquidity_high, liquidity_low = liquidity_levels(candles["5min"])

        decision = "WAIT"

        if score >= 6:
            decision = "BUY SIDE"

        elif score <= -6:
            decision = "SELL SIDE"

        if trend_1d != "WAIT" and trend_1h != "WAIT":
            if trend_1d != trend_1h:
                decision = "WAIT"
                reasons.append("1D and 1H trend conflict")

        if decision == "BUY SIDE":
            entry = price
            invalidation = liquidity_low if liquidity_low else price * 0.995
            target = liquidity_high if liquidity_high else price * 1.01

        elif decision == "SELL SIDE":
            entry = price
            invalidation = liquidity_high if liquidity_high else price * 1.005
            target = liquidity_low if liquidity_low else price * 0.99

        else:
            entry = "--"
            invalidation = "--"
            target = "--"

        confidence = min(95, 50 + abs(score) * 4)

        if decision == "WAIT":
            confidence = min(confidence, 60)

        latest["gold"]["price"] = price
        latest["gold"]["decision"] = decision
        latest["gold"]["confidence"] = confidence
        latest["gold"]["score"] = score
        latest["gold"]["entry"] = (
            round(entry, 4) if isinstance(entry, (int, float)) else entry
        )
        latest["gold"]["invalidation"] = (
            round(invalidation, 4)
            if isinstance(invalidation, (int, float))
            else invalidation
        )
        latest["gold"]["target"] = (
            round(target, 4)
            if isinstance(target, (int, float))
            else target
        )

        latest["gold"]["trend_5m"] = trend_5m
        latest["gold"]["trend_15m"] = trend_15m
        latest["gold"]["trend_1h"] = trend_1h
        latest["gold"]["trend_1d"] = trend_1d

        latest["gold"]["rsi"] = round(rsi, 2) if rsi is not None else None
        latest["gold"]["ema20"] = round(ema20, 4) if ema20 is not None else None
        latest["gold"]["ema50"] = round(ema50, 4) if ema50 is not None else None

        latest["gold"]["candles_5m"] = len(candles["5min"])
        latest["gold"]["candles_15m"] = len(candles["15min"])
        latest["gold"]["candles_1h"] = len(candles["1h"])
        latest["gold"]["candles_1d"] = len(candles["1day"])

        latest["gold"]["why"] = " | ".join(reasons[-6:])


def gold_worker():
    print("GOLD ENGINE STARTING", flush=True)

    while True:
        try:
            if not API_KEY:
                with lock:
                    latest["gold"]["status"] = "API KEY MISSING"

                print("TWELVE_DATA_API_KEY MISSING", flush=True)
                time.sleep(10)
                continue

            def on_open(ws):
                print("TWELVE DATA GOLD CONNECTED", flush=True)

                subscribe_message = {
                    "action": "subscribe",
                    "params": {
                        "symbols": GOLD_SYMBOL
                    },
                    "apikey": API_KEY
                }

                ws.send(json.dumps(subscribe_message))

                print(
                    "GOLD SUBSCRIBE SENT:",
                    GOLD_SYMBOL,
                    flush=True
                )

                with lock:
                    latest["gold"]["status"] = "LIVE"

            def on_message(ws, message):
                try:
                    data = json.loads(message)

                    price = None

                    if isinstance(data, dict):
                        if "price" in data:
                            price = safe_float(data.get("price"))

                        elif "data" in data and isinstance(data["data"], dict):
                            price = safe_float(data["data"].get("price"))

                    if price is None:
                        return

                    with lock:
                        gold_prices.append(price)

                        latest["gold"]["price"] = price
                        latest["gold"]["status"] = "LIVE"
                        latest["gold"]["updated"] = time.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )

                    print("GOLD PRICE:", price, flush=True)

                    calculate_analysis(price)

                except Exception as e:
                    print(
                        "GOLD MESSAGE ERROR:",
                        str(e),
                        flush=True
                    )

            def on_error(ws, error):
                print(
                    "GOLD WEBSOCKET ERROR:",
                    str(error),
                    flush=True
                )

                with lock:
                    latest["gold"]["status"] = "RECONNECTING"

            def on_close(ws, close_status_code, close_msg):
                print(
                    "GOLD WEBSOCKET CLOSED:",
                    close_status_code,
                    close_msg,
                    flush=True
                )

                with lock:
                    latest["gold"]["status"] = "RECONNECTING"

            ws = websocket.WebSocketApp(
                "wss://ws.twelvedata.com/v1/quotes/price",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:
            print(
                "GOLD WORKER ERROR:",
                str(e),
                flush=True
            )

        time.sleep(3)


def fetch_candles(interval):
    try:
        if not API_KEY:
            return []

        url = "https://api.twelvedata.com/time_series"

        params = {
            "symbol": GOLD_SYMBOL,
            "interval": interval,
            "outputsize": 100,
            "apikey": API_KEY
        }

        print(
            f"CANDLE REQUEST {interval}: START",
            flush=True
        )

        response = requests.get(
            url,
            params=params,
            timeout=15
        )

        print(
            f"CANDLE REQUEST {interval}: HTTP {response.status_code}",
            flush=True
        )

        data = response.json()

        if "values" not in data:
            print(
                f"CANDLE ERROR {interval}:",
                data,
                flush=True
            )
            return []

        values = data["values"]

        values.reverse()

        print(
            f"CANDLES {interval}: {len(values)}",
            flush=True
        )

        return values

    except Exception as e:
        print(
            f"CANDLE EXCEPTION {interval}:",
            str(e),
            flush=True
        )
        return []


def candle_worker():
    print("CANDLE ENGINE STARTING", flush=True)

    while True:
        try:
            for interval in [
                "5min",
                "15min",
                "1h",
                "1day"
            ]:
                data = fetch_candles(interval)

                if data:
                    with lock:
                        candles[interval] = data

            calculate_analysis()

        except Exception as e:
            print(
                "CANDLE WORKER ERROR:",
                str(e),
                flush=True
            )

        time.sleep(60)


HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>Trading-AI</title>

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #07111f;
            color: white;
        }

        .container {
            max-width: 1000px;
            margin: auto;
            padding: 20px;
        }

        .header {
            margin-bottom: 20px;
        }

        .title {
            font-size: 30px;
            font-weight: bold;
        }

        .subtitle {
            color: #9ca3af;
            margin-top: 5px;
        }

        .card {
            background: #0d1b2a;
            border: 1px solid #1e344b;
            border-radius: 18px;
            padding: 20px;
            margin-bottom: 18px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.25);
        }

        .gold-title {
            font-size: 22px;
            font-weight: bold;
        }

        .price {
            font-size: 42px;
            font-weight: bold;
            margin: 12px 0;
        }

        .live {
            color: #43e97b;
            font-weight: bold;
        }

        .decision-box {
            margin-top: 20px;
            padding: 22px;
            border-radius: 16px;
            background: #111f31;
            text-align: center;
        }

        .decision {
            font-size: 32px;
            font-weight: bold;
            margin: 8px 0;
        }

        .confidence {
            font-size: 18px;
            color: #cbd5e1;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin-top: 18px;
        }

        .metric {
            background: #0a1624;
            padding: 14px;
            border-radius: 12px;
        }

        .metric-label {
            color: #94a3b8;
            font-size: 13px;
        }

        .metric-value {
            font-size: 18px;
            font-weight: bold;
            margin-top: 5px;
        }

        .why {
            margin-top: 18px;
            color: #cbd5e1;
            line-height: 1.6;
        }

        .trends {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 10px;
            margin-top: 18px;
        }

        .trend {
            background: #0a1624;
            padding: 12px;
            border-radius: 12px;
            text-align: center;
        }

        .trend-name {
            color: #94a3b8;
            font-size: 12px;
        }

        .trend-value {
            font-weight: bold;
            margin-top: 5px;
        }

        .footer {
            color: #64748b;
            font-size: 12px;
            text-align: center;
            margin-top: 25px;
        }

        @media (max-width: 650px) {
            .grid {
                grid-template-columns: 1fr;
            }

            .trends {
                grid-template-columns: repeat(2, 1fr);
            }

            .price {
                font-size: 34px;
            }
        }
    </style>
</head>

<body>

<div class="container">

    <div class="header">
        <div class="title">
            Trading-AI
        </div>

        <div class="subtitle">
            Live Market Intelligence Engine
        </div>
    </div>

    <div class="card">

        <div class="gold-title">
            🥇 Gold — XAU/USD
        </div>

        <div class="live" id="status">
            Connecting...
        </div>

        <div class="price" id="price">
            --
        </div>

        <div>
            Source:
            <span id="source">
                Twelve Data
            </span>
        </div>

        <div class="decision-box">

            <div>
                AI MARKET CONCLUSION
            </div>

            <div class="decision" id="decision">
                WAIT
            </div>

            <div class="confidence">
                Confidence:
                <span id="confidence">
                    0
                </span>%
            </div>

        </div>

        <div class="grid">

            <div class="metric">
                <div class="metric-label">
                    AI Score
                </div>

                <div
                    class="metric-value"
                    id="score"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    RSI
                </div>

                <div
                    class="metric-value"
                    id="rsi"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    EMA 20
                </div>

                <div
                    class="metric-value"
                    id="ema20"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    EMA 50
                </div>

                <div
                    class="metric-value"
                    id="ema50"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    Entry
                </div>

                <div
                    class="metric-value"
                    id="entry"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    Invalidation
                </div>

                <div
                    class="metric-value"
                    id="invalidation"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    Target
                </div>

                <div
                    class="metric-value"
                    id="target"
                >
                    --
                </div>
            </div>

            <div class="metric">
                <div class="metric-label">
                    Last Update
                </div>

                <div
                    class="metric-value"
                    id="updated"
                >
                    --
                </div>
            </div>

        </div>

        <div class="trends">

            <div class="trend">
                <div class="trend-name">
                    5 MIN
                </div>

                <div
                    class="trend-value"
                    id="trend5"
                >
                    --
                </div>
            </div>

            <div class="trend">
                <div class="trend-name">
                    15 MIN
                </div>

                <div
                    class="trend-value"
                    id="trend15"
                >
                    --
                </div>
            </div>

            <div class="trend">
                <div class="trend-name">
                    1 HOUR
                </div>

                <div
                    class="trend-value"
                    id="trend1h"
                >
                    --
                </div>
            </div>

            <div class="trend">
                <div class="trend-name">
                    1 DAY
                </div>

                <div
                    class="trend-value"
                    id="trend1d"
                >
                    --
                </div>
            </div>

        </div>

        <div class="why">
            <strong>AI Reasoning:</strong>
            <span id="why">
                Waiting for market data...
            </span>
        </div>

    </div>

    <div class="footer">
        Trading-AI analysis is informational and should not be treated as guaranteed financial advice.
    </div>

</div>

<script>

async function updateState() {

    try {

        const response = await fetch(
            "/api/state?t=" + Date.now(),
            {
                cache: "no-store"
            }
        );

        if (!response.ok) {
            throw new Error("State request failed");
        }

        const data = await response.json();

        const gold = data.gold;

        if (!gold) {
            return;
        }

        document.getElementById("status").innerText =
            gold.status || "Connecting...";

        document.getElementById("price").innerText =
            gold.price !== null && gold.price !== undefined
                ? Number(gold.price).toFixed(4)
                : "--";

        document.getElementById("decision").innerText =
            gold.decision || "WAIT";

        document.getElementById("confidence").innerText =
            gold.confidence ?? 0;

        document.getElementById("score").innerText =
            gold.score ?? "--";

        document.getElementById("rsi").innerText =
            gold.rsi !== null && gold.rsi !== undefined
                ? Number(gold.rsi).toFixed(2)
                : "--";

        document.getElementById("ema20").innerText =
            gold.ema20 !== null && gold.ema20 !== undefined
                ? Number(gold.ema20).toFixed(4)
                : "--";

        document.getElementById("ema50").innerText =
            gold.ema50 !== null && gold.ema50 !== undefined
                ? Number(gold.ema50).toFixed(4)
                : "--";

        document.getElementById("entry").innerText =
            gold.entry ?? "--";

        document.getElementById("invalidation").innerText =
            gold.invalidation ?? "--";

        document.getElementById("target").innerText =
            gold.target ?? "--";

        document.getElementById("updated").innerText =
            gold.updated || "--";

        document.getElementById("trend5").innerText =
            gold.trend_5m || "--";

        document.getElementById("trend15").innerText =
            gold.trend_15m || "--";

        document.getElementById("trend1h").innerText =
            gold.trend_1h || "--";

        document.getElementById("trend1d").innerText =
            gold.trend_1d || "--";

        document.getElementById("why").innerText =
            gold.why || "Waiting for market data...";

    } catch (error) {

        document.getElementById("status").innerText =
            "Connection retrying...";

    }

}

updateState();

setInterval(
    updateState,
    1000
);

</script>

</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/state")
def api_state():
    with lock:
        data = {
            "gold": dict(latest["gold"])
        }

    return jsonify(data)


@app.route("/health")
def health():
    with lock:
        return jsonify({
            "status": "ok",
            "gold_status": latest["gold"]["status"],
            "gold_price": latest["gold"]["price"]
        })


def start_engines():
    print("LIVE ENGINE STARTING", flush=True)

    threading.Thread(
        target=gold_worker,
        daemon=True
    ).start()

    threading.Thread(
        target=candle_worker,
        daemon=True
    ).start()

    print("LIVE ENGINE STARTED", flush=True)


start_engines()


if __name__ == "__main__":
    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
