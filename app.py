from flask import Flask, render_template_string, jsonify
import os
import requests
import time
import threading

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")

# -------------------------------------------------
# SYMBOLS
# -------------------------------------------------
SYMBOLS = {
    "gold": {
        "name": "Gold — XAU/USD",
        "symbol": "XAU/USD"
    },
    "oil": {
        "name": "Crude Oil — WTI",
        "symbol": "WTI/USD"
    }
}

# -------------------------------------------------
# MARKET DATA
# -------------------------------------------------
market = {
    "gold": {
        "price": 0,
        "trend": "WAITING",
        "momentum": "WAITING",
        "structure": "WAITING",
        "rsi": 0,
        "ema20": 0,
        "ema50": 0,
        "support": 0,
        "resistance": 0,
        "signal": "WAIT",
        "confidence": 50,
        "score": 0,
        "updated": ""
    },
    "oil": {
        "price": 0,
        "trend": "WAITING",
        "momentum": "WAITING",
        "structure": "WAITING",
        "rsi": 0,
        "ema20": 0,
        "ema50": 0,
        "support": 0,
        "resistance": 0,
        "signal": "WAIT",
        "confidence": 50,
        "score": 0,
        "updated": ""
    }
}


# -------------------------------------------------
# TWELVE DATA HELPERS
# -------------------------------------------------
def get_price(symbol):
    try:
        url = "https://api.twelvedata.com/price"

        params = {
            "symbol": symbol,
            "apikey": API_KEY
        }

        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        if "price" in data:
            return float(data["price"])

    except Exception as e:
        print("PRICE ERROR:", e)

    return None


def get_indicator(indicator, symbol, interval="5min"):
    try:
        url = f"https://api.twelvedata.com/{indicator}"

        params = {
            "symbol": symbol,
            "interval": interval,
            "apikey": API_KEY,
            "outputsize": 50
        }

        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        values = data.get("values")

        if values:
            return values

    except Exception as e:
        print(indicator, "ERROR:", e)

    return None


# -------------------------------------------------
# MARKET ANALYSIS
# -------------------------------------------------
def analyze(asset):

    symbol = SYMBOLS[asset]["symbol"]

    price = get_price(symbol)

    if price is None:
        return

    # EMA 20
    ema20_data = get_indicator("ema", symbol)

    # EMA 50
    ema50_data = get_indicator(
        "ema",
        symbol,
        "5min"
    )

    # RSI
    rsi_data = get_indicator(
        "rsi",
        symbol,
        "5min"
    )

    ema20 = price
    ema50 = price
    rsi = 50

    try:
        if ema20_data:
            ema20 = float(ema20_data[0]["ema"])

        if ema50_data:
            ema50 = float(ema50_data[0]["ema"])

        if rsi_data:
            rsi = float(rsi_data[0]["rsi"])

    except Exception:
        pass

    # -------------------------------------------------
    # TREND
    # -------------------------------------------------

    if price > ema20 and ema20 >= ema50:
        trend = "BULLISH"

    elif price < ema20 and ema20 <= ema50:
        trend = "BEARISH"

    else:
        trend = "NEUTRAL"


    # -------------------------------------------------
    # MOMENTUM
    # -------------------------------------------------

    if rsi >= 55:
        momentum = "BUYING"

    elif rsi <= 45:
        momentum = "SELLING"

    else:
        momentum = "NEUTRAL"


    # -------------------------------------------------
    # STRUCTURE
    # -------------------------------------------------

    if price > ema20:
        structure = "HIGHER"

    elif price < ema20:
        structure = "LOWER"

    else:
        structure = "RANGE"


    # -------------------------------------------------
    # SUPPORT / RESISTANCE
    # -------------------------------------------------

    support = min(price, ema20, ema50)
    resistance = max(price, ema20, ema50)

    # Small calculated range
    support = support - abs(price * 0.00025)
    resistance = resistance + abs(price * 0.00025)


    # -------------------------------------------------
    # AI STYLE DECISION ENGINE
    # -------------------------------------------------

    score = 0

    if trend == "BULLISH":
        score += 3

    if trend == "BEARISH":
        score -= 3

    if momentum == "BUYING":
        score += 2

    if momentum == "SELLING":
        score -= 2

    if price > ema20:
        score += 1

    else:
        score -= 1

    if price > ema50:
        score += 1

    else:
        score -= 1


    if score >= 4:
        signal = "BUY"

    elif score <= -4:
        signal = "SELL"

    else:
        signal = "WAIT"


    confidence = min(
        95,
        max(
            50,
            50 + abs(score) * 7
        )
    )


    market[asset] = {
        "price": price,
        "trend": trend,
        "momentum": momentum,
        "structure": structure,
        "rsi": round(rsi, 2),
        "ema20": round(ema20, 3),
        "ema50": round(ema50, 3),
        "support": round(support, 3),
        "resistance": round(resistance, 3),
        "signal": signal,
        "confidence": confidence,
        "score": score,
        "updated": time.strftime("%H:%M:%S")
    }


# -------------------------------------------------
# BACKGROUND LIVE UPDATE
# -------------------------------------------------

def market_loop():

    while True:

        try:
            analyze("gold")
            analyze("oil")

        except Exception as e:
            print("LOOP ERROR:", e)

        time.sleep(20)


threading.Thread(
    target=market_loop,
    daemon=True
).start()


# -------------------------------------------------
# API
# -------------------------------------------------

@app.route("/api/market")
def api_market():
    return jsonify(market)


# -------------------------------------------------
# OLD DASHBOARD UI
# -------------------------------------------------

HTML = """

<!DOCTYPE html>

<html>

<head>

<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>Trading AI</title>


<style>

* {
    box-sizing: border-box;
}


body {

    margin: 0;

    font-family:
    Arial,
    Helvetica,
    sans-serif;

    background:
    radial-gradient(
        circle at top,
        #143d78,
        #07152d 55%,
        #020817
    );

    color: white;

    min-height: 100vh;

}


.header {

    text-align: center;

    padding: 28px 15px 18px;

}


.header h1 {

    margin: 0;

    font-size: 28px;

}


.header p {

    margin-top: 7px;

    color: #78a7ff;

    font-size: 14px;

}


.container {

    width: 92%;

    max-width: 1100px;

    margin: auto;

}


.card {

    background:
    linear-gradient(
        145deg,
        rgba(20,43,82,.92),
        rgba(5,20,45,.94)
    );

    border: 1px solid
    rgba(100,150,255,.18);

    border-radius: 18px;

    padding: 25px;

    margin-bottom: 25px;

    box-shadow:
    0 15px 45px
    rgba(0,0,0,.35);

}


.asset-title {

    font-size: 20px;

    font-weight: bold;

}


.price {

    font-size: 38px;

    font-weight: bold;

    margin-top: 12px;

}


.live {

    color: #29e68b;

    font-weight: bold;

    margin-top: 5px;

}


.metrics {

    display: grid;

    grid-template-columns:
    repeat(5, 1fr);

    gap: 10px;

    margin-top: 25px;

}


.metric {

    background:
    rgba(13,38,75,.75);

    border:
    1px solid
    rgba(100,150,255,.14);

    border-radius: 12px;

    padding: 14px;

    min-height: 70px;

}


.label {

    color: #77a6ed;

    font-size: 11px;

    margin-bottom: 7px;

}


.value {

    font-size: 17px;

    font-weight: bold;

}


.analysis {

    margin-top: 15px;

    background:
    linear-gradient(
        100deg,
        #124f9e,
        #0b3978
    );

    border-radius: 13px;

    padding: 17px;

}


.analysis-title {

    color: #79aaf7;

    font-size: 11px;

}


.signal {

    font-size: 27px;

    font-weight: bold;

    margin-top: 5px;

}


.confidence {

    margin-top: 5px;

    font-size: 15px;

}


.score {

    margin-top: 8px;

    font-weight: bold;

}


.status {

    text-align: center;

    color: #7fa9e8;

    font-size: 12px;

    padding: 10px;

}


@media(max-width: 800px) {

    .metrics {

        grid-template-columns:
        repeat(2, 1fr);

    }

}


@media(max-width: 480px) {

    .container {

        width: 95%;

    }

    .card {

        padding: 17px;

    }

    .price {

        font-size: 32px;

    }

    .metrics {

        grid-template-columns:
        repeat(2, 1fr);

    }

}

</style>

</head>


<body>


<div class="header">

<h1>Trading AI</h1>

<p>Real-Time Market Intelligence</p>

</div>


<div class="container">


<div id="gold"></div>

<div id="oil"></div>


<div class="status">

LIVE MARKET DATA • UPDATES AUTOMATICALLY

</div>


</div>


<script>


function number(value) {

    if (!value)
        return "—";

    return Number(value).toFixed(3);

}


function createCard(id, data) {

    return `

    <div class="card">

        <div class="asset-title">

            ${id === "gold"
                ? "🥇 "
                : "🛢️ "
            }

            ${data.name ||
              (id === "gold"
                ? "Gold — XAU/USD"
                : "Crude Oil — WTI"
              )}

        </div>


        <div class="price">

            ${number(data.price)}

        </div>


        <div class="live">

            ● LIVE

        </div>


        <div class="metrics">


            <div class="metric">

                <div class="label">
                    TREND
                </div>

                <div class="value">
                    ${data.trend}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    MOMENTUM
                </div>

                <div class="value">
                    ${data.momentum}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    STRUCTURE
                </div>

                <div class="value">
                    ${data.structure}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    RSI
                </div>

                <div class="value">
                    ${number(data.rsi)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    EMA 20
                </div>

                <div class="value">
                    ${number(data.ema20)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    EMA 50
                </div>

                <div class="value">
                    ${number(data.ema50)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    SUPPORT
                </div>

                <div class="value">
                    ${number(data.support)}
                </div>

            </div>


            <div class="metric">

                <div class="label">
                    RESISTANCE
                </div>

                <div class="value">
                    ${number(data.resistance)}
                </div>

            </div>


        </div>


        <div class="analysis">

            <div class="analysis-title">

                TRADING-AI ANALYSIS

            </div>


            <div class="signal">

                ${data.signal}

            </div>


            <div class="confidence">

                Confidence:
                ${data.confidence}%

            </div>


            <div class="score">

                AI SCORE:
                ${data.score}

            </div>

        </div>


    </div>

    `;

}


async function updateMarket() {

    try {

        const response =
            await fetch("/api/market");

        const data =
            await response.json();


        data.gold.name =
            "Gold — XAU/USD";

        data.oil.name =
            "Crude Oil — WTI";


        document.getElementById(
            "gold"
        ).innerHTML =
            createCard(
                "gold",
                data.gold
            );


        document.getElementById(
            "oil"
        ).innerHTML =
            createCard(
                "oil",
                data.oil
            );


    }

    catch(error) {

        console.log(error);

    }

}


updateMarket();


setInterval(
    updateMarket,
    5000
);


</script>


</body>

</html>

"""


# -------------------------------------------------
# HOME
# -------------------------------------------------

@app.route("/")
def home():

    return render_template_string(HTML)


# -------------------------------------------------
# RUN
# -------------------------------------------------

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                10000
            )
        )
    )
