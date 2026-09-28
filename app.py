from flask import Flask, render_template_string
import yfinance as yf

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Trading-AI</title>

    <style>
        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #0f172a;
            color: white;
        }

        .container {
            max-width: 850px;
            margin: auto;
            padding: 25px 15px;
        }

        h1 {
            text-align: center;
            color: #60a5fa;
            margin-bottom: 5px;
        }

        .subtitle {
            text-align: center;
            color: #94a3b8;
            margin-bottom: 25px;
        }

        .card {
            background: #1e293b;
            border-radius: 16px;
            padding: 22px;
            margin-bottom: 20px;
            box-shadow: 0 5px 18px rgba(0,0,0,0.3);
        }

        .card h2 {
            margin-top: 0;
        }

        .price {
            font-size: 34px;
            font-weight: bold;
            margin: 15px 0;
        }

        .row {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid #334155;
        }

        .label {
            color: #94a3b8;
        }

        .up {
            color: #22c55e;
            font-weight: bold;
        }

        .down {
            color: #ef4444;
            font-weight: bold;
        }

        .neutral {
            color: #facc15;
            font-weight: bold;
        }

        .signal {
            margin-top: 18px;
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            font-size: 21px;
            font-weight: bold;
            background: #334155;
        }

        .refresh {
            display: block;
            margin: 25px auto;
            padding: 13px 28px;
            border: none;
            border-radius: 10px;
            background: #2563eb;
            color: white;
            font-size: 16px;
            cursor: pointer;
        }

        .footer {
            text-align: center;
            color: #64748b;
            font-size: 13px;
            margin-top: 25px;
        }
    </style>
</head>

<body>

<div class="container">

    <h1>Trading-AI</h1>

    <div class="subtitle">
        Gold + Crude Oil Market Intelligence
    </div>

    <div class="card">
        <h2>🛢️ Crude Oil — WTI</h2>

        <div class="price">{{ oil.price }}</div>

        <div class="row">
            <span class="label">1 Minute</span>
            <span class="{{ oil.m1_class }}">{{ oil.m1 }}</span>
        </div>

        <div class="row">
            <span class="label">5 Minute</span>
            <span class="{{ oil.m5_class }}">{{ oil.m5 }}</span>
        </div>

        <div class="row">
            <span class="label">15 Minute</span>
            <span class="{{ oil.m15_class }}">{{ oil.m15 }}</span>
        </div>

        <div class="row">
            <span class="label">1 Hour</span>
            <span class="{{ oil.h1_class }}">{{ oil.h1 }}</span>
        </div>

        <div class="row">
            <span class="label">Momentum</span>
            <span class="{{ oil.momentum_class }}">{{ oil.momentum }}</span>
        </div>

        <div class="row">
            <span class="label">Support</span>
            <span>{{ oil.support }}</span>
        </div>

        <div class="row">
            <span class="label">Resistance</span>
            <span>{{ oil.resistance }}</span>
        </div>

        <div class="signal">
            Market Condition: {{ oil.condition }}
        </div>
    </div>


    <div class="card">
        <h2>🥇 Gold</h2>

        <div class="price">{{ gold.price }}</div>

        <div class="row">
            <span class="label">1 Minute</span>
            <span class="{{ gold.m1_class }}">{{ gold.m1 }}</span>
        </div>

        <div class="row">
            <span class="label">5 Minute</span>
            <span class="{{ gold.m5_class }}">{{ gold.m5 }}</span>
        </div>

        <div class="row">
            <span class="label">15 Minute</span>
            <span class="{{ gold.m15_class }}">{{ gold.m15 }}</span>
        </div>

        <div class="row">
            <span class="label">1 Hour</span>
            <span class="{{ gold.h1_class }}">{{ gold.h1 }}</span>
        </div>

        <div class="row">
            <span class="label">Momentum</span>
            <span class="{{ gold.momentum_class }}">{{ gold.momentum }}</span>
        </div>

        <div class="row">
            <span class="label">Support</span>
            <span>{{ gold.support }}</span>
        </div>

        <div class="row">
            <span class="label">Resistance</span>
            <span>{{ gold.resistance }}</span>
        </div>

        <div class="signal">
            Market Condition: {{ gold.condition }}
        </div>
    </div>


    <button class="refresh" onclick="location.reload()">
        🔄 Refresh Market
    </button>

    <div class="footer">
        Trading-AI • Gold + Crude Oil Intelligence Engine
    </div>

</div>

</body>
</html>
"""


def get_market_data(symbol):

    try:
        data = yf.Ticker(symbol).history(
            period="5d",
            interval="5m"
        )

        if data.empty:
            return {
                "price": "Unavailable",
                "m1": "Unavailable",
                "m5": "Unavailable",
                "m15": "Unavailable",
                "h1": "Unavailable",
                "momentum": "Unavailable",
                "support": "Unavailable",
                "resistance": "Unavailable",
                "condition": "Data unavailable"
            }

        close = data["Close"].dropna()

        current = float(close.iloc[-1])

        def trend(periods):

            if len(close) < periods + 1:
                return "N/A", "neutral"

            old = float(close.iloc[-periods - 1])

            if current > old:
                return "UP", "up"

            elif current < old:
                return "DOWN", "down"

            return "FLAT", "neutral"


        m1, m1_class = trend(1)
        m5, m5_class = trend(5)
        m15, m15_class = trend(15)
        h1, h1_class = trend(60)


        momentum_change = 0

        if len(close) >= 10:
            old = float(close.iloc[-10])
            momentum_change = ((current - old) / old) * 100


        if momentum_change > 0.15:
            momentum = "STRONG"
            momentum_class = "up"

        elif momentum_change < -0.15:
            momentum = "WEAK"
            momentum_class = "down"

        else:
            momentum = "NEUTRAL"
            momentum_class = "neutral"


        recent = close.tail(60)

        support = float(recent.min())
        resistance = float(recent.max())


        up_count = sum([
            m1 == "UP",
            m5 == "UP",
            m15 == "UP",
            h1 == "UP"
        ])

        down_count = sum([
            m1 == "DOWN",
            m5 == "DOWN",
            m15 == "DOWN",
            h1 == "DOWN"
        ])


        if up_count >= 3:
            condition = "BULLISH"

        elif down_count >= 3:
            condition = "BEARISH"

        else:
            condition = "MIXED / WAIT"


        return {
            "price": f"${current:,.2f}",

            "m1": m1,
            "m1_class": m1_class,

            "m5": m5,
            "m5_class": m5_class,

            "m15": m15,
            "m15_class": m15_class,

            "h1": h1,
            "h1_class": h1_class,

            "momentum": momentum,
            "momentum_class": momentum_class,

            "support": f"${support:,.2f}",
            "resistance": f"${resistance:,.2f}",

            "condition": condition
        }

    except Exception as e:

        return {
            "price": "Unavailable",
            "m1": "Unavailable",
            "m5": "Unavailable",
            "m15": "Unavailable",
            "h1": "Unavailable",
            "momentum": "Unavailable",
            "support": "Unavailable",
            "resistance": "Unavailable",
            "condition": "Data unavailable"
        }


@app.route("/")
def home():

    oil = get_market_data("CL=F")

    gold = get_market_data("GC=F")

    return render_template_string(
        HTML,
        oil=oil,
        gold=gold
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=10000
    )
