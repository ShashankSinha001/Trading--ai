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
            background: #111827;
            color: white;
        }

        .container {
            max-width: 700px;
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
            color: #9ca3af;
            margin-bottom: 25px;
        }

        .card {
            background: #1f2937;
            border-radius: 15px;
            padding: 20px;
            margin-bottom: 18px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.25);
        }

        .card h2 {
            margin-top: 0;
        }

        .price {
            font-size: 32px;
            font-weight: bold;
            margin: 12px 0;
        }

        .trend {
            font-size: 18px;
            margin-top: 10px;
        }

        .buy {
            color: #22c55e;
        }

        .sell {
            color: #ef4444;
        }

        .neutral {
            color: #facc15;
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
            color: #6b7280;
            font-size: 13px;
            margin-top: 25px;
        }
    </style>
</head>

<body>

<div class="container">

    <h1>Trading-AI</h1>
    <div class="subtitle">
        Crude Oil + Gold Market Dashboard
    </div>

    <div class="card">
        <h2>🛢️ Crude Oil (WTI)</h2>

        <div class="price">
            {{ oil_price }}
        </div>

        <div class="trend">
            Trend:
            <span class="{{ oil_class }}">
                {{ oil_trend }}
            </span>
        </div>
    </div>

    <div class="card">
        <h2>🥇 Gold</h2>

        <div class="price">
            {{ gold_price }}
        </div>

        <div class="trend">
            Trend:
            <span class="{{ gold_class }}">
                {{ gold_trend }}
            </span>
        </div>
    </div>

    <button class="refresh" onclick="location.reload()">
        🔄 Refresh Market
    </button>

    <div class="footer">
        Trading-AI • Market data powered by Yahoo Finance
    </div>

</div>

</body>
</html>
"""


def get_market_data(symbol):
    try:
        data = yf.Ticker(symbol).history(period="2d", interval="5m")

        if data.empty:
            return "Data unavailable", "Unavailable", "neutral"

        current_price = float(data["Close"].iloc[-1])

        if len(data) >= 10:
            old_price = float(data["Close"].iloc[-10])

            if current_price > old_price:
                trend = "BUY / UP"
                trend_class = "buy"
            elif current_price < old_price:
                trend = "SELL / DOWN"
                trend_class = "sell"
            else:
                trend = "NEUTRAL"
                trend_class = "neutral"
        else:
            trend = "NEUTRAL"
            trend_class = "neutral"

        return f"${current_price:,.2f}", trend, trend_class

    except Exception:
        return "Unavailable", "Data unavailable", "neutral"


@app.route("/")
def home():

    oil_price, oil_trend, oil_class = get_market_data("CL=F")

    gold_price, gold_trend, gold_class = get_market_data("GC=F")

    return render_template_string(
        HTML,
        oil_price=oil_price,
        oil_trend=oil_trend,
        oil_class=oil_class,
        gold_price=gold_price,
        gold_trend=gold_trend,
        gold_class=gold_class
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
