from flask import Flask, jsonify
import yfinance as yf

app = Flask(__name__)

@app.route("/")
def home():
    return """
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Trading-AI</title>
    </head>
    <body>
        <h1>Trading-AI</h1>
        <p>Trading AI server is running successfully.</p>
        <p>Crude Oil + Gold analysis coming next.</p>
    </body>
    </html>
    """

@app.route("/price/<symbol>")
def price(symbol):
    try:
        data = yf.Ticker(symbol).history(period="1d", interval="5m")

        if data.empty:
            return jsonify({"error": "No market data found"}), 404

        last = data.iloc[-1]

        return jsonify({
            "symbol": symbol,
            "price": round(float(last["Close"]), 4),
            "time": str(data.index[-1])
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run()
