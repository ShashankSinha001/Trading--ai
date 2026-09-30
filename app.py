import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
import websocket
from flask import Flask, jsonify, Response

app = Flask(__name__)

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()
SYMBOL = "XAU/USD"

REST_URL = "https://api.twelvedata.com"
WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"

MAX_CANDLES = 1200
REST_FALLBACK_SECONDS = 600
HISTORY_REFRESH_SECONDS = 1800
DAILY_REFRESH_SECONDS = 21600

state = {
    "price": None,
    "timestamp": None,
    "ws_connected": False,
    "ws_error": None,
    "rest_cooldown_until": 0,
    "candles": {
        "1m": deque(maxlen=MAX_CANDLES),
        "5m": deque(maxlen=MAX_CANDLES),
        "15m": deque(maxlen=MAX_CANDLES),
        "30m": deque(maxlen=MAX_CANDLES),
        "1h": deque(maxlen=MAX_CANDLES),
        "4h": deque(maxlen=MAX_CANDLES),
        "1d": deque(maxlen=MAX_CANDLES),
    },
    "analysis": {},
    "history": [],
    "last_history_load": 0,
    "last_daily_load": 0,
    "last_rest_attempt": 0,
}

lock = threading.RLock()
startup_done = False


def now_utc():
    return datetime.now(timezone.utc)


def iso_now():
    return now_utc().isoformat()


def next_utc_midnight():
    n = now_utc()
    tomorrow = (n + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return tomorrow.timestamp()


def rest_allowed():
    return time.time() >= state["rest_cooldown_until"]


def set_rest_cooldown():
    with lock:
        state["rest_cooldown_until"] = next_utc_midnight()


def td_get(endpoint, params=None, timeout=15):
    if not API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is missing")

    if not rest_allowed():
        raise RuntimeError("REST cooldown active until next UTC midnight")

    query = dict(params or {})
    query["apikey"] = API_KEY

    with lock:
        state["last_rest_attempt"] = time.time()

    r = requests.get(
        f"{REST_URL}/{endpoint.lstrip('/')}",
        params=query,
        timeout=timeout,
    )

    if r.status_code == 429:
        set_rest_cooldown()
        raise RuntimeError(
            "Twelve Data HTTP 429: daily API quota exhausted"
        )

    r.raise_for_status()

    data = r.json()

    if isinstance(data, dict) and data.get("status") == "error":
        message = data.get(
            "message",
            "Twelve Data API error",
        )

        if (
            "credit" in message.lower()
            or "limit" in message.lower()
        ):
            set_rest_cooldown()

        raise RuntimeError(message)

    return data


def to_epoch(value):
    if isinstance(value, (int, float)):
        return int(value)

    s = str(value).strip()

    if not s:
        return None

    try:
        return int(float(s))
    except ValueError:
        pass

    s = s.replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return int(dt.timestamp())

    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(
                s,
                fmt,
            ).replace(tzinfo=timezone.utc)

            return int(dt.timestamp())

        except ValueError:
            continue

    return None


def normalize_candle(item):
    if not isinstance(item, dict):
        return None

    ts = item.get(
        "datetime",
        item.get(
            "timestamp",
            item.get("time"),
        ),
    )

    epoch = to_epoch(ts)

    if epoch is None:
        return None

    try:
        o = float(item["open"])
        h = float(item["high"])
        l = float(item["low"])
        c = float(item["close"])
    except (
        KeyError,
        TypeError,
        ValueError,
    ):
        return None

    volume = item.get("volume")

    try:
        volume = (
            float(volume)
            if volume is not None
            else 0.0
        )
    except (
        TypeError,
        ValueError,
    ):
        volume = 0.0

    return {
        "time": epoch,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": volume,
    }


def sort_unique(candles):
    by_time = {}

    for candle in candles:
        if candle and candle.get("time") is not None:
            by_time[int(candle["time"])] = candle

    return [
        by_time[k]
        for k in sorted(by_time)
    ]


def bucket_seconds(tf):
    return {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }[tf]


def aggregate(candles, seconds):
    groups = {}

    for c in candles:
        t = int(c["time"])
        bucket = (t // seconds) * seconds

        groups.setdefault(
            bucket,
            [],
        ).append(c)

    result = []

    for bucket, rows in sorted(groups.items()):
        rows.sort(
            key=lambda x: x["time"]
        )

        result.append(
            {
                "time": bucket,
                "open": rows[0]["open"],
                "high": max(
                    x["high"]
                    for x in rows
                ),
                "low": min(
                    x["low"]
                    for x in rows
                ),
                "close": rows[-1]["close"],
                "volume": sum(
                    x.get("volume", 0.0)
                    for x in rows
                ),
            }
        )

    return result


def load_history():
    if not rest_allowed():
        return False

    try:
        data = td_get(
            "time_series",
            {
                "symbol": SYMBOL,
                "interval": "5min",
                "outputsize": 500,
                "format": "JSON",
            },
        )

        values = data.get(
            "values",
            [],
        )

        candles_5m = sort_unique(
            [
                normalize_candle(x)
                for x in values
            ]
        )

        if not candles_5m:
            raise RuntimeError(
                "No 5m candles returned"
            )

        with lock:
            state["candles"]["5m"].clear()

            state["candles"]["5m"].extend(
                candles_5m[-MAX_CANDLES:]
            )

            base = list(
                state["candles"]["5m"]
            )

            for tf in (
                "15m",
                "30m",
                "1h",
                "4h",
            ):
                agg = aggregate(
                    base,
                    bucket_seconds(tf),
                )

                state["candles"][tf].clear()

                state["candles"][tf].extend(
                    agg[-MAX_CANDLES:]
                )

            state["last_history_load"] = time.time()

        return True

    except Exception as exc:
        with lock:
            state["ws_error"] = str(exc)

        return False


def load_daily_history():
    if not rest_allowed():
        return False

    try:
        data = td_get(
            "time_series",
            {
                "symbol": SYMBOL,
                "interval": "1day",
                "outputsize": 250,
                "format": "JSON",
            },
        )

        values = data.get(
            "values",
            [],
        )

        daily = sort_unique(
            [
                normalize_candle(x)
                for x in values
            ]
        )

        if not daily:
            raise RuntimeError(
                "No daily candles returned"
            )

        with lock:
            state["candles"]["1d"].clear()

            state["candles"]["1d"].extend(
                daily[-MAX_CANDLES:]
            )

            state["last_daily_load"] = time.time()

        return True

    except Exception as exc:
        with lock:
            state["ws_error"] = str(exc)

        return False


def update_live_candle(price, epoch):
    price = float(price)
    epoch = int(epoch)

    with lock:
        state["price"] = price
        state["timestamp"] = epoch

        one_minute = state["candles"]["1m"]

        bucket = (
            epoch // 60
        ) * 60

        if (
            not one_minute
            or one_minute[-1]["time"] != bucket
        ):
            one_minute.append(
                {
                    "time": bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0.0,
                }
            )

        else:
            c = one_minute[-1]

            c["high"] = max(
                c["high"],
                price,
            )

            c["low"] = min(
                c["low"],
                price,
            )

            c["close"] = price

        base = list(one_minute)

        if base:
            for tf in (
                "5m",
                "15m",
                "30m",
                "1h",
                "4h",
            ):
                agg = aggregate(
                    base,
                    bucket_seconds(tf),
                )

                target = state["candles"][tf]

                target.clear()

                target.extend(
                    agg[-MAX_CANDLES:]
                )


def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    current = sum(
        values[:period]
    ) / period

    for value in values[period:]:
        current = (
            (value - current)
            * multiplier
            + current
        )

    return current


def rsi(values, period=14):
    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(
        1,
        period + 1,
    ):
        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    avg_gain = (
        sum(gains) / period
    )

    avg_loss = (
        sum(losses) / period
    )

    for i in range(
        period + 1,
        len(values),
    ):
        change = (
            values[i]
            - values[i - 1]
        )

        gain = max(
            change,
            0,
        )

        loss = max(
            -change,
            0,
        )

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gain
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + loss
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


def atr(candles, period=14):
    if len(candles) <= period:
        return None

    trs = []

    for i in range(
        1,
        len(candles),
    ):
        current = candles[i]
        previous = candles[i - 1]

        tr = max(
            current["high"]
            - current["low"],

            abs(
                current["high"]
                - previous["close"]
            ),

            abs(
                current["low"]
                - previous["close"]
            ),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(
        trs[-period:]
    ) / period


def structure(
    candles,
    lookback=20,
):
    if len(candles) < lookback:
        return "INSUFFICIENT"

    recent = candles[-lookback:]

    half = lookback // 2

    old = recent[:half]
    new = recent[half:]

    old_high = max(
        c["high"]
        for c in old
    )

    new_high = max(
        c["high"]
        for c in new
    )

    old_low = min(
        c["low"]
        for c in old
    )

    new_low = min(
        c["low"]
        for c in new
    )

    if (
        new_high > old_high
        and new_low > old_low
    ):
        return "HIGHER"

    if (
        new_high < old_high
        and new_low < old_low
    ):
        return "LOWER"

    return "RANGE"


def liquidity_levels(
    candles,
    lookback=30,
):
    if len(candles) < 2:
        return None, None

    recent = candles[-lookback:]

    return (
        max(
            c["high"]
            for c in recent
        ),
        min(
            c["low"]
            for c in recent
        ),
    )


def detect_sweep(
    candles,
    high_liq,
    low_liq,
):
    if (
        len(candles) < 2
        or high_liq is None
        or low_liq is None
    ):
        return "NONE"

    last = candles[-1]

    if (
        last["high"] > high_liq
        and last["close"] < high_liq
    ):
        return "HIGH_SWEEP"

    if (
        last["low"] < low_liq
        and last["close"] > low_liq
    ):
        return "LOW_SWEEP"

    return "NONE"


def analyze():
    with lock:
        price = state["price"]
        candles = list(
            state["candles"]["5m"]
        )

    if (
        price is None
        or len(candles) < 50
    ):
        result = {
            "signal": "WAIT",
            "signal_strength": 0,
            "trend": "WAIT",
            "momentum": "WAIT",
            "structure": "INSUFFICIENT",
            "price": price,
            "ema20": None,
            "ema50": None,
            "rsi": None,
            "atr": None,
            "liquidity_high": None,
            "liquidity_low": None,
            "sweep": "NONE",
            "entry": None,
            "stop": None,
            "target1": None,
            "target2": None,
            "timestamp": iso_now(),
        }

        with lock:
            state["analysis"] = result

        return result

    closes = [
        c["close"]
        for c in candles
    ]

    ema20 = ema(
        closes,
        20,
    )

    ema50 = ema(
        closes,
        50,
    )

    current_rsi = rsi(
        closes,
        14,
    )

    current_atr = atr(
        candles,
        14,
    )

    struct = structure(
        candles,
        30,
    )

    liq_high, liq_low = (
        liquidity_levels(
            candles,
            30,
        )
    )

    sweep = detect_sweep(
        candles,
        liq_high,
        liq_low,
    )

    buy_score = 0
    sell_score = 0

    if (
        ema20 is not None
        and price > ema20
    ):
        buy_score += 2

    elif (
        ema20 is not None
        and price < ema20
    ):
        sell_score += 2

    if (
        ema20 is not None
        and ema50 is not None
    ):
        if ema20 > ema50:
            buy_score += 2

        elif ema20 < ema50:
            sell_score += 2

    if current_rsi is not None:
        if (
            52
            <= current_rsi
            <= 70
        ):
            buy_score += 2

        elif (
            30
            <= current_rsi
            <= 48
        ):
            sell_score += 2

        elif current_rsi > 75:
            sell_score += 1

        elif current_rsi < 25:
            buy_score += 1

    if struct == "HIGHER":
        buy_score += 2

    elif struct == "LOWER":
        sell_score += 2

    if sweep == "HIGH_SWEEP":
        sell_score += 2

    elif sweep == "LOW_SWEEP":
        buy_score += 2

    if buy_score >= sell_score + 3:
        signal = "BUY"

    elif sell_score >= buy_score + 3:
        signal = "SELL"

    else:
        signal = "WAIT"

    signal_strength = min(
        100,
        max(
            buy_score,
            sell_score,
        ) * 10,
    )

    entry = price
    stop = None
    target1 = None
    target2 = None

    if current_atr is not None:

        if signal == "BUY":
            stop = (
                price
                - current_atr * 1.2
            )

            target1 = (
                price
                + current_atr * 1.5
            )

            target2 = (
                price
                + current_atr * 2.5
            )

        elif signal == "SELL":
            stop = (
                price
                + current_atr * 1.2
            )

            target1 = (
                price
                - current_atr * 1.5
            )

            target2 = (
                price
                - current_atr * 2.5
            )

    if signal == "BUY":
        trend = "BULLISH"
        momentum = "BUYING"

    elif signal == "SELL":
        trend = "BEARISH"
        momentum = "SELLING"

    else:
        trend = "MIXED"
        momentum = "NEUTRAL"

    result = {
        "signal": signal,
        "signal_strength": signal_strength,
        "trend": trend,
        "momentum": momentum,
        "structure": struct,
        "price": price,
        "ema20": ema20,
        "ema50": ema50,
        "rsi": current_rsi,
        "atr": current_atr,
        "liquidity_high": liq_high,
        "liquidity_low": liq_low,
        "sweep": sweep,
        "entry": (
            entry
            if signal != "WAIT"
            else None
        ),
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "timestamp": iso_now(),
    }

    with lock:
        state["analysis"] = result

    return result


def websocket_worker():

    if not API_KEY:
        with lock:
            state["ws_error"] = (
                "TWELVE_DATA_API_KEY is missing"
            )

        return

    while True:

        # IMPORTANT:
        # API key is attached to the WebSocket URL.
        # Never print this URL in logs.
        url = (
            f"{WS_URL}"
            f"?apikey={API_KEY}"
        )

        try:
            ws = websocket.create_connection(
                url,
                timeout=5,
                enable_multithread=True,
            )

            with lock:
                state["ws_connected"] = True
                state["ws_error"] = None

            subscribe = {
                "action": "subscribe",
                "params": {
                    "symbols": SYMBOL,
                },
            }

            ws.send(
                json.dumps(subscribe)
            )

            last_ping = time.time()

            while True:

                try:
                    message = ws.recv()

                    if message is None:
                        raise RuntimeError(
                            "WebSocket closed"
                        )

                    if isinstance(
                        message,
                        bytes,
                    ):
                        message = (
                            message.decode(
                                "utf-8",
                                errors="ignore",
                            )
                        )

                    if message:

                        data = json.loads(
                            message
                        )

                        event = data.get(
                            "event"
                        )

                        if event == "price":

                            price = data.get(
                                "price"
                            )

                            timestamp = data.get(
                                "timestamp"
                            )

                            if price is not None:
                                try:
                                    price_f = float(
                                        price
                                    )

                                    ts = (
                                        int(
                                            float(
                                                timestamp
                                            )
                                        )
                                        if timestamp
                                        else int(
                                            time.time()
                                        )
                                    )

                                    update_live_candle(
                                        price_f,
                                        ts,
                                    )

                                except (
                                    TypeError,
                                    ValueError,
                                ):
                                    pass

                        elif (
                            event
                            == "subscribe-status"
                        ):

                            if (
                                data.get(
                                    "status"
                                )
                                == "error"
                            ):
                                with lock:
                                    state[
                                        "ws_error"
                                    ] = str(data)

                        elif (
                            data.get(
                                "status"
                            )
                            == "error"
                        ):

                            with lock:
                                state[
                                    "ws_error"
                                ] = str(data)

                except (
                    websocket.WebSocketTimeoutException
                ):
                    pass

                if (
                    time.time()
                    - last_ping
                    >= 10
                ):
                    try:
                        ws.send(
                            json.dumps(
                                {
                                    "action":
                                        "heartbeat"
                                }
                            )
                        )

                        last_ping = time.time()

                    except Exception:
                        raise RuntimeError(
                            "WebSocket heartbeat failed"
                        )

        except Exception as exc:

            with lock:
                state["ws_connected"] = False
                state["ws_error"] = str(exc)

            time.sleep(15)


def rest_fallback_worker():

    while True:

        time.sleep(20)

        with lock:
            ws_ok = state[
                "ws_connected"
            ]

        if ws_ok:
            continue

        if not rest_allowed():
            continue

        if (
            time.time()
            - state["last_rest_attempt"]
            < REST_FALLBACK_SECONDS
        ):
            continue

        try:

            data = td_get(
                "price",
                {
                    "symbol": SYMBOL,
                },
            )

            price_value = data.get(
                "price"
            )

            if price_value is None:
                raise RuntimeError(
                    f"Invalid price response: {data}"
                )

            update_live_candle(
                float(price_value),
                int(time.time()),
            )

        except Exception as exc:

            with lock:
                state[
                    "ws_error"
                ] = str(exc)


def history_worker():

    time.sleep(3)

    if rest_allowed():
        load_history()

    time.sleep(2)

    if rest_allowed():
        load_daily_history()

    while True:

        time.sleep(60)

        now = time.time()

        with lock:
            last_hist = (
                state[
                    "last_history_load"
                ]
            )

            last_daily = (
                state[
                    "last_daily_load"
                ]
            )

        if (
            rest_allowed()
            and now - last_hist
            >= HISTORY_REFRESH_SECONDS
        ):
            load_history()

        if (
            rest_allowed()
            and now - last_daily
            >= DAILY_REFRESH_SECONDS
        ):
            load_daily_history()


def analysis_worker():

    while True:

        try:
            analyze()

        except Exception as exc:

            with lock:
                state[
                    "ws_error"
                ] = (
                    f"Analysis error: {exc}"
                )

        time.sleep(5)


def start_background_workers():

    global startup_done

    if startup_done:
        return

    startup_done = True

    workers = [
        websocket_worker,
        rest_fallback_worker,
        history_worker,
        analysis_worker,
    ]

    for target in workers:

        thread = threading.Thread(
            target=target,
            daemon=True,
            name=target.__name__,
        )

        thread.start()


@app.route("/")
def home():
    return Response(
        HTML_PAGE,
        mimetype="text/html",
    )


@app.route("/health")
def health():

    with lock:

        return jsonify(
            {
                "status": "ok",
                "symbol": SYMBOL,
                "price": state["price"],
                "ws_connected":
                    state["ws_connected"],
                "ws_error":
                    state["ws_error"],
                "rest_cooldown":
                    max(
                        0,
                        int(
                            state[
                                "rest_cooldown_until"
                            ]
                            - time.time()
                        ),
                    ),
            }
        )


@app.route("/api/market")
def api_market():

    with lock:

        return jsonify(
            {
                "symbol": SYMBOL,
                "price": state["price"],
                "timestamp":
                    state["timestamp"],
                "ws_connected":
                    state["ws_connected"],
                "ws_error":
                    state["ws_error"],
                "analysis":
                    state["analysis"],
            }
        )


@app.route("/api/candles")
def api_candles():

    with lock:

        result = {
            tf: list(
                state["candles"][tf]
            )[-500:]
            for tf in state["candles"]
        }

    return jsonify(result)


@app.route("/stream")
def stream():

    return Response(
        "Trading AI live stream endpoint",
        mimetype="text/plain",
    )


HTML_PAGE = r"""
<!doctype html>
<html lang="en">

<head>

<meta charset="utf-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1"
>

<title>Trading AI</title>

<script
src="https://unpkg.com/lightweight-charts@5.2.1/dist/lightweight-charts.standalone.production.js">
</script>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    background:#071018;
    color:#e9eef5;
    font-family:Arial,Helvetica,sans-serif;
}

header{
    padding:18px 24px;
    border-bottom:1px solid #1d2a35;
    display:flex;
    justify-content:space-between;
    align-items:center;
}

.brand{
    font-size:24px;
    font-weight:700;
}

.status{
    font-size:13px;
    color:#9caab8;
}

.container{
    padding:20px;
    max-width:1500px;
    margin:auto;
}

.hero{
    display:grid;
    grid-template-columns:1.2fr .8fr;
    gap:16px;
}

.card{
    background:#0d1822;
    border:1px solid #1b2a36;
    border-radius:14px;
    padding:18px;
}

.symbol{
    color:#8ea0b1;
    font-size:14px;
}

.price{
    font-size:42px;
    font-weight:700;
    margin:8px 0;
}

.signal{
    font-size:34px;
    font-weight:800;
}

.buy{
    color:#35d07f;
}

.sell{
    color:#ff6574;
}

.wait{
    color:#f2c94c;
}

.grid{
    display:grid;
    grid-template-columns:
        repeat(4,1fr);
    gap:12px;
    margin-top:16px;
}

.metric{
    background:#0a141d;
    border:1px solid #192833;
    border-radius:12px;
    padding:14px;
}

.metric .label{
    color:#7f91a1;
    font-size:12px;
}

.metric .value{
    font-size:18px;
    font-weight:700;
    margin-top:6px;
}

.chart-card{
    margin-top:16px;
}

#chart{
    height:520px;
    width:100%;
}

.small{
    color:#7f91a1;
    font-size:12px;
}

@media(max-width:900px){

    .hero{
        grid-template-columns:1fr;
    }

    .grid{
        grid-template-columns:
            repeat(2,1fr);
    }
}

</style>

</head>

<body>

<header>

<div class="brand">
    Trading AI
</div>

<div
    class="status"
    id="connection"
>
    CONNECTING...
</div>

</header>

<div class="container">

<div class="hero">

<div class="card">

<div class="symbol">
    XAU/USD • GOLD
</div>

<div
    class="price"
    id="price"
>
    --
</div>

<div
    class="small"
    id="time"
>
    Waiting for live data...
</div>

</div>

<div class="card">

<div class="symbol">
    AI MARKET SIGNAL
</div>

<div
    class="signal wait"
    id="signal"
>
    WAIT
</div>

<div
    class="small"
    id="strength"
>
    Signal Strength: 0%
</div>

</div>

</div>

<div class="grid">

<div class="metric">

<div class="label">
    TREND
</div>

<div
    class="value"
    id="trend"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    MOMENTUM
</div>

<div
    class="value"
    id="momentum"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    STRUCTURE
</div>

<div
    class="value"
    id="structure"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    RSI
</div>

<div
    class="value"
    id="rsi"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    EMA 20
</div>

<div
    class="value"
    id="ema20"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    EMA 50
</div>

<div
    class="value"
    id="ema50"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    LIQUIDITY HIGH
</div>

<div
    class="value"
    id="liqHigh"
>
    --
</div>

</div>

<div class="metric">

<div class="label">
    LIQUIDITY LOW
</div>

<div
    class="value"
    id="liqLow"
>
    --
</div>

</div>

</div>

<div class="card chart-card">

<div
style="
display:flex;
justify-content:space-between;
align-items:center;
margin-bottom:10px
"
>

<div>

<b>
    Gold — 5 Minute Structure
</b>

<div class="small">
    Live candles + AI market structure
</div>

</div>

<div
    class="small"
    id="sweep"
>
    Sweep: --
</div>

</div>

<div id="chart"></div>

</div>

</div>

<script>

const chart =
    LightweightCharts.createChart(
        document.getElementById(
            "chart"
        ),
        {
            layout:{
                background:{
                    color:"#0d1822"
                },
                textColor:"#9caab8"
            },

            grid:{
                vertLines:{
                    color:"#16232d"
                },

                horzLines:{
                    color:"#16232d"
                }
            },

            rightPriceScale:{
                borderColor:"#263641"
            },

            timeScale:{
                borderColor:"#263641",
                timeVisible:true,
                secondsVisible:false
            }
        }
    );


const candleSeries =
    chart.addSeries(
        LightweightCharts.CandlestickSeries,
        {
            upColor:"#35d07f",
            downColor:"#ff6574",
            borderVisible:false,
            wickUpColor:"#35d07f",
            wickDownColor:"#ff6574"
        }
    );


async function loadCandles(){

    try{

        const r =
            await fetch(
                "/api/candles",
                {
                    cache:"no-store"
                }
            );

        const data =
            await r.json();

        const candles =
            data["5m"] || [];

        candleSeries.setData(
            candles.map(
                c => ({
                    time:c.time,
                    open:c.open,
                    high:c.high,
                    low:c.low,
                    close:c.close
                })
            )
        );

        chart
            .timeScale()
            .fitContent();

    }catch(e){

        console.error(e);

    }

}


function fmt(
    v,
    digits=3
){

    if(
        v === null
        || v === undefined
        || Number.isNaN(
            Number(v)
        )
    ){
        return "--";
    }

    return Number(v).toFixed(
        digits
    );
}


function setSignal(
    signal
){

    const el =
        document.getElementById(
            "signal"
        );

    el.textContent =
        signal || "WAIT";

    el.className =
        "signal " +
        (
            signal === "BUY"
            ? "buy"
            : signal === "SELL"
            ? "sell"
            : "wait"
        );

}


async function update(){

    try{

        const r =
            await fetch(
                "/api/market",
                {
                    cache:"no-store"
                }
            );

        const data =
            await r.json();

        const a =
            data.analysis || {};

        document.getElementById(
            "price"
        ).textContent =
            fmt(
                data.price,
                3
            );

        document.getElementById(
            "connection"
        ).textContent =
            data.ws_connected
            ? "● LIVE"
            : "○ RECONNECTING";

        if(data.timestamp){

            document.getElementById(
                "time"
            ).textContent =
                new Date(
                    data.timestamp * 1000
                ).toLocaleString();

        }

        setSignal(
            a.signal
        );

        document.getElementById(
            "strength"
        ).textContent =
            "Signal Strength: "
            + (
                a.signal_strength
                ?? 0
            )
            + "%";

        document.getElementById(
            "trend"
        ).textContent =
            a.trend || "--";

        document.getElementById(
            "momentum"
        ).textContent =
            a.momentum || "--";

        document.getElementById(
            "structure"
        ).textContent =
            a.structure || "--";

        document.getElementById(
            "rsi"
        ).textContent =
            fmt(
                a.rsi,
                2
            );

        document.getElementById(
            "ema20"
        ).textContent =
            fmt(
                a.ema20,
                3
            );

        document.getElementById(
            "ema50"
        ).textContent =
            fmt(
                a.ema50,
                3
            );

        document.getElementById(
            "liqHigh"
        ).textContent =
            fmt(
                a.liquidity_high,
                3
            );

        document.getElementById(
            "liqLow"
        ).textContent =
            fmt(
                a.liquidity_low,
                3
            );

        document.getElementById(
            "sweep"
        ).textContent =
            "Sweep: "
            + (
                a.sweep
                || "NONE"
            );

    }catch(e){

        document.getElementById(
            "connection"
        ).textContent =
            "○ OFFLINE";

        console.error(e);

    }

}


loadCandles();
update();

setInterval(
    update,
    3000
);

setInterval(
    loadCandles,
    10000
);


window.addEventListener(
    "resize",
    () => {

        chart.applyOptions(
            {
                width:
                    document
                    .getElementById(
                        "chart"
                    )
                    .clientWidth
            }
        );

    }
);

</script>

</body>
</html>
"""


start_background_workers()


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
        threaded=True,
    )
