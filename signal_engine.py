```python
"""
TRADING AI — MULTI-FACTOR SIGNAL ENGINE
----------------------------------------
Goal:
    Better information + fewer false signals.

This engine combines:
    1. Multi-timeframe trend
    2. Market structure
    3. Momentum
    4. RSI
    5. EMA alignment
    6. Liquidity / sweep detection
    7. Breakout confirmation
    8. Support / resistance
    9. Volatility
    10. Conflict filtering

Final outputs:
    BUY / SELL / WAIT

Important:
    This is an analysis engine, not a guarantee of profitable trades.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
import math


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class SignalResult:
    decision: str
    confidence: int
    score: float
    trend: str
    momentum: str
    structure: str
    liquidity: str
    breakout: str
    reasons: List[str]
    warnings: List[str]


# ============================================================
# BASIC INDICATORS
# ============================================================

def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    value = sum(values[:period]) / period

    for price in values[period:]:
        value = (price - value) * multiplier + value

    return value


def rsi(values: List[float], period: int = 14) -> Optional[float]:
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

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i - 1]

        tr = max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close)
        )

        trs.append(tr)

    return sum(trs[-period:]) / period


# ============================================================
# MARKET STRUCTURE
# ============================================================

def market_structure(candles: List[Candle]) -> str:
    if len(candles) < 6:
        return "UNKNOWN"

    recent = candles[-6:]

    highs = [c.high for c in recent]
    lows = [c.low for c in recent]

    first_high = max(highs[:3])
    second_high = max(highs[3:])

    first_low = min(lows[:3])
    second_low = min(lows[3:])

    if second_high > first_high and second_low > first_low:
        return "BULLISH_STRUCTURE"

    if second_high < first_high and second_low < first_low:
        return "BEARISH_STRUCTURE"

    return "MIXED_STRUCTURE"


# ============================================================
# TREND ENGINE
# ============================================================

def trend_analysis(candles: List[Candle]) -> str:
    closes = [c.close for c in candles]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)

    if ema20 is None or ema50 is None:
        return "UNKNOWN"

    price = closes[-1]

    if price > ema20 > ema50:
        return "BULLISH"

    if price < ema20 < ema50:
        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# MOMENTUM ENGINE
# ============================================================

def momentum_analysis(candles: List[Candle]) -> str:
    closes = [c.close for c in candles]

    if len(closes) < 20:
        return "UNKNOWN"

    rsi_value = rsi(closes)

    ema_fast = ema(closes, 9)
    ema_slow = ema(closes, 20)

    if rsi_value is None or ema_fast is None or ema_slow is None:
        return "UNKNOWN"

    if ema_fast > ema_slow and rsi_value >= 55:
        return "BUYING"

    if ema_fast < ema_slow and rsi_value <= 45:
        return "SELLING"

    return "WEAK/MIXED"


# ============================================================
# LIQUIDITY ENGINE
# ============================================================

def liquidity_analysis(candles: List[Candle]) -> str:
    if len(candles) < 10:
        return "UNKNOWN"

    previous = candles[-6:-1]
    current = candles[-1]

    previous_high = max(c.high for c in previous)
    previous_low = min(c.low for c in previous)

    # Liquidity sweep above previous high
    if current.high > previous_high and current.close < previous_high:
        return "HIGH_LIQUIDITY_SWEEP"

    # Liquidity sweep below previous low
    if current.low < previous_low and current.close > previous_low:
        return "LOW_LIQUIDITY_SWEEP"

    if current.close > previous_high:
        return "HIGH_BREAKOUT"

    if current.close < previous_low:
        return "LOW_BREAKDOWN"

    return "NO_CLEAR_SWEEP"


# ============================================================
# BREAKOUT CONFIRMATION
# ============================================================

def breakout_confirmation(candles: List[Candle]) -> str:
    if len(candles) < 12:
        return "UNKNOWN"

    current = candles[-1]

    previous = candles[-11:-1]

    resistance = max(c.high for c in previous)
    support = min(c.low for c in previous)

    average_volume = sum(c.volume for c in previous) / len(previous)

    # If volume isn't available, use price confirmation only.
    if average_volume <= 0:
        if current.close > resistance:
            return "BULLISH_BREAKOUT"

        if current.close < support:
            return "BEARISH_BREAKDOWN"

        return "NO_CONFIRMATION"

    if current.close > resistance and current.volume >= average_volume:
        return "BULLISH_BREAKOUT"

    if current.close < support and current.volume >= average_volume:
        return "BEARISH_BREAKDOWN"

    return "NO_CONFIRMATION"


# ============================================================
# SUPPORT / RESISTANCE
# ============================================================

def support_resistance(candles: List[Candle]) -> Dict[str, float]:
    if len(candles) < 10:
        return {}

    window = candles[-20:]

    return {
        "resistance": max(c.high for c in window),
        "support": min(c.low for c in window),
    }


# ============================================================
# VOLATILITY
# ============================================================

def volatility_state(candles: List[Candle]) -> str:
    current_atr = atr(candles)

    if current_atr is None:
        return "UNKNOWN"

    recent_ranges = [
        c.high - c.low
        for c in candles[-20:]
    ]

    average_range = sum(recent_ranges) / len(recent_ranges)

    if current_atr > average_range * 1.4:
        return "HIGH"

    if current_atr < average_range * 0.7:
        return "LOW"

    return "NORMAL"


# ============================================================
# MAIN DECISION ENGINE
# ============================================================

def generate_signal(
    candles: List[Candle],
    higher_timeframe_candles: Optional[List[Candle]] = None
) -> SignalResult:

    reasons = []
    warnings = []

    if len(candles) < 60:
        return SignalResult(
            decision="WAIT",
            confidence=0,
            score=0,
            trend="UNKNOWN",
            momentum="UNKNOWN",
            structure="UNKNOWN",
            liquidity="UNKNOWN",
            breakout="UNKNOWN",
            reasons=["Not enough market data"],
            warnings=["Need at least 60 candles"]
        )

    # --------------------------------------------------------
    # ANALYSIS
    # --------------------------------------------------------

    trend = trend_analysis(candles)
    structure = market_structure(candles)
    momentum = momentum_analysis(candles)
    liquidity = liquidity_analysis(candles)
    breakout = breakout_confirmation(candles)
    volatility = volatility_state(candles)

    closes = [c.close for c in candles]

    current_price = closes[-1]

    current_rsi = rsi(closes)

    ema20_value = ema(closes, 20)
    ema50_value = ema(closes, 50)

    score = 0.0

    # --------------------------------------------------------
    # TREND
    # --------------------------------------------------------

    if trend == "BULLISH":
        score += 2
        reasons.append("Price is above aligned EMA trend.")

    elif trend == "BEARISH":
        score -= 2
        reasons.append("Price is below aligned EMA trend.")

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    if structure == "BULLISH_STRUCTURE":
        score += 2
        reasons.append("Higher-high / higher-low structure detected.")

    elif structure == "BEARISH_STRUCTURE":
        score -= 2
        reasons.append("Lower-high / lower-low structure detected.")

    else:
        warnings.append("Market structure is mixed.")

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    if momentum == "BUYING":
        score += 1.5
        reasons.append("Momentum supports buyers.")

    elif momentum == "SELLING":
        score -= 1.5
        reasons.append("Momentum supports sellers.")

    else:
        warnings.append("Momentum is mixed.")

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if current_rsi is not None:

        if 55 <= current_rsi <= 70:
            score += 1
            reasons.append(f"RSI supports bullish momentum ({current_rsi:.1f}).")

        elif 30 <= current_rsi <= 45:
            score -= 1
            reasons.append(f"RSI supports bearish momentum ({current_rsi:.1f}).")

        elif current_rsi > 75:
            warnings.append(
                f"RSI is very high ({current_rsi:.1f}); chasing BUY is risky."
            )

        elif current_rsi < 25:
            warnings.append(
                f"RSI is very low ({current_rsi:.1f}); chasing SELL is risky."
            )

    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    if liquidity == "LOW_LIQUIDITY_SWEEP":
        score += 1.5
        reasons.append("Sell-side liquidity sweep detected.")

    elif liquidity == "HIGH_LIQUIDITY_SWEEP":
        score -= 1.5
        reasons.append("Buy-side liquidity sweep detected.")

    elif liquidity == "HIGH_BREAKOUT":
        score += 0.5
        reasons.append("Price is above recent liquidity.")

    elif liquidity == "LOW_BREAKDOWN":
        score -= 0.5
        reasons.append("Price is below recent liquidity.")

    # --------------------------------------------------------
    # BREAKOUT
    # --------------------------------------------------------

    if breakout == "BULLISH_BREAKOUT":
        score += 2
        reasons.append("Breakout has participation confirmation.")

    elif breakout == "BEARISH_BREAKDOWN":
        score -= 2
        reasons.append("Breakdown has participation confirmation.")

    # --------------------------------------------------------
    # HIGHER TIMEFRAME CONFIRMATION
    # --------------------------------------------------------

    if higher_timeframe_candles:

        higher_trend = trend_analysis(higher_timeframe_candles)

        if higher_trend == "BULLISH":
            score += 1
            reasons.append("Higher timeframe trend is bullish.")

        elif higher_trend == "BEARISH":
            score -= 1
            reasons.append("Higher timeframe trend is bearish.")

    # --------------------------------------------------------
    # SUPPORT / RESISTANCE
    # --------------------------------------------------------

    levels = support_resistance(candles)

    if levels:
        resistance = levels["resistance"]
        support = levels["support"]

        distance_to_resistance = abs(resistance - current_price)
        distance_to_support = abs(current_price - support)

        atr_value = atr(candles)

        if atr_value:
            if distance_to_resistance < atr_value * 0.5:
                warnings.append(
                    "Price is close to resistance; BUY confirmation is weaker."
                )

            if distance_to_support < atr_value * 0.5:
                warnings.append(
                    "Price is close to support; SELL confirmation is weaker."
                )

    # --------------------------------------------------------
    # CONFLICT DETECTION
    # --------------------------------------------------------

    bullish_votes = 0
    bearish_votes = 0

    if trend == "BULLISH":
        bullish_votes += 1
    elif trend == "BEARISH":
        bearish_votes += 1

    if structure == "BULLISH_STRUCTURE":
        bullish_votes += 1
    elif structure == "BEARISH_STRUCTURE":
        bearish_votes += 1

    if momentum == "BUYING":
        bullish_votes += 1
    elif momentum == "SELLING":
        bearish_votes += 1

    if breakout == "BULLISH_BREAKOUT":
        bullish_votes += 1
    elif breakout == "BEARISH_BREAKDOWN":
        bearish_votes += 1

    # Strong conflict = WAIT
    if bullish_votes >= 2 and bearish_votes >= 2:
        warnings.append("Major indicators conflict with each other.")
        return SignalResult(
            decision="WAIT",
            confidence=35,
            score=score,
            trend=trend,
            momentum=momentum,
            structure=structure,
            liquidity=liquidity,
            breakout=breakout,
            reasons=reasons,
            warnings=warnings
        )

    # --------------------------------------------------------
    # FINAL DECISION
    # --------------------------------------------------------

    max_score = 12.0

    normalized = min(abs(score) / max_score, 1.0)

    confidence = int(50 + normalized * 45)

    # Require meaningful agreement.
    if score >= 5 and bullish_votes >= 3:
        decision = "BUY"

    elif score <= -5 and bearish_votes >= 3:
        decision = "SELL"

    else:
        decision = "WAIT"
        confidence = min(confidence, 65)

    # --------------------------------------------------------
    # EXTRA FALSE SIGNAL PROTECTION
    # --------------------------------------------------------

    if volatility == "HIGH":
        warnings.append(
            "High volatility detected; breakout/reversal risk is elevated."
        )

    if volatility == "LOW":
        warnings.append(
            "Low volatility detected; directional move may be weak."
        )

    # Don't allow very high confidence when warnings are significant.
    if len(warnings) >= 3:
        confidence = min(confidence, 60)

    return SignalResult(
        decision=decision,
        confidence=max(0, min(confidence, 95)),
        score=round(score, 2),
        trend=trend,
        momentum=momentum,
        structure=structure,
        liquidity=liquidity,
        breakout=breakout,
        reasons=reasons,
        warnings=warnings
    )


# ============================================================
# SIMPLE JSON OUTPUT
# ============================================================

def signal_to_dict(result: SignalResult) -> Dict:
    return {
        "decision": result.decision,
        "confidence": result.confidence,
        "score": result.score,
        "trend": result.trend,
        "momentum": result.momentum,
        "structure": result.structure,
        "liquidity": result.liquidity,
        "breakout": result.breakout,
        "reasons": result.reasons,
        "warnings": result.warnings,
    }
```
