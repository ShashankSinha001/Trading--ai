"""
TRADING AI - MULTI-FACTOR SIGNAL ENGINE
Goal: Better information + fewer false signals.

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
11. Signal quality filtering

Final outputs:
BUY / SELL / WAIT

This is an analysis engine and does not guarantee profitable trades.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional


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
        value = ((price - value) * multiplier) + value

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
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i - 1]

        true_range = max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )

        true_ranges.append(true_range)

    if len(true_ranges) < period:
        return None

    return sum(true_ranges[-period:]) / period


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

    ema20_value = ema(closes, 20)
    ema50_value = ema(closes, 50)

    if ema20_value is None or ema50_value is None:
        return "UNKNOWN"

    price = closes[-1]

    if price > ema20_value > ema50_value:
        return "BULLISH"

    if price < ema20_value < ema50_value:
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
    ema9_value = ema(closes, 9)
    ema20_value = ema(closes, 20)

    if rsi_value is None or ema9_value is None or ema20_value is None:
        return "UNKNOWN"

    if ema9_value > ema20_value and rsi_value >= 55:
        return "BUYING"

    if ema9_value < ema20_value and rsi_value <= 45:
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

    # Price takes high liquidity but closes back below it.
    if current.high > previous_high and current.close < previous_high:
        return "HIGH_LIQUIDITY_SWEEP"

    # Price takes low liquidity but closes back above it.
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

    # Some market-data feeds do not provide volume.
    # In that case price confirmation is still evaluated.
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

    recent = candles[-20:]

    if not recent:
        return "UNKNOWN"

    recent_ranges = [
        max(0.0, c.high - c.low)
        for c in recent
    ]

    average_range = sum(recent_ranges) / len(recent_ranges)

    if average_range <= 0:
        return "UNKNOWN"

    if current_atr > average_range * 1.4:
        return "HIGH"

    if current_atr < average_range * 0.7:
        return "LOW"

    return "NORMAL"


# ============================================================
# CANDLE DIRECTION
# ============================================================

def recent_candle_direction(candles: List[Candle]) -> str:
    if not candles:
        return "UNKNOWN"

    current = candles[-1]

    if current.close > current.open:
        return "BULLISH"

    if current.close < current.open:
        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# SIGNAL GENERATOR
# ============================================================

def generate_signal(
    candles: List[Candle],
    higher_timeframe_candles: Optional[List[Candle]] = None,
) -> SignalResult:

    reasons: List[str] = []
    warnings: List[str] = []

    # --------------------------------------------------------
    # DATA QUALITY FILTER
    # --------------------------------------------------------

    if len(candles) < 60:
        return SignalResult(
            decision="WAIT",
            confidence=0,
            score=0.0,
            trend="UNKNOWN",
            momentum="UNKNOWN",
            structure="UNKNOWN",
            liquidity="UNKNOWN",
            breakout="UNKNOWN",
            reasons=["Not enough market data."],
            warnings=["Need at least 60 candles before generating a signal."],
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
    candle_direction = recent_candle_direction(candles)

    closes = [c.close for c in candles]

    current_price = closes[-1]

    current_rsi = rsi(closes)
    ema20_value = ema(closes, 20)
    ema50_value = ema(closes, 50)
    atr_value = atr(candles)

    score = 0.0

    # ========================================================
    # TREND
    # ========================================================

    if trend == "BULLISH":
        score += 2.0
        reasons.append("Price is above aligned EMA trend.")

    elif trend == "BEARISH":
        score -= 2.0
        reasons.append("Price is below aligned EMA trend.")

    else:
        warnings.append("Primary trend is not clearly aligned.")

    # ========================================================
    # STRUCTURE
    # ========================================================

    if structure == "BULLISH_STRUCTURE":
        score += 2.0
        reasons.append("Higher-high / higher-low structure detected.")

    elif structure == "BEARISH_STRUCTURE":
        score -= 2.0
        reasons.append("Lower-high / lower-low structure detected.")

    else:
        warnings.append("Market structure is mixed.")

    # ========================================================
    # MOMENTUM
    # ========================================================

    if momentum == "BUYING":
        score += 1.5
        reasons.append("Momentum supports buyers.")

    elif momentum == "SELLING":
        score -= 1.5
        reasons.append("Momentum supports sellers.")

    else:
        warnings.append("Momentum is mixed.")

    # ========================================================
    # RSI
    # ========================================================

    if current_rsi is not None:

        if 55.0 <= current_rsi <= 70.0:
            score += 1.0
            reasons.append(
                f"RSI supports bullish momentum ({current_rsi:.1f})."
            )

        elif 30.0 <= current_rsi <= 45.0:
            score -= 1.0
            reasons.append(
                f"RSI supports bearish momentum ({current_rsi:.1f})."
            )

        elif current_rsi > 75.0:
            warnings.append(
                f"RSI is very high ({current_rsi:.1f}); chasing BUY is risky."
            )

        elif current_rsi < 25.0:
            warnings.append(
                f"RSI is very low ({current_rsi:.1f}); chasing SELL is risky."
            )

    # ========================================================
    # EMA DISTANCE / EXTENSION FILTER
    # ========================================================

    if atr_value and ema20_value:

        distance_from_ema20 = abs(current_price - ema20_value)

        if distance_from_ema20 > atr_value * 2.0:
            warnings.append(
                "Price is extended far from EMA20; chasing the move is risky."
            )

    # ========================================================
    # LIQUIDITY
    # ========================================================

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

    # ========================================================
    # BREAKOUT
    # ========================================================

    if breakout == "BULLISH_BREAKOUT":
        score += 2.0
        reasons.append("Bullish breakout has participation confirmation.")

    elif breakout == "BEARISH_BREAKDOWN":
        score -= 2.0
        reasons.append("Bearish breakdown has participation confirmation.")

    # ========================================================
    # CANDLE CONFIRMATION
    # ========================================================

    if score > 0 and candle_direction == "BULLISH":
        score += 0.5
        reasons.append("Latest candle confirms bullish direction.")

    elif score < 0 and candle_direction == "BEARISH":
        score -= 0.5
        reasons.append("Latest candle confirms bearish direction.")

    elif abs(score) >= 4 and candle_direction != "NEUTRAL":
        if (
            (score > 0 and candle_direction == "BEARISH")
            or (score < 0 and candle_direction == "BULLISH")
        ):
            warnings.append(
                "Latest candle is moving against the current signal direction."
            )

    # ========================================================
    # HIGHER TIMEFRAME CONFIRMATION
    # ========================================================

    higher_trend = "UNKNOWN"

    if higher_timeframe_candles and len(higher_timeframe_candles) >= 50:

        higher_trend = trend_analysis(higher_timeframe_candles)

        if higher_trend == "BULLISH":
            score += 1.0
            reasons.append("Higher timeframe trend is bullish.")

        elif higher_trend == "BEARISH":
            score -= 1.0
            reasons.append("Higher timeframe trend is bearish.")

        else:
            warnings.append("Higher timeframe trend is not clearly aligned.")

    # ========================================================
    # SUPPORT / RESISTANCE
    # ========================================================

    levels = support_resistance(candles)

    if levels and atr_value:

        resistance = levels["resistance"]
        support = levels["support"]

        distance_to_resistance = abs(resistance - current_price)
        distance_to_support = abs(current_price - support)

        if distance_to_resistance < atr_value * 0.5:
            warnings.append(
                "Price is close to resistance; BUY confirmation is weaker."
            )

        if distance_to_support < atr_value * 0.5:
            warnings.append(
                "Price is close to support; SELL confirmation is weaker."
            )

    # ========================================================
    # CONFLICT DETECTION
    # ========================================================

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

    if higher_trend == "BULLISH":
        bullish_votes += 1
    elif higher_trend == "BEARISH":
        bearish_votes += 1

    # Strong two-sided conflict.
    if bullish_votes >= 2 and bearish_votes >= 2:
        warnings.append(
            "Major market factors are conflicting."
        )

        return SignalResult(
            decision="WAIT",
            confidence=35,
            score=round(score, 2),
            trend=trend,
            momentum=momentum,
            structure=structure,
            liquidity=liquidity,
            breakout=breakout,
            reasons=reasons,
            warnings=warnings,
        )

    # ========================================================
    # MARKET REGIME FILTER
    # ========================================================

    regime_warning = False

    if (
        trend in ("BULLISH", "BEARISH")
        and structure == "MIXED_STRUCTURE"
        and momentum == "WEAK/MIXED"
    ):
        regime_warning = True
        warnings.append(
            "Trend exists, but structure and momentum are not confirming it."
        )

    # ========================================================
    # SCORE / CONFIDENCE
    # ========================================================

    # The theoretical maximum is deliberately larger than the
    # minimum decision threshold. This prevents easy high scores.
    max_score = 13.0

    normalized = min(abs(score) / max_score, 1.0)

    confidence = int(50 + normalized * 45)

    # ========================================================
    # FINAL DECISION
    # ========================================================

    # We intentionally require multiple confirmations.
    if (
        score >= 5.0
        and bullish_votes >= 3
        and bearish_votes == 0
        and not regime_warning
    ):
        decision = "BUY"

    elif (
        score <= -5.0
        and bearish_votes >= 3
        and bullish_votes == 0
        and not regime_warning
    ):
        decision = "SELL"

    else:
        decision = "WAIT"
        confidence = min(confidence, 65)

    # ========================================================
    # FALSE-SIGNAL PROTECTION
    # ========================================================

    if volatility == "HIGH":
        warnings.append(
            "High volatility detected; breakout and reversal risk is elevated."
        )

    elif volatility == "LOW":
        warnings.append(
            "Low volatility detected; directional move may be weak."
        )

    # A sweep by itself should not create an aggressive signal.
    if liquidity in (
        "LOW_LIQUIDITY_SWEEP",
        "HIGH_LIQUIDITY_SWEEP",
    ):
        if breakout == "NO_CONFIRMATION":
            warnings.append(
                "Liquidity sweep detected without breakout confirmation."
            )

            # Reduce confidence because the sweep has not yet
            # received enough follow-through confirmation.
            confidence = min(confidence, 72)

    # Don't allow very high confidence when warnings are significant.
    if len(warnings) >= 3:
        confidence = min(confidence, 60)

    if len(warnings) >= 4:
        decision = "WAIT"
        confidence = min(confidence, 55)

    # Prevent extreme confidence on borderline scores.
    if abs(score) < 6:
        confidence = min(confidence, 70)

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
        warnings=warnings,
    )


# ============================================================
# JSON OUTPUT
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
