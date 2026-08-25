"""
Technical indicators for the Woodies CCI sub-panels: CCI, Stochastic, MACD.

Ported verbatim from the `infra` project so the sub-panels here are
pixel-for-pixel identical to the ones there. **Do not "improve" anything in this
module** — the maths, the default parameters and the colours are a specification,
not a starting point. One default in particular looks like a typo and is not:
Stochastic `%K` smoothing defaults to **6**, not the usual 3.

`woodies_cci` still returns `zlr_long` / `zlr_short`. This project does not draw
the zero-line-reject markers (or the CCI x Turbo crossing markers, whose helper
has been dropped), but the columns stay because they are part of the ported
function's contract and cost nothing to compute.

Like `kpis.py`, this module imports no Streamlit: it takes a bars DataFrame and
returns numbers, so it can be checked from the command line with no browser.

Input frame: one row per bar, ascending by `ts`, with columns
`ts, open, high, low, close, volume`.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Woodies UI palette — confirmed-trend / chop histogram colors.
UP_COLOR = "#26a69a"     # green  — confirmed uptrend
DOWN_COLOR = "#ef5350"   # red    — confirmed downtrend
CHOP_COLOR = "#9e9e9e"   # gray   — no trend confirmed yet


# ---------------------------------------------------------------------------
# Defaults. Exposed as inputs in the UI; these are the values the source
# project ships, and the panels only match while they are unchanged.
# ---------------------------------------------------------------------------
DEFAULTS = {
    "cci_period": 14,        # 2-100
    "turbo_period": 6,       # 2-100
    "trend_bars": 6,         # 2-30
    "stoch_k": 14,           # 1-100
    "stoch_k_smooth": 6,     # 1-50   <-- not 3
    "stoch_d": 3,            # 1-50
    "macd_fast": 12,         # 1-100
    "macd_slow": 26,         # 2-200
    "macd_signal": 9,        # 1-100
}


def compute_cci(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """CCI = (TP - SMA(TP,n)) / (0.015 * MeanAbsDev(TP,n)), TP=(H+L+C)/3."""
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    sma = tp.rolling(period).mean()
    mad = tp.rolling(period).apply(lambda w: np.abs(w - w.mean()).mean(), raw=True)
    return (tp - sma) / (0.015 * mad)


def _trend_state(cci: pd.Series, trend_bars: int):
    """Woodies trend: +1/-1 only after `trend_bars` consecutive bars on one side
    of zero, else 0 (chop). Also returns the running same-side streak count."""
    side = np.sign(cci.to_numpy())
    trend = np.zeros(len(cci), dtype=int)
    count = np.zeros(len(cci), dtype=int)
    run = 0
    prev = 0
    for i, s in enumerate(side):
        if np.isnan(s) or s == 0:
            run, prev = 0, 0
            continue
        run = run + 1 if s == prev else 1
        prev = s
        count[i] = run
        if run >= trend_bars:
            trend[i] = int(s)
    return pd.Series(trend, index=cci.index), pd.Series(count, index=cci.index)


def woodies_cci(df, cci_period=14, turbo_period=6, trend_bars=6, zlr_guard=100.0):
    """Columns: cci, turbo, trend, trend_count, hist_color, zlr_long, zlr_short."""
    cci = compute_cci(df, period=cci_period)
    turbo = compute_cci(df, period=turbo_period)
    trend, count = _trend_state(cci, trend_bars)

    hist_color = pd.Series(CHOP_COLOR, index=df.index)
    hist_color[trend > 0] = UP_COLOR
    hist_color[trend < 0] = DOWN_COLOR

    # Zero-Line Reject: confirmed trend + Turbo crosses back through zero the
    # trend's way, having only dipped shallowly past it (<= zlr_guard).
    t_prev = turbo.shift(1)
    cross_up = (t_prev < 0) & (turbo >= 0) & (t_prev >= -zlr_guard)
    cross_dn = (t_prev > 0) & (turbo <= 0) & (t_prev <= zlr_guard)
    zlr_long = (trend > 0) & cross_up
    zlr_short = (trend < 0) & cross_dn

    return pd.DataFrame({
        "cci": cci, "turbo": turbo, "trend": trend, "trend_count": count,
        "hist_color": hist_color,
        "zlr_long": zlr_long.fillna(False),
        "zlr_short": zlr_short.fillna(False),
    })


def trend_label(trend_value: int) -> str:
    return {1: "Trending ↑", -1: "Trending ↓"}.get(int(trend_value), "Chop")


def stochastic_kd(df, k_period: int = 14, k_smooth: int = 6, d_period: int = 3):
    """raw %K = 100*(close - lowest_low)/(highest_high - lowest_low) over k_period;
    %K = SMA(raw %K, k_smooth); %D = SMA(%K, d_period)."""
    low_n = df["low"].rolling(k_period).min()
    high_n = df["high"].rolling(k_period).max()
    # Avoid /0 on a flat window. Use float NaN (not pd.NA) — pd.NA upcasts to
    # object dtype and a later rolling().mean() then raises.
    rng = (high_n - low_n).replace(0, float("nan"))
    raw_k = 100 * (df["close"] - low_n) / rng
    k = raw_k.rolling(k_smooth).mean()
    d = k.rolling(d_period).mean()
    return k, d


def macd_lines(df, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD = EMA(close,fast) - EMA(close,slow); signal = EMA(MACD,signal);
    hist = MACD - signal. EMAs use adjust=False (conventional MACD recursion)."""
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    return macd, sig, macd - sig


# ---------------------------------------------------------------------------
# Warm-up
# ---------------------------------------------------------------------------
def warmup_bars(cci_period=14, turbo_period=6, trend_bars=6,
                stoch_k=14, stoch_k_smooth=6, stoch_d=3,
                macd_fast=12, macd_slow=26, macd_signal=9) -> int:
    """How many bars of history every indicator needs before it reads true.

    The sub-panels must be computed on bars extending BEFORE the visible window
    and then tail-trimmed, or the lines are blank at the left edge of the chart.
    """
    return max(cci_period + turbo_period + trend_bars,
               stoch_k + stoch_k_smooth + stoch_d,
               macd_slow + macd_signal,
               20) + 5
