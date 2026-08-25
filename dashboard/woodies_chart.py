"""
The four-row Woodies chart: candles, CCI, Stochastic, MACD.

Ported verbatim from the `infra` project. Row heights, colours, guide lines and
the axis-finishing sequence are a specification — changing any of them makes
this stop matching the original, which is the whole point of the module.

The axis work in `finish_axes` is the part that looks droppable and is not.
Collapsing every trace onto `x4` is what makes the cursor spike one continuous
vertical line through all four panels; the range pin and the xref re-point are
its mandatory companions. Skip either and the guide lines vanish or the chart
jumps to 1970.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from indicators import macd_lines, stochastic_kd, warmup_bars, woodies_cci


OHLC = ("open", "high", "low", "close")


def bars_frame(prices: pd.DataFrame):
    """Normalise the Elasticsearch price documents into the spec's bars frame.

    The spec's frame is `ts, open, high, low, close, volume`, ascending by `ts`.
    Elasticsearch carries both `ts` (the raw contract field, an ISO string) and
    `date` (what the Spark transform adds); either is accepted, matching how
    `kpis.price_view` reads the same documents.

    Returns None when the documents carry no OHLC — the candle row and the CCI
    both need high/low, so there is nothing to draw rather than something wrong.
    """
    if prices is None or prices.empty:
        return None
    df = prices.copy()
    if "ts" not in df.columns and "date" in df.columns:
        df["ts"] = df["date"]
    if "ts" not in df.columns or not set(OHLC).issubset(df.columns):
        return None

    # Normalised to midnight, and that is load-bearing for `_hide_gaps`.
    #
    # `ts` on this project's daily bars is midnight *Eastern* expressed in UTC,
    # so it lands at 04:00 under EDT and 05:00 under EST — 331 vs 169 bars on the
    # current AAPL snapshot. `_hide_gaps` lays a uniform one-day grid from the
    # first bar's clock time and hides every slot not on it, so half the year
    # falls off the grid and gets hidden as a "gap": 126 visible bars rendered as
    # 24. The indicator maths is unaffected (it never reads `ts`), which is what
    # makes the symptom so easy to misread as a plotting bug.
    #
    # Truncating to the calendar date is correct for daily bars either way: both
    # 04:00Z and 05:00Z resolve to the same trading day they already represent.
    df["ts"] = (pd.to_datetime(df["ts"], errors="coerce", utc=True)
                  .dt.tz_localize(None).dt.normalize())
    for c in OHLC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    else:
        df["volume"] = float("nan")

    keep = ["ts", *OHLC, "volume"]
    df = (df.dropna(subset=["ts", "close"])
            .sort_values("ts")
            .drop_duplicates(subset=["ts"], keep="last")
            .loc[:, keep]
            .reset_index(drop=True))
    return None if df.empty else df


def prepare_frames(full: pd.DataFrame, visible_dates, **params):
    """Split a full price history into (ind_df, df) per the warm-up rule.

    `ind_df` runs from `warmup_bars * 2` rows before the visible window through
    its end; `df` is the visible tail of it. Everything is computed on `ind_df`
    and tail-trimmed to `len(df)`, so the leftmost visible candle already has a
    warm CCI(14), %K(14)+6+3 and MACD(26,9).

    Returns (ind_df, df, short_by) where `short_by` counts warm-up bars the
    snapshot could not supply — reported rather than hidden, because too little
    warm-up shows up as a blank left edge and should be explained, not guessed at.
    """
    need = warmup_bars(**params) * 2
    vis = pd.to_datetime(pd.Series(list(visible_dates)), errors="coerce").dropna()
    lo, hi = vis.min(), vis.max()

    ts = full["ts"]
    first_visible = int(ts.searchsorted(lo, side="left"))
    last_visible = int(ts.searchsorted(hi, side="right"))
    n_vis = max(1, last_visible - first_visible)

    start = max(0, first_visible - need)
    ind_df = full.iloc[start:last_visible].reset_index(drop=True)
    df = ind_df.tail(n_vis).reset_index(drop=True)
    return ind_df, df, max(0, need - (first_visible - start))


def build_figure(ind_df: pd.DataFrame, df: pd.DataFrame, symbol: str,
                 cci_period=14, turbo_period=6, trend_bars=6,
                 stoch_k=14, stoch_k_smooth=6, stoch_d=3,
                 macd_fast=12, macd_slow=26, macd_signal=9):
    """The four-row figure. `ind_df` carries the warm-up, `df` is what is drawn."""
    n_vis = len(df)
    wdf = woodies_cci(ind_df, cci_period, turbo_period,
                      trend_bars).tail(n_vis).reset_index(drop=True)
    k_line, d_line = stochastic_kd(ind_df, stoch_k, stoch_k_smooth, stoch_d)
    k_line = k_line.tail(n_vis).reset_index(drop=True)
    d_line = d_line.tail(n_vis).reset_index(drop=True)
    macd_line, signal_line, macd_hist = macd_lines(ind_df, macd_fast, macd_slow,
                                                   macd_signal)
    macd_line = macd_line.tail(n_vis).reset_index(drop=True)
    signal_line = signal_line.tail(n_vis).reset_index(drop=True)
    macd_hist = macd_hist.tail(n_vis).reset_index(drop=True)

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.46, 0.18, 0.18, 0.18], vertical_spacing=0.025,
    )

    # Row 1 — candles
    fig.add_trace(
        go.Candlestick(x=df["ts"], open=df["open"], high=df["high"],
                       low=df["low"], close=df["close"], name=symbol),
        row=1, col=1,
    )

    # Row 2 — Woodies CCI: histogram = CCI value, colored by confirmed trend state
    fig.add_trace(
        go.Bar(x=df["ts"], y=wdf["cci"], name="CCI histogram",
               marker_color=wdf["hist_color"], marker_line_width=0,
               opacity=0.55, hoverinfo="skip", showlegend=False),
        row=2, col=1,
    )
    # Turbo (fast, yellow) + CCI (slow, black)
    fig.add_trace(
        go.Scatter(x=df["ts"], y=wdf["turbo"], name=f"Turbo ({int(turbo_period)})",
                   line=dict(color="#b59f00", width=1.5)),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["ts"], y=wdf["cci"], name=f"CCI ({int(cci_period)})",
                   line=dict(color="#111111", width=1.5)),
        row=2, col=1,
    )
    # No CCI x Turbo crossing markers and no zero-line-reject triangles: the two
    # lines and the trend-coloured histogram carry the signal on their own, and
    # the markers crowded the panel. `woodies_cci` still returns `zlr_long` /
    # `zlr_short` — that function is ported verbatim and its columns are part of
    # its contract — they are simply not drawn.

    # Reference bands: +/-100 and +/-200 dotted green, zero line solid gray
    for lvl in (200, 100, -100, -200):
        fig.add_hline(y=lvl, line=dict(color="#3cb371", width=1, dash="dot"),
                      row=2, col=1)
    fig.add_hline(y=0, line=dict(color="#888888", width=1), row=2, col=1)

    # Row 3 — Stochastic: %K light blue, %D dark red, 80/20 dotted gray guides
    fig.add_trace(
        go.Scatter(x=df["ts"], y=k_line,
                   name=f"%K ({int(stoch_k)},{int(stoch_k_smooth)})",
                   line=dict(color="#4da6ff", width=1.5)),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["ts"], y=d_line, name=f"%D ({int(stoch_d)})",
                   line=dict(color="#8b0000", width=1.5)),
        row=3, col=1,
    )
    for lvl in (80, 20):
        fig.add_hline(y=lvl, line=dict(color="#888888", width=1, dash="dot"),
                      row=3, col=1)

    # Row 4 — MACD: dark-gray histogram, blue MACD line, red signal line
    fig.add_trace(
        go.Bar(x=df["ts"], y=macd_hist, name="MACD histogram",
               marker_color="#555555", marker_line_width=0, opacity=0.7,
               hoverinfo="skip", showlegend=False),
        row=4, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["ts"], y=macd_line,
                   name=f"MACD ({int(macd_fast)},{int(macd_slow)})",
                   line=dict(color="#1f77ff", width=1.5)),
        row=4, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["ts"], y=signal_line, name=f"Signal ({int(macd_signal)})",
                   line=dict(color="#d62728", width=1.5)),
        row=4, col=1,
    )
    fig.add_hline(y=0, line=dict(color="#888888", width=1), row=4, col=1)

    fig.update_layout(
        height=1050, showlegend=True, bargap=0.15,
        xaxis4_title="Time", xaxis_rangeslider_visible=False,
        hovermode="x",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    # Cursor spike — one narrow black vertical line following the cursor,
    # spanning every panel that shares the x-axis.
    fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor",
                     spikecolor="#000000", spikethickness=1, spikedash="solid")
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="CCI", row=2, col=1)
    fig.update_yaxes(title_text="Stoch", range=[0, 100], row=3, col=1)
    fig.update_yaxes(title_text="MACD", row=4, col=1)

    finish_axes(fig, df)
    return fig, wdf


# ---------------------------------------------------------------------------
# Axis finishing — runs LAST, after every trace is added.
# ---------------------------------------------------------------------------
def _hide_gaps(fig, ts):
    """Collapse every empty slot on the time x-axis. Infers the bar interval from
    the modal spacing, lays a regular grid over the span, and feeds every empty
    grid slot to rangebreaks (dvalue = one interval). Present bars are excluded,
    so a real bar is never hidden. Applies to all x-axes → rows stay aligned."""
    ts = pd.to_datetime(pd.Series(ts)).dropna().sort_values()
    if len(ts) < 3:
        return
    interval = ts.diff().dropna().mode()
    if interval.empty or interval.iloc[0] <= pd.Timedelta(0):
        return
    interval = interval.iloc[0]
    full = pd.date_range(ts.iloc[0], ts.iloc[-1], freq=interval)
    missing = full.difference(pd.DatetimeIndex(ts.unique()))
    if len(missing) == 0:
        return
    dvalue = interval / pd.Timedelta(milliseconds=1)
    fig.update_xaxes(rangebreaks=[dict(values=missing, dvalue=dvalue)])


def finish_axes(fig, df):
    """(a) hide non-trading gaps, (b) collapse onto x4 + pin the range,
    (c) re-point domain-referenced shapes/annotations to x4.

    (b) and (c) are a matched pair. The collapse is what makes the cursor spike
    one continuous line; it also leaves x1/x2/x3 without data, so anything
    referencing an empty axis's domain — the CCI ±100/±200 bands, the Stoch
    80/20 guides — stops rendering until it is re-pointed. Doing (b) without (c)
    loses the guide lines; doing (b) without the range pin sends the chart to
    1970.
    """
    _hide_gaps(fig, df["ts"])

    fig.update_traces(xaxis="x4")
    x_lo, x_hi = df["ts"].min(), df["ts"].max()
    fig.update_xaxes(rangeslider_visible=False, range=[x_lo, x_hi])

    for _s in fig.layout.shapes:
        if isinstance(_s.xref, str) and _s.xref.endswith(" domain") and _s.xref != "x4 domain":
            _s.xref = "x4 domain"
    for _a in fig.layout.annotations:
        if isinstance(_a.xref, str) and _a.xref.endswith(" domain") and _a.xref != "x4 domain":
            _a.xref = "x4 domain"
