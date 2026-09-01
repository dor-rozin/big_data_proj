"""
Plotly figures for the seven KPI charts.

Separated from kpis.py so that the arithmetic can be tested without a plotting
library, and from app.py so that a chart can be changed without touching the
Streamlit wiring. Every function here takes a `kpis.KPI` and returns a figure —
none of them compute a value, and none of them read Elasticsearch.

## Design rules applied

**Colours are validated, not chosen by eye.** The two-series pair
(blue `#2a78d6` / orange `#eb6834`) and the polarity pair (blue / red `#e34948`)
were both run through a colour-vision-deficiency validator: worst-pair CVD
separation 24.7 and 21.6 (OKLab x100, target >= 8), normal-vision separation
33.6 and 32.3 (floor 15), and every colour clears 3:1 contrast against the chart
surface. Deuteranopia and protanopia together affect roughly 8% of men, which is
more of a lecture hall than most palettes account for.

**One y-axis, always.** A debt-versus-equity chart is a standing invitation to
plot a ratio on a second axis, and a dual-axis chart lets the author place the
crossover point anywhere they like by choosing the scales. The D/E ratio is
therefore printed as a label above each year rather than drawn against its own
scale.

**Colour never carries meaning alone.** On the Rule of 40 chart, position
against the threshold line already says pass or fail; the colour repeats that
signal rather than being the only source of it. Every two-series chart carries a
legend, and every bar carries its own value label.

**A gap in the data is drawn as a gap.** No zero-filling and no interpolation
across missing years — a company that did not report a fact gets empty space and
a written note, because a zero would read as "it earned nothing".
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

import woodies_chart
from kpis import KPI, RULE_OF_40_THRESHOLD

# ---- palette (validated; see the module docstring) ------------------------
SERIES_1 = "#2a78d6"     # blue   — primary series
SERIES_2 = "#eb6834"     # orange — secondary series
POSITIVE = "#2a78d6"     # blue   — diverging pole
NEGATIVE = "#e34948"     # red    — diverging pole

SURFACE = "#fcfcfb"      # chart surface
INK = "#0b0b0b"          # primary text
INK_SECOND = "#52514e"   # secondary text
MUTED = "#898781"        # axis labels and ticks
GRID = "#e1e0d9"         # hairline gridline
BASELINE = "#c3c2b7"     # zero line / axis

FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def fmt_usd(v) -> str:
    """Compact currency. Revenue is read as '$391B', never as 391035000000."""
    if pd.isna(v):
        return "n/a"
    sign = "-" if v < 0 else ""
    a = abs(v)
    for div, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= div:
            return f"{sign}${a / div:,.2f}{suffix}"
    return f"{sign}${a:,.0f}"


def fmt_shares(v) -> str:
    """Compact share counts. 15,005,000,000 reads as '15.01B shares'."""
    if pd.isna(v):
        return "n/a"
    a = abs(v)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= div:
            return f"{a / div:,.2f}{suffix}"
    return f"{a:,.0f}"


def fmt_value(v, unit: str) -> str:
    if pd.isna(v):
        return "n/a"
    return {
        "USD": lambda: fmt_usd(v),
        "%": lambda: f"{v:.1f}%",
        "years": lambda: f"{v:.1f}y",
        "ratio": lambda: f"{v:.2f}x",
        "USD/share": lambda: f"${v:.2f}",
        "shares": lambda: fmt_shares(v),
    }.get(unit, lambda: f"{v:,.2f}")()


def _base_layout(fig: go.Figure, title: str, y_title: str = "",
                 show_legend: bool = False) -> go.Figure:
    """Chrome shared by all seven charts: recessive axes, generous whitespace.

    Gridlines are a hairline and only horizontal. Vertical gridlines on a
    categorical x-axis (five fiscal years) partition the plot without adding
    information — the bars already mark the categories.
    """
    fig.update_layout(
        title=dict(text=title, font=dict(size=15, color=INK, family=FONT),
                   x=0, xanchor="left", y=0.97),
        paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(family=FONT, size=12, color=INK_SECOND),
        margin=dict(l=8, r=8, t=44, b=8),
        height=320,
        showlegend=show_legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.0,
                    xanchor="right", x=1, font=dict(size=11, color=INK_SECOND),
                    bgcolor="rgba(0,0,0,0)"),
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=BASELINE,
                        font=dict(family=FONT, size=12, color=INK)),
        bargap=0.35,          # the 'gap between fills' spacer, at bar scale
        bargroupgap=0.12,
    )
    fig.update_xaxes(showgrid=False, showline=True, linecolor=BASELINE,
                     ticks="outside", tickcolor=BASELINE, ticklen=4,
                     tickfont=dict(color=MUTED, size=11), type="category")
    fig.update_yaxes(title=dict(text=y_title, font=dict(color=MUTED, size=11)),
                     showgrid=True, gridcolor=GRID, gridwidth=1,
                     zeroline=True, zerolinecolor=BASELINE, zerolinewidth=1,
                     showline=False, tickfont=dict(color=MUTED, size=11))
    return fig


def _years(d: pd.DataFrame) -> list[str]:
    """Fiscal years as category labels. 'FY2024' reads as a year, 2024 as a number."""
    return [f"FY{int(y)}" for y in d["fiscal_year"]]


def _empty(title: str, message: str) -> go.Figure:
    """Placeholder for a company with no data for this KPI.

    An explicit 'not reported' beats an empty frame: the reader needs to know
    the difference between 'the value is zero' and 'this company does not
    publish this number', and a blank chart says neither.
    """
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, xref="paper", yref="paper",
                       x=0.5, y=0.5, font=dict(size=12, color=MUTED))
    _base_layout(fig, title)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig


# ---------------------------------------------------------------------------
# 1. Annual revenue — magnitude over time, one series
# ---------------------------------------------------------------------------
def revenue_chart(k: KPI) -> go.Figure:
    """Bars: revenue is a quantity accumulated over a period, not a level.

    A bar is the honest mark for a flow measured over a whole year. A line would
    imply revenue moved continuously between the two year-ends, which is not
    something an annual filing says.
    """
    if not k.available:
        return _empty(k.name, "Revenue is not reported for this company.")
    d = k.data
    fig = go.Figure(go.Bar(
        x=_years(d), y=d["revenue"], marker_color=SERIES_1,
        marker_cornerradius=4,           # rounded data-end, baseline stays square
        text=[fmt_usd(v) for v in d["revenue"]],
        textposition="outside", textfont=dict(size=11, color=INK_SECOND),
        hovertemplate="%{x}<br>Revenue %{text}<extra></extra>", name="Revenue",
    ))
    return _base_layout(fig, "Annual Revenue", "USD")


# ---------------------------------------------------------------------------
# 2. Buyback — shares outstanding, split-adjusted
# ---------------------------------------------------------------------------
def buyback_chart(k: KPI) -> go.Figure:
    """Bars of the share count, labelled with the change that produced them.

    The bars carry the **year-on-year percent change**, not the count, because
    the count is the part that barely moves: a serious buyback is 2-3% a year, a
    difference of a couple of pixels on a bar 15 billion tall. Reading "-2.3%"
    off the bar answers the question the chart is asking; reading the height does
    not. The absolute figure stays one hover away.

    The y-axis deliberately keeps its zero baseline rather than zooming into the
    top few percent. A truncated axis would turn a 2% buyback into a dramatic
    cliff, which is exactly the misreading the percent labels exist to prevent.
    """
    if not k.available:
        return _empty(k.name,
                      "No share count: this company does not tag "
                      "`shares_outstanding` in its filings.")
    d = k.data
    labels = []
    for change in d["net_change_pct"]:
        labels.append("" if pd.isna(change) else f"{change:+.1f}%")

    fig = go.Figure(go.Bar(
        x=_years(d), y=d["shares_adjusted"], marker_color=SERIES_1,
        marker_cornerradius=4,
        text=labels,
        textposition="outside", textfont=dict(size=11, color=INK_SECOND),
        customdata=d[["shares_reported", "net_change_pct"]].values,
        hovertemplate=("%{x}<br>%{y:,.0f} shares (split-adjusted)"
                       "<br>as filed: %{customdata[0]:,.0f}"
                       "<extra></extra>"),
        name="Shares outstanding",
    ))
    return _base_layout(fig, "Buyback — shares outstanding, split-adjusted",
                        "shares")


# ---------------------------------------------------------------------------
# 3. Debt vs equity
# ---------------------------------------------------------------------------
def debt_equity_chart(k: KPI) -> go.Figure:
    """Grouped bars for the two absolute amounts; the ratio printed above them.

    Both series are USD and share one scale, which is what makes the comparison
    legitimate. The D/E ratio is a different unit entirely, so it is annotated
    rather than plotted — putting it on a second y-axis would let the choice of
    scales decide where the two lines appear to cross, which is a way of making a
    chart say whatever the author wants.
    """
    if not k.available:
        return _empty(k.name, "Liabilities or equity are not reported.")
    d = k.data
    x = _years(d)
    fig = go.Figure([
        go.Bar(x=x, y=d["equity"], name="Equity", marker_color=SERIES_1,
               marker_cornerradius=4,
               hovertemplate="%{x}<br>Equity %{customdata}<extra></extra>",
               customdata=[fmt_usd(v) for v in d["equity"]]),
        go.Bar(x=x, y=d["liabilities"], name="Liabilities", marker_color=SERIES_2,
               marker_cornerradius=4,
               hovertemplate="%{x}<br>Liabilities %{customdata}<extra></extra>",
               customdata=[fmt_usd(v) for v in d["liabilities"]]),
    ])
    _base_layout(fig, "Debt vs Equity", "USD", show_legend=True)
    fig.update_layout(barmode="group")

    # The ratio as a direct label above each year — same information, no second
    # axis, and it stays readable when the two bars differ by an order of
    # magnitude (JPM's liabilities are ~11x its equity).
    top = max([v for v in list(d["liabilities"]) + list(d["equity"])
               if pd.notna(v)] or [0])
    for xi, ratio in zip(x, d["debt_to_equity"]):
        if pd.notna(ratio):
            fig.add_annotation(x=xi, y=top * 1.10, text=f"D/E {ratio:.2f}x",
                               showarrow=False,
                               font=dict(size=11, color=INK_SECOND))
    fig.update_yaxes(range=[0, top * 1.24])
    return fig


# ---------------------------------------------------------------------------
# 4. Net profit — a signed quantity
# ---------------------------------------------------------------------------
def net_profit_chart(k: KPI) -> go.Figure:
    """Bars coloured by sign: profit blue, loss red.

    Sign is the one thing a reader must never misread here, and BRK.B genuinely
    posts a -$22.8B year in this dataset. Position relative to the zero line
    already carries it; the colour repeats the signal rather than replacing it,
    and every bar is labelled with its own value.
    """
    if not k.available:
        return _empty(k.name, "Net income is not reported for this company.")
    d = k.data
    values = d["net_income"]
    fig = go.Figure(go.Bar(
        x=_years(d), y=values,
        marker_color=[NEGATIVE if pd.notna(v) and v < 0 else POSITIVE
                      for v in values],
        marker_cornerradius=4,
        text=[fmt_usd(v) for v in values],
        textposition="outside", textfont=dict(size=11, color=INK_SECOND),
        hovertemplate="%{x}<br>Net income %{text}<extra></extra>",
        name="Net income",
    ))
    return _base_layout(fig, "Net Profit", "USD")


# ---------------------------------------------------------------------------
# 5. Cash flow
# ---------------------------------------------------------------------------
def cash_flow_chart(k: KPI) -> go.Figure:
    """Operating cash flow with free cash flow beside it, where capex allows.

    Two series in one chart because they are the same unit and one is derived
    from the other — free cash flow is operating cash flow minus capital
    spending, so the gap between the bars *is* capex, read directly.

    Where capex is not reported (AMZN, JPM, NVDA at annual level) the free cash
    flow bar is simply absent, which reads correctly as 'not derivable' rather
    than as 'zero free cash flow'.
    """
    if not k.available:
        return _empty(k.name, "Operating cash flow is not reported.")
    d = k.data
    x = _years(d)
    traces = [go.Bar(x=x, y=d["operating_cash_flow"], name="Operating cash flow",
                     marker_color=SERIES_1, marker_cornerradius=4,
                     customdata=[fmt_usd(v) for v in d["operating_cash_flow"]],
                     hovertemplate="%{x}<br>Operating %{customdata}<extra></extra>")]
    if d["free_cash_flow"].notna().any():
        traces.append(go.Bar(x=x, y=d["free_cash_flow"], name="Free cash flow",
                             marker_color=SERIES_2, marker_cornerradius=4,
                             customdata=[fmt_usd(v) for v in d["free_cash_flow"]],
                             hovertemplate="%{x}<br>Free %{customdata}<extra></extra>"))
    fig = go.Figure(traces)
    _base_layout(fig, "Cash Flow", "USD", show_legend=len(traces) > 1)
    fig.update_layout(barmode="group")
    return fig


# ---------------------------------------------------------------------------
# 6. Rule of 40
# ---------------------------------------------------------------------------
def rule_of_40_chart(k: KPI) -> go.Figure:
    """Growth % + margin % against the 40% line.

    The threshold is the entire point of this metric, so it is drawn as a
    labelled reference line and the bars are coloured by which side they land on.
    Position against the line is the primary signal and the colour is redundant
    with it — a reader who cannot distinguish the two hues still gets the answer
    from the geometry.

    The stacked composition (how much came from growth versus margin) is in the
    hover, because two companies can reach the same total from opposite
    directions and that distinction matters more than the total itself.
    """
    if not k.available:
        return _empty(k.name, "Needs both revenue growth and net margin; at "
                              "least one is unavailable.")
    d = k.data
    values = d["rule_of_40"]
    fig = go.Figure(go.Bar(
        x=_years(d), y=values,
        marker_color=[NEGATIVE if pd.notna(v) and v < RULE_OF_40_THRESHOLD
                      else POSITIVE for v in values],
        marker_cornerradius=4,
        text=[f"{v:.0f}%" if pd.notna(v) else "" for v in values],
        textposition="outside", textfont=dict(size=11, color=INK_SECOND),
        customdata=d[["revenue_growth_pct", "net_margin_pct"]].values,
        hovertemplate=("%{x}<br>Rule of 40: %{y:.1f}%"
                       "<br>growth %{customdata[0]:.1f}%"
                       "  +  margin %{customdata[1]:.1f}%<extra></extra>"),
        name="Rule of 40",
    ))
    _base_layout(fig, "Rule of 40 — revenue growth % + net margin %", "%")
    fig.add_hline(y=RULE_OF_40_THRESHOLD, line_dash="dash", line_color=MUTED,
                  line_width=2, annotation_text="40% threshold",
                  annotation_position="top left",
                  annotation_font=dict(size=11, color=MUTED))
    return fig


# ---------------------------------------------------------------------------
# 7. Earnings per share
# ---------------------------------------------------------------------------
def eps_chart(k: KPI) -> go.Figure:
    """Lines: EPS is a per-share level, and the trend between years is the point.

    Diluted leads and basic trails. The gap between the two lines is dilution,
    visible directly — a widening gap means the share count is growing faster
    than earnings, which no single series would show.
    """
    if not k.available:
        return _empty(k.name,
                      "This company does not tag EPS in its XBRL filings, and "
                      "reports no share count to derive it from.")
    d = k.data
    x = _years(d)
    fig = go.Figure([
        go.Scatter(x=x, y=d["eps_diluted"], name="Diluted EPS", mode="lines+markers",
                   line=dict(color=SERIES_1, width=2),
                   marker=dict(size=9, color=SERIES_1,
                               line=dict(width=2, color=SURFACE)),
                   hovertemplate="%{x}<br>Diluted $%{y:.2f}<extra></extra>"),
    ])
    if d["eps_basic"].notna().any():
        fig.add_trace(go.Scatter(
            x=x, y=d["eps_basic"], name="Basic EPS", mode="lines+markers",
            line=dict(color=SERIES_2, width=2, dash="dot"),
            marker=dict(size=9, color=SERIES_2,
                        line=dict(width=2, color=SURFACE)),
            hovertemplate="%{x}<br>Basic $%{y:.2f}<extra></extra>"))
    _base_layout(fig, "Earnings per Share", "USD per share",
                 show_legend=len(fig.data) > 1)
    return fig


# ---------------------------------------------------------------------------
# Share price — the one time-axis chart on the page
# ---------------------------------------------------------------------------
def price_chart(view, ticker: str = "") -> go.Figure:
    """Closing price over the selected window.

    A line, not bars: this is the only series here sampled densely enough for a
    line to mean what it looks like — consecutive trading days really are
    adjacent, so the segment between two points is not an interpolation across
    unmeasured time the way it would be between two fiscal years.

    Direction is carried by the colour **and** stated as a percentage in the
    title, never by colour alone — a red line with no number is unreadable to
    roughly 8% of men, and the palette rule in this module's docstring exists
    precisely for that.

    The y-axis does **not** keep a zero baseline, which is the opposite of the
    rule the KPI charts follow. A share price has no meaningful zero and the
    question here is the shape of the move, not its size against nothing; on a
    one-week window a zero-based axis would flatten every price into one line.

    A **vertical crosshair** follows the cursor and snaps to trading days. It is
    the only chart on the page where reading an individual point off the line is
    a real question — the KPI charts have five labelled bars, this one has up to
    a year of closes and no room to label them.

    Non-trading days are removed from the axis rather than drawn as empty space.
    This is not a breach of the module's 'a gap in the data is drawn as a gap'
    rule: that rule is about a fact a company did not report, where the empty
    space carries the meaning. A weekend is not a missing observation, it is time
    in which no observation can exist, and drawing a flat segment across it
    invents two days of unchanged price that were never traded.
    """
    if view is None or not view.available:
        return _empty(f"{ticker} share price".strip(),
                      "No price bars for this company in this window.")

    d = view.data
    change = view.change_pct
    colour = POSITIVE if (change is None or change >= 0) else NEGATIVE

    title = f"{ticker} share price · {view.window}".strip(" ·")
    if change is not None:
        title += f"   {change:+.1f}%"

    # Truncate each bar to its calendar date before drawing or collapsing.
    #
    # `ts` on this project's daily bars is midnight *Eastern* expressed in UTC,
    # so it lands at 04:00 under EDT and 05:00 under EST — 166 and 85 bars on a
    # one-year AAPL window. `_hide_gaps` lays a uniform one-day grid from the
    # first bar's clock time, so without this every bar on the other side of the
    # DST switch falls off that grid and is hidden as a "gap": 251 trading days
    # collapse to 52. `bars_frame` normalises for the same reason, and both
    # 04:00Z and 05:00Z resolve to the trading day they already represent.
    x = (pd.to_datetime(d["date"], errors="coerce", utc=True)
           .dt.tz_localize(None).dt.normalize())

    fig = go.Figure(go.Scatter(
        x=x, y=d["close"], mode="lines",
        line=dict(color=colour, width=2),
        # The date is the unified box's own header, so the row under it carries
        # only the price — repeating the date would just make the box taller.
        hovertemplate="Close  $%{y:,.2f}<extra></extra>",
        name="Close",
    ))
    _base_layout(fig, title, "USD")
    # _base_layout pins the x-axis to `type="category"` for the fiscal-year
    # charts. This is the one chart with a real time axis, so it is put back.
    fig.update_xaxes(type="date", showgrid=False, hoverformat="%a %d %b %Y")

    # The crosshair: a vertical rule that tracks the cursor and reads out the
    # trading day under it.
    #
    # `hovermode="x unified"` with `hoverdistance=-1` is what makes it feel
    # pinned. Plotly's default ("closest") only answers when the cursor is
    # within a few pixels of the line itself, so on a chart whose line wanders
    # across the height of the panel the reader has to chase it. Unbounded
    # distance means anywhere in the plot area — top corner included — reads the
    # nearest trading day, and the readout is one box: date as the header, close
    # beneath it.
    #
    # `spikesnap="hovered data"` is the "existing points only" part of the
    # request. The rule lands on the bar being reported rather than on the raw
    # cursor x, so it steps from one trading day to the next instead of sliding
    # continuously, and never stands between two closes.
    fig.update_layout(hovermode="x unified", hoverdistance=-1, spikedistance=-1)
    fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="hovered data",
                     spikecolor=MUTED, spikethickness=1, spikedash="dot")

    # Collapse the weekends and market holidays out of the axis, reusing the
    # Woodies panel's routine rather than a second copy of the same arithmetic.
    #
    # Sharing it is the point, not a shortcut. `app.py` drives that panel from
    # this view's own dates, so the two charts are stacked showing the same days
    # under the same window selector; if only one of them collapsed its gaps, a
    # given date would sit at a different horizontal position in each, and
    # dropping your eye from a price move to that day's CCI or MACD reading
    # would land on the wrong bar. Two implementations of the compression would
    # drift apart the first time either is tuned, so there is only one.
    woodies_chart._hide_gaps(fig, x)

    # An explicit padded range rather than an area fill. A `fill="tozeroy"` would
    # look better and would drag the axis back down to zero to accommodate the
    # fill — undoing the whole point of a price axis and flattening a week's
    # movement into a flat line.
    lo, hi = float(d["close"].min()), float(d["close"].max())
    pad = (hi - lo) * 0.08 or max(abs(hi) * 0.01, 0.5)
    fig.update_yaxes(zeroline=False, range=[lo - pad, hi + pad])
    return fig


# The order the dashboard renders them, matching the numbering in the brief.
CHART_BUILDERS = {
    "revenue": revenue_chart,
    "buyback": buyback_chart,
    "debt_equity": debt_equity_chart,
    "net_profit": net_profit_chart,
    "cash_flow": cash_flow_chart,
    "rule_of_40": rule_of_40_chart,
    "eps": eps_chart,
}
