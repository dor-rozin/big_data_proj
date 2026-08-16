"""
KPI computation for the dashboard — pure pandas. No Elasticsearch, no Streamlit.

Kept separate from the ES access layer (es_client.py) and the chart layer
(charts.py) on purpose: every number the dashboard displays is produced here, so
the arithmetic can be verified offline against the parquet snapshot without a
running stack. `verify_kpis.py` does exactly that.

Three rules run through all of it, and all three come from how the pipeline
already behaves rather than from preference:

  * **A null fact is data, not a failure.** Coverage across the snapshot runs
    70-100% per fact: banks (JPM) report no gross profit and no classified
    balance sheet, BRK.B does not tag EPS in XBRL, Amazon does not tag total
    Liabilities. Every function below reports *which* inputs were missing
    instead of filling a zero, and the chart layer renders that gap explicitly.
    A zero here would read as "the company earned nothing", which is a different
    and false claim.

  * **A missing fact is an ABSENT KEY in Elasticsearch, not a null.**
    `es_writer._clean()` (es_writer.py:162) drops null fields from the document
    entirely. Rebuilding the hits into a DataFrame turns the absent key into
    NaN, which is what these functions expect — hence `.get(col)` style access
    and `pd.to_numeric(..., errors="coerce")` throughout.

  * **A ratio with a non-positive denominator is undefined, not negative.**
    Zero or negative equity would turn debt/equity into a negative ratio, which
    is arithmetically fine and financially meaningless. Those points are dropped
    and reported as a reason, never plotted.

Everything is derived from whatever arrived in the index — no ticker list, no
date range and no fiscal-year list is hardcoded, matching the rule the Spark
transforms already follow.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


# The annual filing. `fiscal_period` is a required, non-nullable enum on the
# frozen contract (schemas/sec.filings.v1.schema.json), so this is a safe filter.
ANNUAL = "FY"


@dataclass
class KPI:
    """One chart's worth of computed numbers, plus why anything is missing.

    `definition` and `source_fields` are surfaced in the UI beside every chart.
    That is a deliberate requirement of this project: a number on a dashboard
    that you cannot trace back to a named field in a named index is a number
    nobody can defend in a review.
    """
    name: str
    definition: str            # the formula, in words, shown under the chart
    source_fields: list[str]   # exact ES field names the formula consumed
    data: pd.DataFrame         # tidy: fiscal_year + the value column(s)
    value_col: str             # which column carries the plotted value
    unit: str = ""             # "USD" | "%" | "years" | "ratio"
    notes: list[str] = field(default_factory=list)   # gaps, in plain language

    @property
    def available(self) -> bool:
        """True when there is at least one plottable point."""
        return (not self.data.empty
                and self.data[self.value_col].notna().any())


# ---------------------------------------------------------------------------
# Shaping
# ---------------------------------------------------------------------------
def annual_frame(filings: pd.DataFrame, years: int = 5) -> pd.DataFrame:
    """Reduce a filings DataFrame to the last `years` annual (FY) filings.

    Fiscal year is labelled from `period_end`, not from `filed_date`: a 10-K is
    filed weeks to months after the year it reports on, so filing date would
    shift roughly half the universe into the wrong bucket. It is also not read
    from any XBRL "fiscal year" tag, because the contract does not carry one.

    Labelling by `period_end.year` matches each issuer's own convention closely:
    Apple's FY2023 ends 2023-09-30, NVDA's FY2025 ends 2025-01-26. Both land on
    the year the issuer itself calls it.
    """
    if filings is None or filings.empty:
        return pd.DataFrame()

    df = filings.copy()
    if "fiscal_period" not in df.columns or "period_end" not in df.columns:
        return pd.DataFrame()

    df = df[df["fiscal_period"] == ANNUAL].copy()
    if df.empty:
        return df

    df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")
    df = df.dropna(subset=["period_end"])
    df["fiscal_year"] = df["period_end"].dt.year

    # The pipeline already deduplicates restatements by (cik, fiscal_period,
    # period_end) keeping the latest filed_date (transforms.py:113-118). This
    # second guard covers the case where one fiscal YEAR label collides across
    # two period_end dates — a 52/53-week issuer changing its year end.
    df = (df.sort_values(["fiscal_year", "period_end"])
            .drop_duplicates(subset=["fiscal_year"], keep="last"))

    return df.sort_values("fiscal_year").tail(years).reset_index(drop=True)


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    """Fetch a column as float, tolerating the column being absent entirely.

    Absent is the normal case here, not an edge case: Elasticsearch documents
    omit null facts, so a ticker that never reports `capex` yields a DataFrame
    with no `capex` column at all rather than a column of NaN.
    """
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _missing_note(fy: pd.DataFrame, series: pd.Series, field_name: str) -> str | None:
    """Describe which fiscal years lack a given input, for display under a chart."""
    gaps = fy.loc[series.isna(), "fiscal_year"].tolist()
    if not gaps:
        return None
    if len(gaps) == len(fy):
        return f"`{field_name}` is not reported at all for this company."
    return (f"`{field_name}` is missing for "
            f"{', '.join('FY' + str(g) for g in gaps)}.")


# ---------------------------------------------------------------------------
# 1. Annual revenue
# ---------------------------------------------------------------------------
def revenue(fy: pd.DataFrame) -> KPI:
    """Total revenue per fiscal year, straight from the filing. No derivation."""
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})
    out["revenue"] = _num(fy, "revenue")

    notes = []
    if (note := _missing_note(fy, out["revenue"], "revenue")):
        notes.append(note)

    return KPI(
        name="Annual Revenue",
        definition="revenue, as reported on the annual (FY) filing",
        source_fields=["stock_filings.revenue"],
        data=out, value_col="revenue", unit="USD", notes=notes,
    )


# ---------------------------------------------------------------------------
# 2. Buyback  —  shares outstanding, split-adjusted
# ---------------------------------------------------------------------------
# Common split ratios, used to snap a measured jump to the ratio that caused it.
#
# Snapping matters more than it looks. AVGO's count jumps by a measured 11.32x
# across its 10:1 split. Dividing the earlier years by 11.32 would silently
# deflate them by 13% and hide the fact that AVGO genuinely issued ~13% more
# shares that year (the VMware acquisition). Snapping to exactly 10 leaves that
# 13% standing where it belongs: as real issuance, on the chart, in the year it
# happened. The split is the artefact; what is left over is the answer.
# Kept to ratios that issuers actually declare. Padding this list is not free:
# with 12 in it, AVGO's measured 11.32 snapped to 12 rather than to the 10:1 it
# really was, inflating every earlier year by 20% and turning that year's genuine
# ~13% issuance into a fake 5.7% buyback. A near-miss against a plausible ratio
# is better than an exact hit against one nobody declares.
SPLIT_RATIOS = (2, 3, 4, 5, 6, 7, 8, 10, 15, 20)

# A jump has to be this large before it is treated as a split at all. Real
# buybacks and issuance in this dataset move single-digit percents; the smallest
# true split here is 3:1. Anything between is left alone rather than guessed at.
SPLIT_MIN_RATIO = 1.5

# How far a measured jump may sit from a clean ratio and still be attributed to
# it. AVGO's 11.32 against 10 is the worst real case at 13%.
SPLIT_TOLERANCE = 0.25


def _snap_split_ratio(observed: float) -> float | None:
    """The clean split ratio that best explains `observed`, or None.

    Handles reverse splits too: a 1:10 reverse split shows up as 0.1, which is
    the reciprocal of a 10 in the same table.
    """
    if observed <= 0:
        return None
    forward = observed >= 1
    probe = observed if forward else 1 / observed
    best = min(SPLIT_RATIOS, key=lambda r: abs(probe / r - 1))
    if abs(probe / best - 1) > SPLIT_TOLERANCE:
        return None
    return float(best) if forward else 1 / float(best)


def _split_adjust(years: list[int], counts: list[float]
                  ) -> tuple[list[float], list[tuple[int, float]]]:
    """Restate every share count on the LATEST year's basis.

    Walks forward to find the splits, then multiplies each year by the product of
    every split that happened *after* it — so the most recent year is untouched
    and history is expressed in today's shares. Returns the adjusted counts and
    the (fiscal_year, ratio) of each split found.
    """
    splits: list[tuple[int, float]] = []
    prev_i = None
    for i in range(len(counts)):
        if counts[i] is None or pd.isna(counts[i]):
            continue
        if prev_i is not None and counts[prev_i] not in (None, 0) \
                and not pd.isna(counts[prev_i]):
            observed = counts[i] / counts[prev_i]
            if observed >= SPLIT_MIN_RATIO or observed <= 1 / SPLIT_MIN_RATIO:
                if (ratio := _snap_split_ratio(observed)) is not None:
                    splits.append((years[i], ratio))
        prev_i = i

    adjusted = list(counts)
    for split_year, ratio in splits:
        for j, y in enumerate(years):
            if y < split_year and adjusted[j] is not None and not pd.isna(adjusted[j]):
                adjusted[j] = adjusted[j] * ratio
    return adjusted, splits


def buyback(fy: pd.DataFrame) -> KPI:
    """How many shares exist, and whether the company shrank or grew that number.

    Definition chosen with the user: **`shares_outstanding`**, not
    `shares_diluted`. Diluted is a weighted average that counts options, RSUs and
    convertibles — instruments that are not shares yet — and being an average it
    also smears changes that happened inside the year. The question here is how
    many shares actually exist, so the actual count is the right field.

    Coverage cost of that choice, accepted deliberately: **META tags no
    `shares_outstanding` at all** and gets no series, and TSLA is missing FY2021.
    Substituting `shares_diluted` for META would fill the gap with a *different
    measure*, which is worse than an honest gap — the same principle that stops
    this module substituting zero for a missing fact.

    **Split adjustment is the whole difficulty.** Each year's count comes from
    that year's own filing, so a year before a split carries a pre-split number.
    Left raw, NVDA reads as +894% "dilution" across its 10:1 split when the real
    change was -0.7%. Every year is therefore restated on the latest year's
    basis, and the split years are named in the notes so nothing is silently
    rewritten. BRK.B tags neither share field and gets no series at all.
    """
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})
    reported = _num(fy, "shares_outstanding")
    out["shares_reported"] = reported
    out["shares_adjusted"] = np.nan
    out["net_change_pct"] = np.nan

    notes = []

    if (note := _missing_note(fy, reported, "shares_outstanding")):
        notes.append(note)

    if reported.notna().sum() == 0:
        notes.append(
            "This issuer does not tag `shares_outstanding` in XBRL at all, so the "
            "share count cannot be shown. `shares_diluted` is deliberately not "
            "substituted: it is a weighted average that also counts options and "
            "convertibles, so it would answer a different question under this "
            "chart's heading.")
        return KPI(
            name="Buyback (shares outstanding)",
            definition="shares outstanding, restated on the latest year's basis",
            source_fields=["stock_filings.shares_outstanding"],
            data=out, value_col="shares_adjusted", unit="shares", notes=notes,
        )

    years_list = [int(y) for y in out["fiscal_year"]]
    adjusted, splits = _split_adjust(years_list, list(reported))
    out["shares_adjusted"] = adjusted

    # The net change on the ADJUSTED series — which is the buyback or issuance,
    # with the split arithmetic already taken out of it.
    #
    # Only between *consecutive* fiscal years. Comparing FY2020 with FY2022 and
    # labelling it "-2%" would present two years of change as one year's, and
    # gaps are real here: TSLA has no share count for FY2021.
    adj = out["shares_adjusted"]
    prev = adj.shift(1)
    year = pd.to_numeric(out["fiscal_year"], errors="coerce")
    consecutive = (year - year.shift(1)) == 1
    out["net_change_pct"] = ((adj - prev) / prev * 100).where(
        adj.notna() & prev.notna() & (prev != 0) & consecutive)

    if splits:
        notes.append(
            "Split-adjusted: "
            + "; ".join(f"a {r:g}:1 split at FY{y}" for y, r in splits)
            + ". Every earlier year is multiplied up to the latest year's basis, "
              "because each year's count comes from that year's own filing and a "
              "pre-split filing reports pre-split shares. Without this the chart "
              "would read the split as a share issue of several hundred percent.")

    # State the direction in words. The bars move by single-digit percents, which
    # is easy to misread on an axis that starts near the data.
    series = adj.dropna()
    if len(series) >= 2 and series.iloc[0] != 0:
        total = (series.iloc[-1] - series.iloc[0]) / abs(series.iloc[0]) * 100
        direction = ("reduced" if total < 0 else "increased" if total > 0
                     else "left unchanged")
        notes.append(
            f"Across the years shown the company {direction} its share count by "
            f"{abs(total):.1f}% overall"
            + (" — a net buyback." if total < 0 else
               " — net issuance, not a buyback." if total > 0 else "."))

    return KPI(
        name="Buyback (shares outstanding)",
        definition="shares outstanding as tagged on each annual filing, restated "
                   "on the latest year's basis so splits do not read as issuance",
        source_fields=["stock_filings.shares_outstanding"],
        data=out, value_col="shares_adjusted", unit="shares", notes=notes,
    )


# ---------------------------------------------------------------------------
# 3. Debt vs equity
# ---------------------------------------------------------------------------
def debt_equity(fy: pd.DataFrame) -> KPI:
    """Total liabilities against shareholders' equity, and their ratio.

    Definition chosen with the user: `liabilities / equity` — TOTAL liabilities,
    not just long-term debt.

    Note this deliberately differs from the pipeline's own `debt_to_equity`
    field, which is `long_term_debt / equity` (transforms.py:131). Total
    liabilities was chosen because it covers 9 of 10 companies instead of 8:
    `long_term_debt` is null for both BRK.B and JPM, and JPM is a bank, which is
    precisely the company whose leverage is most worth looking at. The tradeoff
    is that this ratio includes operating liabilities (payables, deposits) and so
    reads much higher for a bank than a debt-only measure would — that is a real
    difference in what is being asked, not an error.
    """
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})
    liab, eq = _num(fy, "liabilities"), _num(fy, "equity")
    out["liabilities"] = liab
    out["equity"] = eq
    out["debt_to_equity"] = np.nan

    # Equity can legitimately be negative (accumulated deficit, large buybacks).
    # The ratio is undefined there rather than a large negative number.
    ok = liab.notna() & eq.notna() & (eq > 0)
    out.loc[ok, "debt_to_equity"] = liab[ok] / eq[ok]

    notes = []
    for series, name in ((liab, "liabilities"), (eq, "equity")):
        if (note := _missing_note(fy, series, name)):
            notes.append(note)

    neg = out.loc[eq.notna() & (eq <= 0), "fiscal_year"].tolist()
    if neg:
        notes.append("Equity was zero or negative for "
                     + ", ".join("FY" + str(y) for y in neg)
                     + ", so the ratio is undefined for those years.")

    return KPI(
        name="Debt vs Equity",
        definition="total liabilities / shareholders' equity",
        source_fields=["stock_filings.liabilities", "stock_filings.equity"],
        data=out, value_col="debt_to_equity", unit="ratio", notes=notes,
    )


# ---------------------------------------------------------------------------
# 4. Net profit
# ---------------------------------------------------------------------------
def net_profit(fy: pd.DataFrame) -> KPI:
    """Net income per fiscal year, straight from the filing.

    Kept as the reported figure rather than a margin: the margin view is already
    half of the Rule of 40 chart below, and showing the same number twice in two
    normalisations makes a dashboard harder to read, not richer.
    """
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})
    out["net_income"] = _num(fy, "net_income")

    notes = []
    if (note := _missing_note(fy, out["net_income"], "net_income")):
        notes.append(note)
    losses = out.loc[out["net_income"] < 0, "fiscal_year"].tolist()
    if losses:
        notes.append("Loss-making years: "
                     + ", ".join("FY" + str(y) for y in losses) + ".")

    return KPI(
        name="Net Profit",
        definition="net_income, as reported on the annual (FY) filing",
        source_fields=["stock_filings.net_income"],
        data=out, value_col="net_income", unit="USD", notes=notes,
    )


# ---------------------------------------------------------------------------
# 5. Cash flow
# ---------------------------------------------------------------------------
def cash_flow(fy: pd.DataFrame) -> KPI:
    """Operating cash flow, with free cash flow alongside it where derivable.

    **Operating cash flow is the primary series** because it is the one that is
    actually complete: it resolves for 100% of annual filings in this snapshot.
    Free cash flow (OCF - capex) is shown as a second series but only resolves
    for about 70%, because `capex` is null for AMZN, JPM and NVDA at the annual
    level — those issuers do not tag `PaymentsToAcquirePropertyPlantAndEquipment`
    in the form the snapshot's extraction looks for.

    Both series come from the filing. `free_cash_flow` is already derived by the
    pipeline (transforms.py:133-137) and read from Elasticsearch here rather than
    recomputed, so the dashboard and the analyst stage cannot disagree about it.
    It is recomputed locally only if the field is absent from the document.
    """
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})
    ocf, capex = _num(fy, "operating_cash_flow"), _num(fy, "capex")
    out["operating_cash_flow"] = ocf

    fcf = _num(fy, "free_cash_flow")
    if fcf.isna().all():
        # Same formula the pipeline uses; capex is reported positive for cash
        # spent, so it is subtracted rather than added.
        fcf = np.where(ocf.notna() & capex.notna(), ocf - capex, np.nan)
    out["free_cash_flow"] = fcf

    notes = []
    if (note := _missing_note(fy, ocf, "operating_cash_flow")):
        notes.append(note)
    if (note := _missing_note(fy, out["free_cash_flow"], "free_cash_flow")):
        notes.append(note + " Free cash flow needs `capex`, which this issuer "
                            "does not tag on its annual filing.")

    return KPI(
        name="Cash Flow",
        definition="operating cash flow (primary); free cash flow = operating "
                   "cash flow - capex (shown where capex is reported)",
        source_fields=["stock_filings.operating_cash_flow",
                       "stock_filings.capex", "stock_filings.free_cash_flow"],
        data=out, value_col="operating_cash_flow", unit="USD", notes=notes,
    )


# ---------------------------------------------------------------------------
# 6. Rule of 40
# ---------------------------------------------------------------------------
RULE_OF_40_THRESHOLD = 40.0


def rule_of_40(fy: pd.DataFrame) -> KPI:
    """Revenue growth % + net profit margin %, against a 40% threshold.

    Both inputs are read from fields the pipeline already computed, rather than
    recomputed here:

      * `revenue_yoy` — (current - previous) / |previous|, compared like with
        like: FY against the previous FY (transforms.py:141-147). The absolute
        value in the denominator matters, because a company moving out of a loss
        has a negative previous figure and a plain division would report growth
        as a decline.
      * `net_margin` — net_income / revenue (transforms.py:128).

    Reading them from Elasticsearch rather than deriving them again is the point:
    it keeps one definition of growth in the project, and it means the number on
    this chart is the same number the LLM is given in stage 3. Both are stored as
    fractions, so both are multiplied by 100 here.

    Recomputation happens only if `revenue_yoy` is absent from the document —
    which also covers the earliest fiscal year of a company whose prior year is
    outside the index.

    Honest limit: the Rule of 40 is a SaaS heuristic. Applied to a bank or a
    conglomerate it is a comparison, not a verdict — the threshold line is drawn
    as a reference, not as a pass/fail mark.
    """
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})

    growth = _num(fy, "revenue_yoy")
    margin = _num(fy, "net_margin")

    notes = []

    # Fall back to deriving growth from the revenue series in the window itself.
    # The first year then has no predecessor and stays NaN, which is correct.
    if growth.isna().all():
        rev = _num(fy, "revenue")
        growth = (rev - rev.shift(1)) / rev.shift(1).abs()
        notes.append("`revenue_yoy` was not present on the documents, so growth "
                     "was derived from the revenue series in view. The earliest "
                     "year has no predecessor and is therefore blank.")
    if margin.isna().all():
        rev, ni = _num(fy, "revenue"), _num(fy, "net_income")
        margin = np.where(rev.notna() & ni.notna() & (rev != 0), ni / rev, np.nan)
        margin = pd.Series(margin, index=fy.index)
        notes.append("`net_margin` was not present on the documents, so it was "
                     "derived as net_income / revenue.")

    out["revenue_growth_pct"] = growth * 100.0
    out["net_margin_pct"] = margin * 100.0
    out["rule_of_40"] = out["revenue_growth_pct"] + out["net_margin_pct"]

    if (note := _missing_note(fy, out["rule_of_40"], "rule_of_40 inputs")):
        notes.append(note + " Both revenue growth and net margin are required.")

    return KPI(
        name="Rule of 40",
        definition=f"revenue growth % + net profit margin %, against a "
                   f"{RULE_OF_40_THRESHOLD:g}% threshold",
        source_fields=["stock_filings.revenue_yoy", "stock_filings.net_margin"],
        data=out, value_col="rule_of_40", unit="%", notes=notes,
    )


# ---------------------------------------------------------------------------
# 7. Earnings per share
# ---------------------------------------------------------------------------
def eps(fy: pd.DataFrame) -> KPI:
    """Diluted EPS per fiscal year, with basic EPS alongside for contrast.

    Diluted leads because it is the conservative figure — it assumes every
    option, warrant and convertible is exercised, which is the share count an
    incoming shareholder is actually diluted against. The gap between the two
    lines is itself informative: a widening gap is dilution in progress.

    BRK.B tags neither `eps_diluted` nor `eps_basic` in XBRL, so it has no series
    at all. There is no fallback via net_income / shares_diluted either, because
    `shares_diluted` is null for the same issuer.
    """
    out = pd.DataFrame({"fiscal_year": fy.get("fiscal_year", pd.Series(dtype=int))})
    out["eps_diluted"] = _num(fy, "eps_diluted")
    out["eps_basic"] = _num(fy, "eps_basic")

    notes = []
    if (note := _missing_note(fy, out["eps_diluted"], "eps_diluted")):
        notes.append(note)

    return KPI(
        name="Earnings per Share",
        definition="eps_diluted, as reported on the annual (FY) filing "
                   "(eps_basic shown for contrast)",
        source_fields=["stock_filings.eps_diluted", "stock_filings.eps_basic"],
        data=out, value_col="eps_diluted", unit="USD/share", notes=notes,
    )


# ---------------------------------------------------------------------------
# Share price — a market series, not a KPI
# ---------------------------------------------------------------------------
# Calendar days, not trading days. "One week" is seven days on the calendar,
# which is about five bars; counting five *bars* instead would silently reach
# further back across a holiday week and quietly change what the label means.
PRICE_WINDOWS: dict[str, int] = {"1W": 7, "1M": 30, "3M": 91,
                                 "6M": 182, "1Y": 365}


@dataclass
class PriceView:
    """One company's closing prices over one window, with its provenance."""
    window: str
    data: pd.DataFrame          # date + close (+ OHLC and volume when present)
    source_fields: list[str]
    notes: list[str] = field(default_factory=list)

    @property
    def available(self) -> bool:
        return (not self.data.empty
                and "close" in self.data.columns
                and self.data["close"].notna().any())

    @property
    def change_pct(self) -> float | None:
        """Move across the window, first close to last."""
        if not self.available:
            return None
        s = self.data["close"].dropna()
        if len(s) < 2 or s.iloc[0] == 0:
            return None
        return (s.iloc[-1] - s.iloc[0]) / abs(s.iloc[0]) * 100

    @property
    def last_close(self) -> float | None:
        if not self.available:
            return None
        return float(self.data["close"].dropna().iloc[-1])


def price_view(prices: pd.DataFrame, window: str = "1Y") -> PriceView:
    """Closing prices for the last `window`, anchored to the newest bar.

    **Anchored to the data, not to today.** The snapshot ends at a fixed date and
    is replayed, so "today" drifts away from it: asking for the last seven days
    of wall-clock time returns nothing at all once the snapshot is a week old,
    and the chart would look broken when it is merely historical. The window is
    therefore measured back from the most recent bar present, and the UI states
    the dates it actually covers.
    """
    days = PRICE_WINDOWS.get(window, PRICE_WINDOWS["1Y"])
    empty = pd.DataFrame(columns=["date", "close"])

    if prices is None or prices.empty:
        return PriceView(window, empty, ["stock_prices.close"],
                         ["No price bars in the index for this company."])

    df = prices.copy()
    # `date` is what the Spark transform writes; `ts` is the raw contract field.
    # Accept either so this works against the index and against the snapshot.
    if "date" not in df.columns and "ts" in df.columns:
        df["date"] = df["ts"]
    if "date" not in df.columns or "close" not in df.columns:
        return PriceView(window, empty, ["stock_prices.close"],
                         ["Price documents carry no `date`/`close` pair."])

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return PriceView(window, empty, ["stock_prices.close"],
                         ["No dated price bars for this company."])

    end = df["date"].max()
    start = end - pd.Timedelta(days=days)
    out = df[df["date"] >= start]

    notes = []
    span = (end - df["date"].min()).days
    if span < days:
        notes.append(
            f"The snapshot holds {span} days of prices, which is less than the "
            f"{days}-day window, so this shows everything available.")

    keep = [c for c in ("date", "open", "high", "low", "close", "volume")
            if c in out.columns]
    return PriceView(window, out[keep].reset_index(drop=True),
                     ["stock_prices.close"], notes)


# ---------------------------------------------------------------------------
# All seven, in the order the dashboard presents them
# ---------------------------------------------------------------------------
def compute_all(filings: pd.DataFrame, prices: pd.DataFrame | None = None,
                years: int = 5) -> dict[str, KPI]:
    """Compute every KPI for one company. The single entry point for the UI.

    Returns a dict keyed by a short slug so the chart layer and the AI stage can
    both address the same objects — stage 3 grounds its prompt on exactly these
    numbers, so there is no second computation of the facts anywhere.

    `prices` is **not read by any of the seven** — every one of them comes from
    `stock_filings` alone. The parameter is kept, and kept optional, so callers
    that hold both frames need no special case. Prices are used by `price_view`,
    which drives the share price chart, not by anything here.
    """
    fy = annual_frame(filings, years=years)
    if fy.empty:
        return {}

    return {
        "revenue": revenue(fy),
        "buyback": buyback(fy),
        "debt_equity": debt_equity(fy),
        "net_profit": net_profit(fy),
        "cash_flow": cash_flow(fy),
        "rule_of_40": rule_of_40(fy),
        "eps": eps(fy),
    }
