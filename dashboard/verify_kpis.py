#!/usr/bin/env python3
"""
Offline check of dashboard/kpis.py against the parquet snapshot.

    python dashboard/verify_kpis.py            # all tickers, summary table
    python dashboard/verify_kpis.py AAPL       # one ticker, every number

Why this exists: the dashboard reads Elasticsearch, and standing Elasticsearch up
needs Docker, Kafka, a producer run and a Spark run. That is a slow loop to be in
while checking whether a ratio is the right way up. This script rebuilds the same
document shape straight from `historical_data/` so the arithmetic in kpis.py can
be exercised with nothing running.

**It is a check of the maths, not of the pipeline.** It reproduces the parts of
spark/transforms.py that produce the fields the KPIs consume — the restatement
dedup, the null-safe ratios and the year-on-year growth. If this script and the
real pipeline ever disagree, the pipeline is right and this file is stale.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kpis  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
FILINGS = REPO / "historical_data" / "sec.filings.v1.historical" / "all.parquet"
PRICES = REPO / "historical_data" / "market.prices.v1.historical" / "all.parquet"


def load_filings() -> pd.DataFrame:
    """Parquet -> the same flat shape `stock_filings` documents have in ES."""
    df = pd.read_parquet(FILINGS)

    # The topic carries the 19 facts in a nested struct; transform_filings
    # flattens them into real columns before the load. Mirror that.
    facts = pd.json_normalize(df["facts"].apply(dict))
    facts.index = df.index
    df = pd.concat([df.drop(columns=["facts"]), facts], axis=1)

    df["filed_date_d"] = pd.to_datetime(df["filed_date"], errors="coerce")
    df["period_end_d"] = pd.to_datetime(df["period_end"], errors="coerce")

    # Restatement dedup: a 10-K/A restates an already-filed period under a new
    # accession. Keep the latest filing per (cik, fiscal_period, period_end).
    df = (df.sort_values(["filed_date_d", "accession_no"])
            .drop_duplicates(subset=["cik", "fiscal_period", "period_end"],
                             keep="last"))

    def ratio(num, den):
        n, d = df[num], df[den]
        return (n / d).where(n.notna() & d.notna() & (d != 0))

    df["net_margin"] = ratio("net_income", "revenue")
    df["gross_margin"] = ratio("gross_profit", "revenue")
    df["return_on_equity"] = ratio("net_income", "equity")
    df["debt_to_equity"] = ratio("long_term_debt", "equity")
    df["free_cash_flow"] = (df["operating_cash_flow"] - df["capex"]).where(
        df["operating_cash_flow"].notna() & df["capex"].notna())

    # Year-on-year growth, like against like: FY vs previous FY, Q2 vs previous
    # Q2. Denominator is an absolute value so a company leaving a loss does not
    # register its recovery as a decline.
    df = df.sort_values(["cik", "fiscal_period", "period_end_d"])
    for src, out in (("revenue", "revenue_yoy"), ("net_income", "net_income_yoy")):
        prev = df.groupby(["cik", "fiscal_period"])[src].shift(1)
        df[out] = ((df[src] - prev) / prev.abs()).where(
            df[src].notna() & prev.notna() & (prev != 0))

    return df


def load_prices() -> pd.DataFrame:
    p = pd.read_parquet(PRICES)
    p["date"] = pd.to_datetime(p["ts"], errors="coerce").dt.tz_localize(None)
    return p


def fmt(v, unit):
    if pd.isna(v):
        return "--"
    if unit == "USD":
        for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
            if abs(v) >= div:
                return f"${v / div:,.2f}{suf}"
        return f"${v:,.0f}"
    if unit == "%":
        return f"{v:.1f}%"
    if unit == "years":
        return f"{v:.1f}y"
    if unit == "USD/share":
        return f"${v:.2f}"
    return f"{v:.2f}"


def main() -> int:
    if not FILINGS.exists():
        print(f"missing {FILINGS}")
        return 1

    filings, prices = load_filings(), load_prices()
    wanted = [t.upper() for t in sys.argv[1:]]
    tickers = wanted or sorted(filings["ticker"].dropna().unique())

    problems = 0
    for ticker in tickers:
        f = filings[filings["ticker"] == ticker]
        p = prices[prices["ticker"] == ticker]
        result = kpis.compute_all(f, p, years=5)

        if not result:
            print(f"{ticker}: no annual filings found")
            problems += 1
            continue

        built = [k for k, v in result.items() if v.available]
        empty = [k for k, v in result.items() if not v.available]
        flag = "OK " if len(built) >= 6 else "!! "
        print(f"\n{flag}{ticker}   {len(built)}/7 charts have data"
              + (f"   EMPTY: {', '.join(empty)}" if empty else ""))

        if not wanted:
            continue

        # Single-ticker mode: print every number, so the values can be eyeballed
        # against the filings themselves.
        for kpi in result.values():
            print(f"\n  --- {kpi.name}  [{kpi.definition}]")
            d = kpi.data
            years = " ".join(f"{int(y):>10}" for y in d["fiscal_year"])
            print(f"      {'fiscal year':<22}{years}")
            for col in d.columns:
                if col == "fiscal_year":
                    continue
                unit = kpi.unit if col == kpi.value_col else (
                    "USD/share" if "eps" in col else
                    "%" if col.endswith("_pct") else
                    "USD" if d[col].abs().max() > 1e6 else "ratio")
                vals = " ".join(f"{fmt(v, unit):>10}" for v in d[col])
                mark = "*" if col == kpi.value_col else " "
                print(f"    {mark} {col:<22}{vals}")
            for note in kpi.notes:
                print(f"      note: {note}")

    print(f"\n{'-' * 70}")
    print("checked", len(tickers), "ticker(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
