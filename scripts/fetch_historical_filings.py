"""Pull SEC XBRL facts (10-K/10-Q) for a fixed set of tickers and save as parquet.

Output conforms to the frozen `sec.filings.v1` contract
(schemas/sec.filings.v1.schema.json / schemas/README.md): one row per filing,
with the closed 19-key `facts` set resolved via the alias table and the
duration/instant selection rules documented there — never a raw XBRL tag lookup.

One file per ticker in `historical_data/sec.filings.v1.historical/`, plus a
combined `all.parquet`.

Requires `SEC_IDENTITY` (a real name + email) in the environment: SEC requires
it as a User-Agent on every request and will block requests without one.
"""

import os
import sys
import time
from datetime import date, datetime, timezone

import pandas as pd
from edgar import Company, set_identity

TICKERS = [
    "NVDA",   # NVIDIA Corp
    "AAPL",   # Apple Inc.
    "MSFT",   # Microsoft Corp.
    "AMZN",   # Amazon.com Inc.
    "GOOGL",  # Alphabet Inc.
    "AVGO",   # Broadcom Inc.
    "META",   # Meta Platforms, Inc.
    "TSLA",   # Tesla, Inc.
    "BRK.B",  # Berkshire Hathaway Inc.
    "JPM",    # JPMorgan Chase & Co.
]

OUT_DIR = "historical_data/sec.filings.v1.historical"
FORMS = ["10-K", "10-Q"]

# Alias table from schemas/README.md — canonical name -> (basis, tags in priority order).
# First matching tag wins; canonical names are ours, never a raw XBRL tag.
ALIAS_TABLE = {
    "revenue": ("duration", [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
    ]),
    "cost_of_revenue": ("duration", ["CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold"]),
    "gross_profit": ("duration", ["GrossProfit"]),
    "operating_income": ("duration", ["OperatingIncomeLoss"]),
    "net_income": ("duration", [
        "NetIncomeLoss", "ProfitLoss", "NetIncomeLossAvailableToCommonStockholdersBasic",
    ]),
    "rnd_expense": ("duration", ["ResearchAndDevelopmentExpense"]),
    "eps_basic": ("duration", ["EarningsPerShareBasic"]),
    "eps_diluted": ("duration", ["EarningsPerShareDiluted"]),
    "shares_diluted": ("duration", ["WeightedAverageNumberOfDilutedSharesOutstanding"]),
    "assets": ("instant", ["Assets"]),
    "assets_current": ("instant", ["AssetsCurrent"]),
    "liabilities": ("instant", ["Liabilities"]),
    "liabilities_current": ("instant", ["LiabilitiesCurrent"]),
    "equity": ("instant", [
        "StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ]),
    "cash": ("instant", [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ]),
    "long_term_debt": ("instant", ["LongTermDebtNoncurrent", "LongTermDebt"]),
    "shares_outstanding": ("instant", [
        "dei:EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding",
    ]),
    "operating_cash_flow": ("duration", [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ]),
    "capex": ("duration", ["PaymentsToAcquirePropertyPlantAndEquipment"]),
}
FACT_KEYS = list(ALIAS_TABLE.keys())

FY_TARGET_DAYS, FY_TOLERANCE_DAYS = 365, 31
Q_TARGET_DAYS, Q_TOLERANCE_DAYS = 91, 20

CONTRACT_COLUMNS = [
    "schema_version", "cik", "ticker", "accession_no", "form_type", "filed_date",
    "fiscal_period", "period_start", "period_end", "facts", "ingested_at",
]


def _duration_days(row) -> "float | None":
    if pd.isna(row["start"]) or pd.isna(row["end"]):
        return None
    return (pd.Timestamp(row["end"]) - pd.Timestamp(row["start"])).days


def _resolve_fact(facts_for_filing: pd.DataFrame, basis: str, tags: list, period_end: str, fiscal_period: str):
    """Return (value, period_start) for the first matching tag, else (None, None)."""
    target, tolerance = (FY_TARGET_DAYS, FY_TOLERANCE_DAYS) if fiscal_period == "FY" else (Q_TARGET_DAYS, Q_TOLERANCE_DAYS)

    for tag in tags:
        namespace, _, bare_tag = tag.rpartition(":")
        namespace = namespace or "us-gaap"
        candidates = facts_for_filing[
            (facts_for_filing["fact"] == bare_tag)
            & (facts_for_filing["namespace"] == namespace)
            & (facts_for_filing["end"] == period_end)
        ]
        if candidates.empty:
            continue

        if basis == "instant":
            row = candidates.iloc[0]
            if pd.notna(row["val"]):
                return float(row["val"]), None
            continue

        # Duration: drop rows with no start, then keep the one closest to the
        # target window (within tolerance) — this is what separates the
        # discrete quarter from the year-to-date figure carrying the same tag.
        durations = candidates.assign(_days=candidates.apply(_duration_days, axis=1))
        durations = durations.dropna(subset=["_days"])
        durations = durations[(durations["_days"] - target).abs() <= tolerance]
        if durations.empty:
            continue
        best = durations.iloc[(durations["_days"] - target).abs().argsort()].iloc[0]
        if pd.notna(best["val"]):
            return float(best["val"]), str(best["start"])[:10]

    return None, None


def build_filing_row(cik: str, ticker: str, facts_df: pd.DataFrame, filing_row, ingested_at: str) -> dict:
    accession_no = filing_row["accession_number"]
    period_end = str(filing_row["reportDate"])[:10] if pd.notna(filing_row["reportDate"]) else None
    form_type = filing_row["form"]
    filed_date = str(filing_row["filing_date"])[:10]

    facts_for_filing = facts_df[facts_df["accn"] == accession_no]

    fiscal_period = "FY" if form_type.startswith("10-K") else None
    if fiscal_period is None and not facts_for_filing.empty:
        fp_mode = facts_for_filing["fp"].mode()
        fiscal_period = fp_mode.iloc[0] if not fp_mode.empty else None
    if fiscal_period not in ("FY", "Q1", "Q2", "Q3", "Q4"):
        fiscal_period = "FY" if form_type.startswith("10-K") else "Q1"

    facts = {}
    period_start = None
    for canonical, (basis, tags) in ALIAS_TABLE.items():
        value, start = (None, None) if period_end is None else _resolve_fact(
            facts_for_filing, basis, tags, period_end, fiscal_period
        )
        facts[canonical] = value
        if period_start is None and start is not None:
            period_start = start

    return {
        "schema_version": 1,
        "cik": cik,
        "ticker": ticker,
        "accession_no": accession_no,
        "form_type": form_type,
        "filed_date": filed_date,
        "fiscal_period": fiscal_period,
        "period_start": period_start,
        "period_end": period_end,
        "facts": facts,
        "ingested_at": ingested_at,
    }


def fetch_ticker(ticker: str, ingested_at: str) -> pd.DataFrame:
    company = Company(ticker)
    cik = f"{company.cik:010d}"

    filings = company.get_filings(form=FORMS)
    filings_df = filings.to_pandas()
    if filings_df.empty:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)

    facts_df = company.get_facts().to_pandas()

    rows = [
        build_filing_row(cik, ticker, facts_df, filing_row, ingested_at)
        for _, filing_row in filings_df.iterrows()
    ]
    return pd.DataFrame(rows, columns=CONTRACT_COLUMNS)


def main() -> None:
    sec_identity = os.getenv("SEC_IDENTITY")
    if not sec_identity:
        sys.exit(
            "SEC_IDENTITY is not set. SEC requires a real name and email as a "
            "User-Agent on every EDGAR request and will block unidentified "
            "traffic. Set it, e.g.:\n"
            '  export SEC_IDENTITY="Your Name your.email@example.com"'
        )
    set_identity(sec_identity)

    os.makedirs(OUT_DIR, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    frames = []
    skipped = []
    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        try:
            df = fetch_ticker(ticker, ingested_at)
        except Exception as exc:
            print(f"  SKIPPED: {exc}")
            skipped.append((ticker, str(exc)))
            continue
        out_path = os.path.join(OUT_DIR, f"{ticker.replace('.', '_')}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  {len(df)} rows -> {out_path}")
        frames.append(df)
        time.sleep(0.5)  # SEC rate limit is 10 req/s; get_facts + get_filings is a handful per ticker

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=CONTRACT_COLUMNS)
    combined_path = os.path.join(OUT_DIR, "all.parquet")
    combined.to_parquet(combined_path, index=False)
    print(f"Combined: {len(combined)} rows -> {combined_path}")
    if skipped:
        print(f"Skipped {len(skipped)} ticker(s): {skipped}")


if __name__ == "__main__":
    main()
