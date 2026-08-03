"""Pull 2 years of daily price history for a fixed set of tickers and save as parquet.

Output conforms to the frozen `market.prices.v1` contract
(schemas/market.prices.v1.schema.json / schemas/README.md): one row per
`(ticker, ts)` with exactly the 10 contract fields, UTC `Z` timestamps, no NaN
(nulls only), and native Python numeric types.

One file per ticker in `historical_data/`, plus a combined `historical_data/all.parquet`.
"""

import os
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

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

OUT_DIR = "historical_data/market.prices.v1.historical"
PERIOD = "2y"
INTERVAL = "1d"  # matches the market.prices.v1 `interval` enum value

# CONTRACT_COLUMNS mirrors schemas/market.prices.v1.schema.json field order exactly.
CONTRACT_COLUMNS = [
    "schema_version", "ticker", "ts", "open", "high", "low", "close",
    "volume", "interval", "ingested_at",
]


def _to_native(value):
    """Cast numpy scalars to native Python types; NaN becomes None."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    return value


def fetch_ticker(ticker: str, ingested_at: str) -> pd.DataFrame:
    # Yahoo Finance uses a dash for share-class tickers (e.g. BRK.B -> BRK-B).
    yahoo_symbol = ticker.replace(".", "-")
    # auto_adjust=True: dividend/split-adjusted OHLC, so historical bars stay
    # comparable across corporate actions without a separate adjustment step.
    raw = yf.Ticker(yahoo_symbol).history(period=PERIOD, interval=INTERVAL, auto_adjust=True)
    raw = raw.reset_index()

    rows = []
    for _, r in raw.iterrows():
        ts = r["Date"]
        if ts.tzinfo is None:
            ts = ts.tz_localize(timezone.utc)
        else:
            ts = ts.tz_convert("UTC")
        rows.append({
            "schema_version": 1,
            "ticker": ticker,
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "open": _to_native(r["Open"]),
            "high": _to_native(r["High"]),
            "low": _to_native(r["Low"]),
            "close": _to_native(r["Close"]),
            "volume": _to_native(r["Volume"]),
            "interval": INTERVAL,
            "ingested_at": ingested_at,
        })
    return pd.DataFrame(rows, columns=CONTRACT_COLUMNS)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    frames = []
    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        df = fetch_ticker(ticker, ingested_at)
        out_path = os.path.join(OUT_DIR, f"{ticker.replace('.', '_')}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  {len(df)} rows -> {out_path}")
        frames.append(df)
        time.sleep(1)

    combined = pd.concat(frames, ignore_index=True)
    combined_path = os.path.join(OUT_DIR, "all.parquet")
    combined.to_parquet(combined_path, index=False)
    print(f"Combined: {len(combined)} rows -> {combined_path}")


if __name__ == "__main__":
    main()
