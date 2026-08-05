---
id: 0005
title: yfinance snapshot — pull the price universe to disk once, safely
status: todo
layer: producers / ingestion
priority: P1
depends_on: [0001]
---

## Goal
`yfinance` is not an API. It scrapes Yahoo Finance's internal endpoints with no
authentication, so sustained use looks like a denial-of-service attempt from
Yahoo's side and gets the source IP rate-limited or blocked, sometimes for more
than a day. There is no quota to request and no key to buy. A pipeline that calls
Yahoo on every run is a pipeline that will fail during the demo.

The fix is to separate *fetching* from *producing*. This ticket fetches the entire
price universe to local disk exactly once. Ticket 0007 replays that file into Kafka
as many times as anyone wants, at any speed, offline. Everything downstream becomes
reproducible, and nobody's laptop gets blocked.

## Scope
- **`ingestion/fetch_prices.py`**, a CLI:

  ```
  python -m ingestion.fetch_prices \
      --tickers-file config/tickers.txt \
      --period 2y \
      --interval 1d \
      --out data/raw/prices.parquet
  ```
- **`config/tickers.txt`** — 30 to 50 large-cap US tickers, one per line, comments
  with `#` allowed. Keep it to companies that file with the SEC, since ticket 0006
  needs the same universe. 30 tickers of 2 years of daily bars is roughly 15,000
  rows: large enough to be a credible "big data" demo, small enough to fetch
  without trouble.
- **Batch, don't loop.** Call `yf.download(tickers, ...)` with the full list in one
  call (it batches internally) rather than iterating `yf.Ticker(t).history()` per
  symbol. Per-symbol looping is what triggers rate limiting.
- **Chunk with backoff.** Split the ticker list into chunks of 10, sleep 2 seconds
  between chunks, and wrap each chunk in a retry with exponential backoff (2s, 4s,
  8s, 16s, then give up) catching `YFRateLimitError` and generic request failures.
  On final failure, record which tickers failed, continue with the rest, and report
  them at the end — a partial dataset is useful, a crashed script is not.
- **Normalize to the ticket 0001 contract before writing to disk.** The output file
  must already be in message shape, so the replay producer is a dumb pipe:
  - Flatten the multi-index columns `yf.download` returns for multiple tickers into
    a long format: one row per `(ticker, ts)`.
  - **Convert timezones explicitly.** yfinance returns timezone-aware timestamps in
    the exchange's local timezone. Call `.tz_convert('UTC')` and format as ISO 8601
    with `Z`. Do not let `str()` decide the format.
  - **Kill NaN before it becomes JSON.** Missing bars come through as `NaN`, and
    `json.dumps` will happily emit a bare `NaN` literal, which is invalid JSON. A
    consumer's `from_json` then returns null for the **entire row**, not just that
    field — a genuinely painful bug to diagnose from the consumer side. Replace all
    NaN with `None` here, at the source.
  - **Cast numpy scalars to Python types.** `np.float64` and `np.int64` are not
    JSON-serializable and raise `TypeError: Object of type int64 is not JSON
    serializable`. Cast explicitly rather than relying on a `default=` handler.
  - Uppercase every ticker.
- **Output**: parquet at `data/raw/prices.parquet`, plus a sidecar
  `data/raw/prices.meta.json` recording fetch time, ticker list, period, interval,
  row count, and any tickers that failed.
- **`--dry-run`** flag that fetches a single ticker and prints the normalized
  output without writing, for fast iteration.

## Non-goals
- No incremental/delta fetching. Re-running overwrites. Two weeks does not justify
  a watermark table.
- No intraday data. Yahoo restricts intraday history windows and it invites rate
  limiting for no pedagogical gain.
- No Kafka in this ticket at all. This script's only output is a file.
- No corporate action adjustment beyond whatever `auto_adjust` gives by default —
  set it explicitly and note the choice in a comment.

## Acceptance criteria
- `python -m ingestion.fetch_prices --dry-run --tickers-file config/tickers.txt`
  prints one normalized row and exits 0 without writing files.
- A full run produces `data/raw/prices.parquet` with at least 10,000 rows across
  at least 25 distinct tickers, and a populated `prices.meta.json`.
- **Zero NaN values in the written file** — assert this in a test that loads the
  parquet and checks `df.isna().sum().sum()` against expected nullable columns
  only, and that no value is the float `NaN`.
- Every row converted to a dict and passed through `json.dumps` succeeds without
  `TypeError`, and the result validates against `market.prices.v1.schema.json`.
  Test this over the full file, not a sample.
- Every `ts` value ends with `Z` and parses as UTC.
- Simulating a rate-limit error (monkeypatch the fetch to raise on the second
  chunk) results in the script completing, reporting the failed tickers, and
  writing the successfully-fetched remainder.
- The script never runs longer than ~3 minutes for 50 tickers.

## Files
- `ingestion/__init__.py` (new)
- `ingestion/fetch_prices.py` (new)
- `config/tickers.txt` (new)
- `tests/test_fetch_prices.py` (new)
- `requirements.txt` (add `yfinance`, `pandas`, `pyarrow`)
- `.gitignore` (ensure `data/` is excluded)
- `versions.md` (record the resolved yfinance version — it breaks between releases)

## References
Output shape is defined by `schemas/market.prices.v1.schema.json` (ticket 0001).
Ticket 0007 consumes `data/raw/prices.parquet`.
