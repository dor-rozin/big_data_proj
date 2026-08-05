---
id: 0006
title: EDGAR snapshot — pull XBRL facts for the ticker universe to disk
status: todo
layer: producers / ingestion
priority: P1
depends_on: [0001, 0005]
---

## Goal
The fundamentals half of the dataset is what makes this project more than a price
ticker. Joining quarterly financials against a price stream is what lets the
consumer half compute P/E ratios, market cap, and growth-versus-price-movement —
the transformations that carry the grade. Without filings there is nothing
interesting to join.

Unlike Yahoo, SEC EDGAR is an official, stable, documented source with a published
rate limit. It will not block you if you follow the rules, and it will block you
immediately if you do not.

## Scope
- **`ingestion/fetch_filings.py`**, a CLI:

  ```
  python -m ingestion.fetch_filings \
      --tickers-file config/tickers.txt \
      --forms 10-K,10-Q \
      --since 2022-01-01 \
      --out data/raw/filings.parquet
  ```
- **Identify yourself or be blocked.** SEC requires a `User-Agent` header
  containing a real name and email on every request. `edgartools` exposes this via
  `set_identity("Your Name your.email@example.com")` — call it at startup from an
  env var `SEC_IDENTITY`, and **exit with a clear error message if it is unset**.
  A silent default gets the whole team's IP blocked.
- **Respect 10 requests/second.** Throttle explicitly with a small sleep between
  company lookups. Do not parallelize this.
- **Resolve ticker → CIK** once, up front, using the SEC's company-tickers mapping.
  Zero-pad every CIK to 10 characters. Cache the mapping to
  `data/raw/cik_map.json` so re-runs skip the lookup. Write this map out
  regardless — ticket 0004's mock producer and the replay producer both want it.
- **Extract the closed fact set only.** Pull the ten XBRL tags named in ticket
  0001's `schemas/README.md` and nothing else. A tag that is absent for a given
  filing is emitted as `null`, never omitted. Do not iterate over whatever tags
  happen to be present — an open key set breaks the consumer's Elasticsearch
  mapping.
- **One row per filing**, matching `sec.filings.v1.schema.json`: `cik`, `ticker`,
  `accession_no`, `form_type`, `filed_date`, `period_end`, the ten facts, and
  `ingested_at`.
- **Emit the raw filing text nowhere.** A 10-K's full text runs to several
  megabytes; Kafka's default `max.message.bytes` is 1 MB, so a raw-text message
  fails to produce. Only the extracted numeric facts travel through the pipeline.
  Note this constraint in a comment so nobody "improves" the script by adding it.
- **Resilience**: a company with no filings in the window, a filing with no
  parseable XBRL, or a lookup failure logs a warning and continues. Report a
  summary of skipped tickers with reasons at the end.
- **Output**: `data/raw/filings.parquet` plus `data/raw/filings.meta.json`
  (fetch time, forms, date range, row count, skipped tickers with reasons).
- **`--limit N`** flag to process only the first N tickers, for fast iteration.

## Non-goals
- No full-text search, no filing sections, no MD&A extraction, no sentiment.
- No 8-K, no proxy statements, no ownership forms. Two form types is enough to
  demonstrate the join.
- No restatement handling. If a company files an amended report, both rows exist;
  the consumer can pick by `filed_date` if they care.
- No Kafka. File output only, same as ticket 0005.
- No attempt to reconcile SEC fiscal periods with calendar quarters. Emit
  `period_end` as filed and let the consumer decide.

## Acceptance criteria
- Running without `SEC_IDENTITY` set exits non-zero with a message explaining
  exactly what to set and why, before any network request is made.
- `--limit 3` completes in under 60 seconds and writes a valid parquet file.
- A full run over the ticker universe produces at least 150 filing rows spanning
  at least 20 distinct CIKs and both form types.
- Every row converted to a dict validates against `sec.filings.v1.schema.json`,
  including: `cik` is exactly 10 characters, `facts` contains exactly the ten
  agreed keys with no extras, and absent facts are `null` rather than missing.
- No row's serialized JSON exceeds 8 KB (proves no raw text leaked in).
- `data/raw/cik_map.json` exists and maps every ticker in `config/tickers.txt`
  that resolved successfully.
- A ticker that does not resolve to a CIK is reported in the summary and does not
  crash the run.
- Sustained request rate stays under 10/second (verify by logging request
  timestamps in a test run).

## Files
- `ingestion/fetch_filings.py` (new)
- `data/raw/cik_map.json` (generated)
- `tests/test_fetch_filings.py` (new)
- `requirements.txt` (add `edgartools`)
- `.env.example` (new — document `SEC_IDENTITY`)
- `README.md` (document the `SEC_IDENTITY` requirement prominently)

## References
Fact key set and message shape from ticket 0001. Ticker universe shared with
ticket 0005 so the two datasets join. Ticket 0007 consumes
`data/raw/filings.parquet`.
