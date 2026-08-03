# So Far — Work Log

Running record of what is done, so everyone knows where the project stands.
Updated whenever someone declares a piece of work done (see [CLAUDE.md](CLAUDE.md)).

## Current status

| Area | Owner | Status | Notes |
|---|---|---|---|
| `producer/` — yfinance → Kafka | Person A | Code written, not verified | Not yet run end-to-end or tested |
| `spark/` — transform + KMeans anomaly detection | Person B | Code written, not verified | Not yet run end-to-end or tested |
| `spark/` — Elasticsearch load | Person C | Code written, not verified | Not yet run end-to-end or tested |
| `dashboard/` — Streamlit | Person C | Code written, not verified | Not yet run end-to-end or tested |
| `schemas/` — Kafka message contract (tickets 0001, 0010) | Amir | Done | 3 topics frozen, samples from real data, validator passing |
| `sec.text.v1` — unstructured text producer (ticket 0010) | Amir | Contract only | Schema + sample done; producer not written |
| Tests | — | Schema validation only | `scripts/validate_schemas.py`; no framework for the other areas |

**Legend:** `Not started` → `Code written, not verified` → `Runs end-to-end` → `Tested` → `Done`

## How to test

Step 3 of the Definition of Done reads this section. Add a row here whenever you
add tests for an area; if an area has no row, there is nothing to run for it.

| Area | Command |
|---|---|
| `schemas/` — message contract | `.venv/bin/python scripts/validate_schemas.py` (see README for venv setup) |
| everything else | No test framework is set up. No `tests/` directory and no test dependency in `producer/`, `spark/`, or `dashboard/` requirements. |

## Log

### 2026-08-02 — Baseline

Recorded the starting state of the repo. All four pipeline stages have code
written but none have been run end-to-end or tested.

- `producer/produce.py` — pulls price bars and news headlines from yfinance,
  publishes to the `prices` and `news` Kafka topics, with broker-connect retries.
- `spark/pipeline.py` — batch-reads both topics; engineers 4 price features
  (daily return, volume change, intraday range %, 10-day rolling volatility);
  StandardScaler → KMeans (k=3) → flags the top `ANOMALY_FRACTION` of days by
  distance to cluster centre; scores headlines with VADER; bulk-loads both
  datasets into Elasticsearch.
- `dashboard/app.py` — Streamlit UI on port 8501 reading from Elasticsearch.
- `docker-compose.yml` — Kafka (KRaft), Elasticsearch, and the three services.
- `.env.example` — full config template.

**Known gaps:**
- No tests anywhere.
- Pipeline is batch-only — Spark reads Kafka as a bounded batch, so the producer
  must finish before Spark starts. No streaming or refresh loop.
- Nothing is committed to git yet; all files are untracked in the working tree.

### 2026-08-03 — Ticket 0001: Kafka message contract frozen (Amir, producer half)

Wrote the message contract that the Spark job's `from_json` schema and the
Elasticsearch index template are written against. No runtime code — this is the
contract only.

- `schemas/market.prices.v1.schema.json` — OHLCV price bar (draft 2020-12).
  OHLCV fields nullable, `interval` a fixed enum, `additionalProperties: false`.
- `schemas/sec.filings.v1.schema.json` — SEC filing with a **closed** 10-key
  `facts` set (missing facts are `null`, never omitted) so Elasticsearch dynamic
  mapping cannot blow past its 1000-field limit.
- `schemas/samples/*.json` — one realistic sample message per topic, one JSON
  object per line so they pipe straight into `kafka-console-producer`.
- `schemas/README.md` — per-topic field tables, the Kafka message key for each
  topic (`ticker` for prices, `cik` for filings), the three cross-cutting rules
  (UTC-with-`Z` timestamps, uppercase tickers, zero-padded CIKs), and the
  plain-JSON-over-Avro tradeoff.
- `scripts/validate_schemas.py` + `scripts/requirements.txt` — validator, exits
  non-zero naming the offending field.

**Verified:** validator exits 0 on both samples. Mutating a sample to drop the
`Z` from `ts`, lowercase a ticker, add an extra key inside `facts`, un-pad a
CIK, or omit a fact each makes it exit 1 naming that field.

**Status:** `schemas/` is Done. Ticket 0001 marked `done` in `.claude/index.md`.

**Note on scope:** these topics (`market.prices.v1`, `sec.filings.v1`) are the
*new* contract from the `.claude/` ticket backlog. The pipeline currently running
still uses the original `prices` / `news` topics — `producer/produce.py` has not
been reconciled against this contract yet (that is tickets 0004–0007).

### 2026-08-03 — Ticket 0001 revised after validating against real SEC data

Before freezing, the filings contract was checked against actual companyfacts
data for AAPL, MSFT, JPM, XOM, TSLA and WMT. It would have validated cleanly
while producing wrong or missing data in four ways, all now fixed. Full detail in
the "Amendments" section of `.claude/0001-freeze-message-schemas.md`.

- **Alias table instead of raw XBRL tags.** Apple stopped tagging `Revenues` in
  2018 (ASC 606). The original contract would have emitted `null` revenue for
  every modern filing of most large issuers. 5 of 19 facts resolve via a
  non-first alias for at least one company; `shares_outstanding` needs the `dei`
  taxonomy because the `us-gaap` tag is stale for Walmart.
- **`fiscal_period` + `period_start` added.** Annual and quarterly revenue were
  previously indistinguishable. `period_start` is not derivable — Apple's FY2023
  is a 370-day (53-week) fiscal year.
- **Fact set expanded 10 → 19**, still closed.
- **Duration-selection rule added to the contract.** JPM's Q2 10-Q reports
  `NetIncomeLoss` four times (current/prior year x quarter/YTD). Rule: keep only
  facts ending at `period_end`, then the window matching `fiscal_period` (365d
  ±31 FY, 91d ±20 quarterly), else `null`. Quarterly is the discrete quarter,
  never YTD.

**Known consequence:** `operating_cash_flow` and `capex` are null on most
quarterly filings, because cash-flow statements are tagged YTD-only and a
181-day window fails the 91-day tolerance. Deliberate — a YTD number in a field
labelled "discrete quarter" is worse than a null. Deriving it by subtracting the
prior YTD filing is a deferred improvement.

**Nulls are informative, not failures.** Only `revenue`, `net_income`, `assets`,
`equity`, `cash`, `eps_basic` and `eps_diluted` resolved for all six companies.
Banks have no gross profit or classified balance sheet; Walmart doesn't tag total
`Liabilities`. The dashboard must not treat a null as an error.

**Verified:** both filing samples are now extracted from live SEC data rather
than hand-written — Apple's FY2023 10-K (19/19 facts) and JPM's Q2 2024 10-Q
(9/19). `filed_date` on both was taken from the source, not typed. Validator
exits 0 on all three samples across the two topics.

### 2026-08-03 — Local dev venv for the producer half (Amir)

Added `requirements-dev.txt` and a `.venv` on **Python 3.11**, matching the
`python:3.11-slim` base image in `producer/Dockerfile`. Covers the producer half
only — schema validation, both Kafka clients, yfinance, edgartools, pytest.
`spark/` and `dashboard/` deps are deliberately excluded: they run in Docker and
belong to the other half of the team. Setup instructions are in the README.

Rebuilt from scratch and verified: all libraries import, `confluent_kafka.admin.
AdminClient` resolves (what ticket 0003 needs), a live yfinance fetch returns
rows, and the schema validator exits 0.

**Finding — `producer/produce.py` cannot fetch data as committed.**
`producer/requirements.txt` pins `yfinance==0.2.40`, and that version now returns
zero rows with "possibly delisted" for a plain `AAPL` 5-day history: Yahoo moved
its endpoints after that release. `yfinance==1.5.2` returns data, and that is
what the venv pins. **`producer/requirements.txt` was left unchanged** — 1.x is a
major bump from 0.2.x and `produce.py` needs reconciling against it, which is
ticket 0005's job, not a silent edit here. Until then the producer container is
non-functional regardless of Kafka being up. This is the first concrete reason
the producer's "Code written, not verified" status was accurate.

Python 3.14 (the homebrew default on this machine) was rejected: the pinned
`numpy==1.26.4` / `pandas==2.2.2` in `spark/requirements.txt` have no 3.14
wheels, and matching the container matters more than being current.

### 2026-08-03 — Third topic `sec.text.v1`: unstructured data restored (Amir)

**Problem it fixes:** moving from news headlines to SEC filings left the project
with no unstructured data at all — `market.prices.v1` and `sec.filings.v1` are
both entirely numeric. No ticket in the backlog covered text.

**Source chosen: the `EX-99.1` earnings press release attached to an 8-K.**
Verified against real filings:

- It is **dated to the day the stock moves** — Apple filed 8-K
  `0000320193-26-000018` on 2026-07-30 and the matching 10-Q on 2026-07-31. The
  join between text and a price anomaly is exact, not approximate.
- It is small and unambiguous: ~11,000 characters, one exhibit, no section
  parsing. The sample message is 12 KB.
- It is opinionated prose, which is the point: "records", "strongest June quarter
  ever", "up 16 percent year over year".
- It reuses ticket 0006's EDGAR fetch — no new source to make reliable.

**Design: a separate topic, not an extension of `sec.filings.v1`.** A facts
message is ~1 KB, a press release ~12 KB, a full 10-K 357 KB. Combining them
makes every small message pay for the large ones, and the two have different
consumers (numeric join vs NLP) and different Elasticsearch mappings (numerics vs
analyzed text). They join on `accession_no`. This also keeps the 0001 contract
untouched — the change is purely additive.

**Deliberately excluded — 10-K Risk Factors and MD&A.** Splitting a real Apple
10-K on `Item N` headings finds all ten headings within 1,300 characters of each
other: that is the table of contents, not the sections, and `Item 1A` matches
twice. A naive implementation extracts a table of contents and reports success.
edgartools' own section accessor also threw `ValueError` on that filing. Both are
reserved in the `section` enum so adding them later is not a re-freeze.

**Sentiment scoring is deliberately unspecified.** The producer's contract ends
at delivering clean text; how it is scored is the consumer half's decision.

- `schemas/sec.text.v1.schema.json` (new) — includes `chunk_index`/`chunk_total`
  from day one so splitting long sections later is not a contract change.
- `schemas/samples/sec.text.v1.json` (new) — real Apple Q3 FY2026 press release.
- `.claude/0010-filing-text-producer.md` (new) — the producer work. Depends on
  0001, 0003, 0006; slots straight after 0006 while the EDGAR code is fresh.

**Verified:** validator exits 0 on all four samples across the three topics.

**Status:** contract done, producer not written. `sec.text.v1` needs adding to
ticket 0003's topic list when that lands.
