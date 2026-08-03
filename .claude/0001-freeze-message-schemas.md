---
id: 0001
title: Freeze the Kafka message schemas and publish sample fixtures
status: done
layer: contract
priority: P0
depends_on: []
---

## Goal
Two other people are blocked on the exact byte shape of the messages this half of
the project produces. Until the schema is written down and a sample message exists
on disk, the Spark job cannot define a `from_json` schema, the Elasticsearch index
template cannot be written, and any work they do is a guess that will need redoing.

This ticket produces no runtime code. It produces the **contract** — two JSON Schema
files and two sample messages — that every later ticket in this repo and every
ticket in the consumer half is written against. It is the first thing to land and
the last thing that should change.

## Scope
- **`schemas/market.prices.v1.schema.json`** — JSON Schema (draft 2020-12) for the
  price bar message. Required fields:
  - `schema_version` (integer, const `1`)
  - `ticker` (string, uppercase A–Z and dots only, e.g. `BRK.B`)
  - `ts` (string, ISO 8601, **always UTC with a trailing `Z`**) — the bar's period
    start
  - `open`, `high`, `low`, `close` (number, nullable)
  - `volume` (integer, nullable)
  - `interval` (string, enum: `1d`, `1h`, `5m`, `1m`)
  - `ingested_at` (string, ISO 8601 UTC) — when the producer emitted it
- **`schemas/sec.filings.v1.schema.json`** — JSON Schema for the filing message.
  Required fields:
  - `schema_version` (integer, const `1`)
  - `cik` (string, **zero-padded to 10 characters**, e.g. `0000320193`)
  - `ticker` (string, uppercase, nullable — not every CIK maps to a ticker)
  - `accession_no` (string, the SEC accession number with dashes)
  - `form_type` (string, e.g. `10-K`, `10-Q`, `8-K`)
  - `filed_date` (string, `YYYY-MM-DD`)
  - `fiscal_period` (string, enum `FY`, `Q1`–`Q4`) — **added during implementation**
  - `period_start` (string, `YYYY-MM-DD`, nullable) — **added during implementation**
  - `period_end` (string, `YYYY-MM-DD`, nullable)
  - `facts` (object) — **fixed key set only**, see below
  - `ingested_at` (string, ISO 8601 UTC)
- **The `facts` key set is closed.** Do not emit arbitrary XBRL tags. A missing
  fact is emitted as `null`, never omitted. Rationale: an open key set makes
  Elasticsearch dynamic mapping create a new field per XBRL tag, which blows past
  the default 1000-field limit and breaks the consumer's index.
- **The key set is 19 normalised, snake_case names, not raw XBRL tags** — see the
  alias table in `schemas/README.md`. See "Amendments" below for why.
- **`schemas/samples/market.prices.v1.json`** and
  **`schemas/samples/sec.filings.v1.json`** — one realistic, hand-written sample
  message each, valid against its schema.
- **`schemas/README.md`** — a short table per topic: field, type, nullable, meaning,
  plus the three cross-cutting rules stated explicitly (UTC-with-`Z` timestamps,
  uppercase tickers, zero-padded CIKs) and the Kafka message key for each topic
  (`ticker` for prices, `cik` for filings).
- **`scripts/validate_schemas.py`** — validates every file in `schemas/samples/`
  against its corresponding schema using `jsonschema`. Exits non-zero on failure.

## Non-goals
- No Avro, no Protobuf, no Confluent Schema Registry. Plain JSON with a documented
  schema is the deliberate choice for a two-week project; record that decision in
  `schemas/README.md` so it reads as a tradeoff rather than an omission.
- No runtime schema enforcement inside the producers. Producers validate in tests
  (ticket 0004 onward), not on every message in the hot path.
- No `v2` of anything. If a field genuinely must change, the topic name changes.

## Acceptance criteria
- `python scripts/validate_schemas.py` exits 0 and prints one line per validated
  sample.
- Mutating a sample to violate its schema (e.g. `ts` without the `Z`, a lowercase
  ticker, an extra key inside `facts`) makes the script exit non-zero with a
  message naming the offending field.
- Both schemas set `"additionalProperties": false` at the top level and inside
  `facts`.
- `schemas/README.md` states the message key for each topic and the three
  cross-cutting rules.
- The two sample files are committed and can be piped straight into
  `kafka-console-producer` without editing.

## Files
- `schemas/market.prices.v1.schema.json` (new)
- `schemas/sec.filings.v1.schema.json` (new)
- `schemas/samples/market.prices.v1.json` (new)
- `schemas/samples/sec.filings.v1.json` (new)
- `schemas/README.md` (new)
- `scripts/validate_schemas.py` (new)

## Amendments

Four changes were agreed during implementation, each driven by checking the
contract against real SEC data (AAPL, MSFT, JPM, XOM, TSLA, WMT) rather than
against expectation. The contract as originally written would have validated
cleanly while producing wrong or missing data.

1. **Facts are normalised names with an alias table, not raw XBRL tags.** Apple
   stopped tagging `Revenues` in 2018 (ASC 606); every filing since uses
   `RevenueFromContractWithCustomerExcludingAssessedTax`. A literal lookup would
   have emitted `null` revenue for every modern filing of most large issuers.
   Each canonical name now resolves against an ordered tag list, first match
   wins. `shares_outstanding` crosses taxonomies (`dei` before `us-gaap`).
2. **`fiscal_period` and `period_start` added.** A 10-K's revenue is annual, a
   10-Q's is quarterly — same field, ~4x apart, previously indistinguishable.
   `period_start` cannot be derived: Apple's FY2023 is 370 days (53-week year).
3. **Fact set expanded from 10 to 19** to support margin, leverage, liquidity and
   per-share ratios. Still closed, so the ES field-limit rationale holds.
4. **A duration-selection rule is now part of the contract.** One filing repeats
   the same tag over several windows (JPM's Q2 10-Q reports `NetIncomeLoss` four
   times: current/prior year x quarter/YTD). Rule: drop facts whose `end` is not
   `period_end`, keep the window matching `fiscal_period` (365d ±31 for FY, 91d
   ±20 for quarters), emit `null` outside tolerance. Quarterly means the discrete
   quarter, never year-to-date.

Consequences to carry into 0006:
- `operating_cash_flow` and `capex` are null on most quarterly filings —
  cash-flow statements are tagged YTD-only. Deriving the discrete quarter by
  subtracting the prior YTD filing is a deferred improvement, not done today.
- Do not filter by form type: Apple's earnings-release 8-Ks carry 569 facts.
- Snapshot the raw companyfacts JSON to disk and project on the way to Kafka, so
  changing the fact list is a local re-run rather than a new SEC crawl.
- SEC requests need a `User-Agent` header or they 403; rate limit is 10 req/sec.

## References
Consumer half needs `facts` to be a fixed key set — see the Elasticsearch dynamic
mapping field-limit note above. Ticket 0004 (mock producer) is the first consumer
of these fixtures.
