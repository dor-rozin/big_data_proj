---
id: 0010
title: Filing text — extract 8-K press releases to the sec.text.v1 topic
status: in-progress
layer: producer
priority: P1
depends_on: [0001, 0003, 0006]
---

## Goal
`market.prices.v1` and `sec.filings.v1` are both entirely numeric. This project
is supposed to handle unstructured data, and right now nothing in the backlog
produces any. The original pipeline had news headlines; the move to EDGAR
dropped them and nothing replaced them.

This ticket puts text back, from the source we are already fetching. It produces
the `sec.text.v1` topic: the `EX-99.1` earnings press release attached to an 8-K,
extracted as clean plain text, keyed and dated so it joins against both the
price bars and the filing facts.

The schema is already frozen in `schemas/sec.text.v1.schema.json` with a real
sample at `schemas/samples/sec.text.v1.json`. This ticket writes the producer,
not the contract.

## Why press releases specifically
- **They land on the day the stock moves.** The earnings 8-K is filed the day
  before the matching 10-Q — Apple filed 8-K `0000320193-26-000018` on
  2026-07-30 and the 10-Q on 2026-07-31. Joining text to a price anomaly by
  `filed_date` is therefore meaningful, not approximate.
- **They are small and unambiguous.** One exhibit, ~11,000 characters, no
  section-splitting required. Apple's most recent is a 12 KB message.
- **They are opinionated prose**, which is the point: "records", "strongest June
  quarter ever", "up 16 percent year over year".

## Scope
- **Extend the 0006 snapshot to save `EX-99.1` exhibits.** Reuse that ticket's
  EDGAR client, identity header, and rate limiting. Do not add a second fetch
  path and do not re-crawl: raw exhibits land on disk, projection happens on the
  way to Kafka, same as the facts.
- **`producers/text_producer.py`** reading the snapshot and publishing to
  `sec.text.v1`, keyed by `cik`.
- **Text normalisation**, exactly as documented in `schemas/README.md`: strip
  HTML, collapse runs of spaces/tabs to one, collapse blank-line runs to one,
  drop a leading `Exhibit 99.1` marker, preserve Unicode. Never emit empty text.
- **`title`** from the first line of the document. Null if nothing usable — do
  not fall back to the first N characters of the body, which produces a title
  that is a truncated sentence.
- **`chunk_index` / `chunk_total`** set to `0` and `1`. Chunking is not needed at
  this size; the fields exist so adding it later is not a contract change.
- **Topic config** added to `scripts/create_topics.py` from ticket 0003:
  `sec.text.v1`, 3 partitions, replication 1, `retention.ms` `-1`, key `cik`.

## Non-goals
- **No `risk_factors` or `mda`.** Both are reserved in the `section` enum and
  neither is produced here. Splitting a 10-K by `Item N` heading finds all ten
  headings inside the table of contents — a naive implementation extracts a
  table of contents and reports success. That is its own ticket if wanted.
- **No sentiment scoring, no NLP, no model.** The producer's job ends at
  delivering clean text. How it is scored belongs to the consumer half and is
  deliberately unspecified.
- **No news headlines.** yfinance news is shallow, has no history, and cannot be
  backfilled to match two years of price bars.
- **No chunking implementation.**

## Acceptance criteria
- Running the producer against the snapshot publishes one message per `EX-99.1`
  exhibit found, and every message validates against `sec.text.v1.schema.json`.
- Every emitted `accession_no` also appears on `sec.filings.v1`, so the two
  topics join. A text message for a filing with no facts message is a bug.
- An 8-K with no `EX-99.1` attachment produces no message and no error — most
  8-Ks are not earnings releases.
- `text` never contains raw HTML tags, never contains a run of three or more
  newlines, and is never empty or whitespace-only.
- Re-running the producer over the same snapshot produces byte-identical
  messages apart from `ingested_at`.
- The `®` in "Apple ®" and typographic quotes survive the round trip through
  Kafka intact, verified by consuming a message back.

## Files
- `producers/text_producer.py` (new)
- `producers/common.py` (reuse from 0004)
- `scripts/create_topics.py` (add the third topic)
- 0006's snapshot script (extend to save exhibits)

## References
Contract and normalisation rules: `schemas/README.md`, section `sec.text.v1`.
Reuses the EDGAR snapshot from 0006 and the producer factory from 0004. Topic
provisioning from 0003. The consumer half needs to know this topic exists before
they write their Elasticsearch mapping — `text` needs an analyzed field, not a
keyword.
