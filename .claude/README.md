# Tickets — Kafka & producers

Scope owned by this half of the project: message contract, Docker infrastructure,
topic provisioning, and everything that writes to Kafka. The consumer half owns
Spark, Elasticsearch, and Kibana.

## Tickets

| id | title | priority | depends on |
|---|---|---|---|
| 0001 | Freeze the Kafka message schemas and publish sample fixtures | P0 | — |
| 0002 | Docker Compose stack — Kafka (KRaft), Elasticsearch, Kibana | P0 | — |
| 0003 | Topic provisioning — create both topics with explicit, replayable config | P0 | 0002 |
| 0004 | Mock producer — synthetic, schema-valid messages with zero external deps | P0 | 0001, 0003 |
| 0005 | yfinance snapshot — pull the price universe to disk once, safely | P1 | 0001 |
| 0006 | EDGAR snapshot — pull XBRL facts for the ticker universe to disk | P1 | 0001, 0005 |
| 0007 | Replay producer — stream the snapshot files into Kafka at controllable speed | P0 | 0003–0006 |
| 0008 | Live producer — Finnhub WebSocket trades aggregated into bars | P2 | 0001, 0003, 0004, 0007 |
| 0009 | README, demo runbook, and cold-start verification | P1 | 0002–0004, 0007 |
| 0010 | Filing text — 8-K press releases into `sec.text.v1` | P1 | 0001, 0003, 0006 |
| 0011 | Dagster orchestration for the interval Spark run (stretch) | P3 | 0007, 0009 |

## Data sources

Three sources, each doing the one thing it is best at:

| source | role | why |
|---|---|---|
| yfinance | historical price bars, fetched once to disk (0005) | free and deep; unreliable under sustained use, which is why it is fetched once and never called at runtime |
| edgartools / SEC EDGAR | quarterly XBRL fundamentals (0006) | official, stable, documented rate limit; the join partner that makes the pipeline interesting |
| Finnhub WebSocket | live trades during the presentation (0008) | 60 calls/min and a free socket for up to 50 symbols; the only source that produces visible motion |

All three land on the same topics with the same message contract. A consumer
cannot tell which source a bar came from. That is deliberate — it is one pipeline
with three ingestion adapters, not three pipelines.

There are **three** topics, not two: `market.prices.v1`, `sec.filings.v1`, and
`sec.text.v1`. The third carries the project's only unstructured data — the
`EX-99.1` earnings press release attached to an 8-K, added in ticket 0010. The
numbers and the words from the same filing travel separately because they differ
by an order of magnitude in size and have different consumers; they join on
`accession_no`.

## Order of work

Tickets 0001 and 0002 are both unblocked and both block everyone. Do them first,
in parallel if you can — 0001 is writing, 0002 is configuration.

Ticket 0004 is the one that unblocks your two teammates. It has no external
dependencies by design, so it can land while yfinance is still rate-limiting you.
Getting it done on day one is worth more than getting the real producers done on
day three.

Suggested schedule against a one-week v1:

- **Day 1** — 0001 and 0002. End state: stack up, schemas frozen and committed.
- **Day 2** — 0003 and 0004. End state: teammates have real bytes to consume.
- **Day 3** — 0005. End state: price data on disk, NaN-free and schema-valid.
- **Day 4** — 0006. End state: filings on disk, joinable against prices.
- **Day 5** — 0007. End state: v1 complete, replayable at any speed.
- **Week 2** — 0009 first (the demo runbook is what gets graded), then 0008.

Ticket 0008 moved from P3 to P2 because the live WebSocket is now a real
differentiator rather than a marginal polling loop. It is still cuttable: if week
two gets tight, drop it and present with the replay producer at
`--speed realtime`, describing snapshot-and-replay as a deliberate choice for
reproducibility. That is a defensible position, not an excuse.

## Conventions for whoever picks these up

- Paste `versions.md` into the context of any AI coding assistant before asking
  for code in this repo. Version drift between the Kafka client, broker, and
  Python is the most common source of failures that look like logic bugs.
- Implement one ticket at a time and run its acceptance criteria before starting
  the next. Several tickets have criteria specifically designed to catch a
  plausible-looking wrong implementation (the interleaving check in 0007, the NaN
  check in 0005, the double-run dedup check in 0008).
- The message contract in 0001 is frozen. If it genuinely must change, that is a
  conversation with both teammates, not a commit.
- Host clients connect to Kafka on `localhost:29092`. Containers connect on
  `kafka:9092`. Nearly every "cannot connect" problem is this.
- **US markets are open 16:30–23:00 Israel time.** Outside that window the
  Finnhub trade socket connects successfully and delivers nothing. Every test and
  every demo rehearsal for ticket 0008 must be runnable against a crypto symbol
  so it does not depend on the hour. Check the presentation slot against this
  window early, not the night before.
