# Index — Current State & Next Up

Quick-glance dashboard for the ticket backlog in this folder. The source of
truth for ticket details is each `NNNN-*.md` file's frontmatter (`status:`,
`depends_on:`); update **both** this file and the ticket's frontmatter when a
status changes.

## Status

| id | title | status | depends on |
|---|---|---|---|
| [0001](0001-freeze-message-schemas.md) | Freeze Kafka message schemas + sample fixtures | done | — |
| [0002](0002-docker-compose-infrastructure.md) | Docker Compose — Kafka (KRaft), Elasticsearch, kafka-ui | done | — |
| [0003](0003-topic-provisioning.md) | Topic provisioning | in-progress | 0002 |
| [0004](0004-mock-producer.md) | Mock producer (synthetic, schema-valid) | todo | 0001, 0003 |
| [0005](0005-yfinance-snapshot.md) | yfinance snapshot to disk | todo | 0001 |
| [0006](0006-edgar-snapshot.md) | EDGAR XBRL snapshot to disk | todo | 0001, 0005 |
| [0007](0007-replay-producer.md) | Replay producer (snapshot → Kafka) | in-progress | 0003–0006 |
| [0008](0008-live-producer-finnhub-websocket.md) | Live producer — Finnhub WebSocket | in-progress | 0001, 0003, 0004, 0007 |
| [0009](0009-readme-and-demo-runbook.md) | README + demo runbook + cold-start check | todo | 0002–0004, 0007 |
| [0010](0010-filing-text-producer.md) | Filing text — 8-K press releases → `sec.text.v1` | in-progress | 0001, 0003, 0006 |
| [0011](0011-dagster-orchestration.md) | Dagster orchestration for the interval Spark run (stretch) | todo | 0007, 0009 |
| [0012](0012-text-filings-period-join.md) | Join `sec.text.v1` ↔ `sec.filings.v1` by fiscal period (contract change) | todo | 0001, 0010 |

**Legend:** `todo` → `in-progress` → `done` (mirror whatever value is in the
ticket's own `status:` frontmatter field).

## Note on existing code

`producer/produce.py` has been rewritten (2026-08-04) from the original
yfinance puller into a two-mode snapshot replay producer covering ticket 0007's
scope: `--mode backfill` sends everything before a shared split point (first
price bar + `BACKFILL_DAYS`, default 365) as fast as possible so the topics
look pre-populated; `--mode live` sends everything after it, paced to look like
data arriving now. Both modes read the same split from `.env`, so they are
exactly complementary. `producer/common.py` holds the shared client factory,
delivery accounting, and validation that 0004 also expects to use.

**0002 verified live and flipped to `done` (2026-08-04)**, once Docker was
available on this machine. Two real bugs were found and fixed in the process,
neither of which showed up in the offline (YAML-parse-only) checks done
earlier:

1. `apache/kafka:3.9.0`'s storage-format step validates every listener name in
   `KAFKA_LISTENERS` against `KAFKA_ADVERTISED_LISTENERS`, including
   `CONTROLLER` — even though the docs only describe this requirement for
   controller-only nodes, not combined broker+controller nodes. Without
   `CONTROLLER` also present in `KAFKA_ADVERTISED_LISTENERS`, the broker
   refused to start at all ("advertised.listeners cannot use the nonroutable
   meta-address 0.0.0.0"), crash-looping forever.
2. The image's default `log.dirs` (`/tmp/kraft-combined-logs`) did not match
   the `kafka-data` volume mount (`/var/lib/kafka/data`), so data was silently
   *not* surviving `docker compose down` even without `-v` — the opposite of
   ticket 0002's stated acceptance criterion. Fixed by setting
   `KAFKA_LOG_DIRS` explicitly.

Full acceptance-criteria checklist for 0002, all now verified against a live
broker: healthchecks green, `verify_stack.sh` all PASS, host
`kafka-console-producer`/`consumer` round trip on `localhost:29092`, in-network
reachability at `kafka:9092`, Elasticsearch yellow/green with no credentials,
`down` (no `-v`) preserving data across a restart, `down -v` actually wiping
it. See the 2026-08-04 entry in [so_far.md](../so_far.md) for the full log.

**0003 and 0007 stay `in-progress`**, not because anything failed, but because
their scope is bigger than what's been built:
- 0003 calls for a `Makefile`/`scripts/topics.sh` wrapper around create+describe
  — not written yet. Everything else (idempotent creation, drift detection
  reporting-not-fighting, per-ticker single-partition guarantee, `retention.ms`
  reading back as `-1`) was verified live and passes.
- 0007 specifies `producers/replay_producer.py` + `producers/common.py` + a
  `tests/` suite + `Makefile` targets; what exists lives at `producer/` instead
  (extending the existing directory rather than a new package) and has no
  automated tests. Verified live: exact delivery counts with 0 failures,
  realtime pacing evenly spread (not bunched), schema_version header present on
  every consumed message, missing-snapshot error is clean not a traceback.
  **Not yet verified**: `BufferError` retry under an artificially shrunk queue,
  Ctrl-C mid-run flush, and byte-identical payloads across two runs.

**0010 in-progress, not `done`**, for the same reason as 0003/0007: the file
layout deviates from the ticket spec. `scripts/fetch_historical_text.py` is a
standalone script (own EDGAR crawl of `8-K` filings, own rate limiting) rather
than an extension of `fetch_historical_filings.py` that reuses one crawl for
both facts and text — the ticket's "do not add a second fetch path" line is not
honored. `producer/produce.py` (not a new `producers/text_producer.py`) merges
`historical_data/sec.text.v1.historical/all.parquet` into the same timeline as
prices and filings, keyed by `cik`, same pattern as the filings path. Not yet
verified against a live broker end to end; the SEC round-trip acceptance
criterion (`®` and typographic quotes surviving Kafka) is untested.

**0008 moved to `in-progress` (2026-09-01), deliberately scoped down for the
one-week demo deadline.** `producer/live_producer.py` (a single file, not a
`producers/` package) does trade→bar aggregation, exponential-backoff
reconnect, Ctrl-C flush, the `FINNHUB_API_KEY`/50-symbol guardrails, and
crypto-symbol market-hours independence — verified offline against hand-built
trade sequences and the frozen schema, no live broker or real key yet. The
ticket's `10s`/`30s` schema widening was explicitly not done (frozen contract,
needs the team); bar interval is pinned to the existing `1m/5m/1h/1d` enum
instead. No `tests/` fixture suite, `docs/DEMO.md`, or `versions.md` updates —
out of scope for the MVP. See the 2026-09-01 entry in
[so_far.md](../so_far.md) for the full account.

## What's up next

Per the suggested schedule in [README.md](README.md#order-of-work):

**0001 and 0002 are done.**

1. **0003**: add the `Makefile`/`scripts/topics.sh` wrapper — the underlying
   scripts are already verified working.
2. **0007**: decide whether to reconcile the file layout with the ticket spec
   (`producers/` package) or update the ticket to match `producer/`, add a test
   suite, and verify the two remaining untested criteria (BufferError retry,
   Ctrl-C flush).
3. **0004** next — the mock producer is less urgent now that 0007 puts real
   bytes on the topics, but it is still what makes tests independent of the
   snapshot files.
4. Then **0005**, **0006** (reconciling the fetch scripts against the tickets).
4. **0010** straight after 0006 — it extends that same snapshot, so doing it
   while the EDGAR code is fresh is cheaper than coming back to it. It is also
   the only ticket that produces unstructured data.
5. **0009** (demo runbook) before **0008** (live WebSocket) in week 2 — 0008 is
   cuttable if time is tight.
6. **0011** (Dagster orchestration) is a backpocket stretch item — only pick it
   up once 0007 and 0009 are done and there's time left before the demo.
7. **0012** (period join) is not urgent — the contract is now *correct* about
   how the two SEC topics join, just not as precise as it could be. Needs Dor
   and Person C in the room before it starts, since it moves `spark/schemas.py`
   and an Elasticsearch mapping.

## How to keep this file honest

When a ticket's status changes:
1. Update the `status:` field in that ticket's frontmatter.
2. Update its row in the table above.
3. If it unblocks a next step, update "What's up next".
