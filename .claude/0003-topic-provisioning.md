---
id: 0003
title: Topic provisioning — create both topics with explicit, replayable config
status: todo
layer: infrastructure
priority: P0
depends_on: [0002]
---

## Goal
Kafka will happily auto-create a topic on first write, with one partition and
seven-day retention. Both of those defaults are wrong for this project. The
consumer half will wipe their Spark checkpoint and re-read from `earliest`
dozens of times over two weeks; with default retention, data written on Monday
is silently gone by the following Tuesday and the failure looks like a consumer
bug. Partition count cannot be reduced after creation, so getting it right up
front matters.

This ticket makes topic creation an explicit, idempotent, version-controlled step.

## Scope
- **`scripts/create_topics.py`** using `confluent_kafka.admin.AdminClient`.
  Creates both topics if absent, and if present, verifies their config matches
  and prints a warning listing any drift. Never deletes or recreates.
- **Topic configuration**, defined as a dict at the top of the script so it reads
  as declarative config rather than imperative calls:

  | topic | partitions | replication | `retention.ms` | key |
  |---|---|---|---|---|
  | `market.prices.v1` | 3 | 1 | `-1` (infinite) | `ticker` |
  | `sec.filings.v1` | 3 | 1 | `-1` (infinite) | `cik` |

  Also set `cleanup.policy=delete` explicitly on both (the default, but stating it
  documents that these are event logs, not compacted state).
- **Rationale comments in the script**, one line each: three partitions so the
  consumer can demonstrate parallel consumption; keying by `ticker`/`cik` so all
  events for one instrument land on one partition and are strictly ordered;
  infinite retention so replay-from-earliest always works.
- **`scripts/describe_topics.py`** — prints each topic's partition count, current
  config, and per-partition low/high watermark offsets. This is the tool anyone on
  the team runs when asking "did the data actually land". Make its output readable
  by a human at a glance, not a config dump.
- **`make topics` target** (or `scripts/topics.sh` if not using make) wrapping
  create + describe.

## Non-goals
- No topic deletion tooling. `docker compose down -v` is the reset mechanism and
  it is less error-prone than a delete script someone runs against the wrong
  broker.
- No log compaction. Tempting for the filings topic, but compaction plus a
  `cik` key would collapse a company's filing history to its latest filing, which
  destroys the time series the consumer half wants to compute growth rates over.
- No schema registry subject registration.
- No per-topic ACLs or quotas.

## Acceptance criteria
- Running `python scripts/create_topics.py` against a fresh broker creates both
  topics; running it a second time prints "already exists, config matches" for
  both and exits 0.
- Manually altering a topic's `retention.ms` and re-running the script produces a
  warning naming the topic, the setting, the expected value, and the actual value
   — and still exits 0 (it reports drift, it does not fight the operator).
- `python scripts/describe_topics.py` on an empty broker shows both topics with 3
  partitions each and all watermarks at 0.
- After writing 100 messages keyed by ticker, `describe_topics.py` shows the
  messages distributed across partitions, and every message for a given ticker
  appears in exactly one partition (verify by consuming with partition info).
- `retention.ms` reads back as `-1` from the broker, not the default `604800000`.

## Files
- `scripts/create_topics.py` (new)
- `scripts/describe_topics.py` (new)
- `Makefile` (new or amended)
- `requirements.txt` (add `confluent-kafka`)

## References
Topic names and keys are fixed by ticket 0001 (`schemas/README.md`). Ticket 0004
is the first thing to write to these topics.
