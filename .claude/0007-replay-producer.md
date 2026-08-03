---
id: 0007
title: Replay producer — stream the snapshot files into Kafka at controllable speed
status: todo
layer: producers
priority: P0
depends_on: [0003, 0004, 0005, 0006]
---

## Goal
This is the centerpiece of this half of the project. It turns the static snapshot
files from tickets 0005 and 0006 into a controllable Kafka stream: instant for a
teammate iterating on a Spark job, real-time for a demo that visibly moves, 100x
for generating volume quickly. It has no network dependency beyond Kafka, so it
works identically on a laptop with no internet, in a classroom with bad wifi, and
during the presentation.

The speed flag is not a nicety. Your teammates will run this dozens of times a day
and will want `instant`; the demo needs `realtime`. Building both into one tool
means one thing to learn and one thing to debug.

## Scope
- **`producers/replay_producer.py`**, a CLI:

  ```
  python -m producers.replay_producer \
      --prices data/raw/prices.parquet \
      --filings data/raw/filings.parquet \
      --speed instant|realtime|<multiplier> \
      --loop \
      --limit 5000 \
      --bootstrap localhost:29092
  ```
- **Speed semantics**, documented in `--help`:
  - `instant` — no delay, produce as fast as the broker accepts. Default.
  - `realtime` — compress the full historical span into `--duration` wall-clock
    seconds (default 300), preserving the *relative* spacing between events. Two
    years of daily bars replayed over five minutes should look like a smoothly
    advancing time series in Kibana, not a burst then silence.
  - `<multiplier>` — a float, e.g. `100` meaning 100x faster than the original
    event spacing.
- **Interleave the two streams by event time.** Merge price rows and filing rows
  into one time-ordered sequence before producing, so a filing lands between the
  price bars that surround it chronologically. If you emit all prices and then all
  filings, the consumer's stream-static join has nothing sensible to do and the
  dashboard's time axis looks wrong. This ordering is the single most valuable
  behavior in the ticket.
- **Preserve original event time in `ts` / `filed_date`.** Replay speed changes
  *when messages are sent*, never the timestamps inside them. Set `ingested_at` to
  the actual send time so the difference between the two is visible and
  explainable.
- **Correct keys and headers**: key by `ticker` / `cik` as per ticket 0001. Set a
  Kafka message header `schema_version: 1` on every message.
- **Reuse `producers/common.py`** from ticket 0004 for the producer factory,
  delivery callback, and summary. Do not duplicate that logic.
- **Backpressure handling.** In `instant` mode the local producer queue will fill;
  `producer.produce()` raises `BufferError` when it does. Catch it, call
  `producer.poll(0.5)` to drain, and retry the same message. A naive loop drops
  messages here and the loss is silent.
- **`--loop`** restarts from the beginning on completion, with `ingested_at`
  advancing, so the demo can run unattended.
- **Progress output**: a single updating line with messages sent, rate, elapsed,
  and estimated remaining. Not one log line per message.
- **`--limit N`** caps total messages, for quick smoke tests.
- **Graceful shutdown**: SIGINT flushes, prints the summary, exits 0.

## Non-goals
- No reading from Kafka, no consumer logic, no offset management. Produce only.
- No transactional or exactly-once semantics. At-least-once with an idempotent
  document ID on the consumer side is the agreed team design; note this in the
  README as a deliberate choice with the reasoning.
- No live fetching. If the snapshot files are missing, exit with a message telling
  the operator to run tickets 0005/0006 first — do not silently fall back to the
  network.
- No dynamic ticker filtering. Replay what is in the file.

## Acceptance criteria
- `--speed instant --limit 1000` delivers exactly 1000 messages, reports 0 failed,
  and completes in under 15 seconds.
- `--speed realtime --duration 60` takes 60 seconds (±10%) and the messages arrive
  spread across that window, not bunched — verify by recording arrival timestamps
  from a test consumer and checking the distribution.
- **Interleaving is correct**: consume everything produced by a full run, and
  assert the sequence of `ts` / `filed_date` values is non-decreasing across the
  merged stream. This is the acceptance criterion most likely to catch a bad
  implementation.
- Every produced message validates against its schema and carries the
  `schema_version` header.
- All messages for a given ticker land on a single partition.
- A run with the producer queue artificially shrunk (set `queue.buffering.max.messages`
  low) still delivers every message — proves the `BufferError` retry path works.
- Ctrl-C mid-run flushes and prints a summary; delivered count matches what a
  consumer actually reads from the topics.
- Missing snapshot file produces a clear, actionable error, not a traceback.
- Two full runs against a cleaned broker produce identical message payloads except
  `ingested_at`.

## Files
- `producers/replay_producer.py` (new)
- `producers/common.py` (extend)
- `tests/test_replay_producer.py` (new)
- `Makefile` (add `make replay` and `make demo` targets)

## References
Consumes the outputs of tickets 0005 and 0006. Message contract from ticket 0001.
Topic config from ticket 0003. Shared producer utilities from ticket 0004.
