---
id: 0004
title: Mock producer — synthetic, schema-valid messages with zero external deps
status: todo
layer: producers
priority: P0
depends_on: [0001, 0003]
---

## Goal
Two teammates cannot start until messages exist on the topics. The real producers
depend on Yahoo Finance (unreliable, rate-limited) and SEC EDGAR (slow to pull a
full ticker universe). If the team waits for those, two people idle for three days.

This producer emits synthetic but *completely schema-valid* messages using nothing
but the standard library and a Kafka client. It exists to unblock, and it keeps
existing afterwards: it is the fixture the whole team develops against, the thing
that makes a demo work when the network does not, and the input to any test that
should not depend on live data.

## Scope
- **`producers/mock_producer.py`**, a CLI:

  ```
  python -m producers.mock_producer \
      --topic both|prices|filings \
      --tickers AAPL,MSFT,GOOGL,AMZN,NVDA \
      --count 500 \
      --rate 50 \
      --bootstrap localhost:29092
  ```
  - `--count` total messages per topic (`0` = run until interrupted)
  - `--rate` messages per second (`0` = as fast as possible)
- **Realistic synthetic prices.** Do not emit random numbers. Seed each ticker at
  a plausible price and walk it with a small random step, then derive the bar:
  `low <= min(open, close)` and `high >= max(open, close)` must always hold. A
  consumer computing a moving average over pure noise produces a flat, useless
  dashboard; a random walk produces something that looks like a chart. Use a fixed
  `--seed` (default 42) so runs are reproducible.
- **Realistic synthetic filings.** One filing per ticker per quarter going back
  eight quarters, alternating `10-Q`/`10-K`, with `facts` values that grow
  plausibly quarter over quarter. Emit `null` for two or three facts at random so
  the consumer half is forced to handle nulls early rather than discovering them
  on real data the night before the demo.
- **Correct message keys**: `ticker` (UTF-8 bytes) for prices, `cik` for filings.
  Maintain a small hardcoded ticker→CIK map for the mock universe; zero-pad CIKs
  to 10 characters.
- **Timestamps**: `ts` and `ingested_at` are ISO 8601 UTC with a trailing `Z`.
  Generate with `datetime.now(timezone.utc)`, never `datetime.now()`.
- **Delivery confirmation.** Pass an `on_delivery` callback to
  `producer.produce()` that counts successes and logs failures with the message
  key. Call `producer.flush()` before exit and assert the delivered count equals
  the attempted count — `confluent-kafka` buffers asynchronously, and a script
  that exits without flushing silently drops its tail.
- **Self-validation**: on startup, generate one message of each type and validate
  it against the JSON Schema from ticket 0001. Fail loudly before sending anything
  if the producer has drifted from the contract.
- **Summary on exit**: messages attempted, delivered, failed, elapsed seconds,
  effective rate.

## Non-goals
- No yfinance, no edgartools, no network calls other than to Kafka. That is the
  entire point of this ticket.
- No pandas or numpy dependency — standard library `random` and `datetime` only.
  This keeps the mock producer runnable even when the real producers' dependencies
  are broken or mid-upgrade.
- No corporate actions, splits, holidays, or market-hours modeling.
- No attempt to make the numbers match reality for the real tickers used. They are
  synthetic and the README says so.

## Acceptance criteria
- With the stack up and topics created, `python -m producers.mock_producer --topic
  both --count 100` exits 0 and reports 100 delivered, 0 failed, for each topic.
- Every emitted message validates against its schema from ticket 0001 — prove it
  with a test that consumes what was produced and validates each message, not just
  by validating before send.
- `low <= min(open, close)` and `high >= max(open, close)` hold for every emitted
  price bar (assert in a test over at least 1000 generated bars).
- Two runs with the same `--seed` and `--count` produce byte-identical message
  payloads except for `ingested_at`.
- `--rate 10 --count 50` takes approximately 5 seconds (within 20%).
- Killing the process with Ctrl-C mid-run still flushes and prints the summary;
  no `KafkaError` traceback reaches the terminal.
- A teammate can run one command from the README and see messages in kafka-ui
  without reading the source.

## Files
- `producers/__init__.py` (new)
- `producers/mock_producer.py` (new)
- `producers/common.py` (new — shared: producer factory, delivery callback,
  ISO-8601-UTC helper, summary printer; reused by 0005–0007)
- `tests/test_mock_producer.py` (new)
- `requirements.txt` (add `jsonschema`)

## References
Schemas and samples from ticket 0001. `producers/common.py` created here is the
shared foundation for the snapshot and replay producers (0005, 0006, 0007).
