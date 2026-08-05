---
id: 0008
title: Live producer — Finnhub WebSocket trades aggregated into bars
status: todo
layer: producers
priority: P2
depends_on: [0001, 0003, 0004, 0007]
supersedes: 0008 (yfinance/EDGAR polling variant)
---

## Goal
The replay producer (0007) makes the demo reliable; this makes it *live*. A
WebSocket streaming real trades into the same Kafka topic, appearing in Kibana
within seconds, is a materially stronger demonstration than replayed history —
and it costs one new producer rather than any change to the pipeline downstream.

The design constraint that shapes everything here: **the demo must work at any
hour**. Finnhub's trade socket emits only when trades actually happen, and the US
market is open 16:30–23:00 Israel time. A morning presentation slot against an
equities symbol produces a successful connection, no errors, and complete silence.
Crypto symbols trade continuously and are available on the same free socket, so
the producer must treat symbol choice as configuration rather than something baked
into the code.

## Prerequisite contract change
Ticket 0001's `interval` enum is currently `1d | 1h | 5m | 1m` and does not admit
sub-minute bars. **Amend `market.prices.v1.schema.json` to add `10s` and `30s`
before starting this ticket**, regenerate nothing else, and tell both teammates in
writing — the consumer's Spark `from_json` schema and any interval filtering in
Kibana need to know. This is a widening change (no existing message becomes
invalid), which is why it is acceptable at all; a narrowing change would require a
new topic.

## Scope
- **`producers/live_producer.py`**, a CLI:

  ```
  python -m producers.live_producer \
      --symbols AAPL,MSFT,NVDA,TSLA,SPY \
      --bar-interval 10 \
      --bootstrap localhost:29092
  ```

  and the market-hours-independent form:

  ```
  python -m producers.live_producer \
      --symbols BINANCE:BTCUSDT,BINANCE:ETHUSDT \
      --bar-interval 10
  ```
- **Connection**: `wss://ws.finnhub.io?token=<FINNHUB_API_KEY>`. Read the key from
  the `FINNHUB_API_KEY` environment variable and **exit non-zero with an
  actionable message before opening a socket if it is unset**. On connect, send one
  `{"type":"subscribe","symbol":"<sym>"}` frame per symbol.
- **Enforce the free-tier symbol cap.** The free plan allows 50 symbols on the
  socket. If `--symbols` exceeds 50, fail immediately with a clear message rather
  than silently having later subscriptions ignored.
- **Inbound message shape** (documented in a comment, because it is easy to get
  wrong from memory): frames arrive as `{"type":"trade","data":[{...}]}` where each
  element has `s` (symbol), `p` (last price), `t` (UNIX **milliseconds**), `v`
  (volume), `c` (trade conditions). Also handle `{"type":"ping"}` frames — respond
  or ignore per the library's keepalive, but do not treat them as trades.
- **Trade-to-bar aggregation, the core of this ticket.** Maintain a tumbling
  window of `--bar-interval` seconds per symbol, keyed on the *trade's* timestamp,
  not arrival time:
  - `ts` = window start, ISO 8601 UTC with `Z`
  - `open` = price of the earliest trade in the window (by `t`, not arrival order —
    trades can arrive slightly out of order)
  - `close` = price of the latest trade in the window
  - `high` / `low` = max / min price in the window
  - `volume` = sum of `v` across the window
  - `interval` = the bar interval as a schema-valid string (`10s`, `30s`, `1m`)
  - `ingested_at` = actual emit time
  - Emit to **`market.prices.v1`**, keyed by `ticker`, exactly as the replay
    producer does. Nothing downstream changes.
- **Windows with no trades emit nothing.** Do not emit zero-volume placeholder
  bars; they would corrupt any downstream average. Document this choice in the
  module docstring.
- **Symbol normalization.** Crypto symbols arrive as `BINANCE:BTCUSDT`. The schema
  requires uppercase with dots only, so map the symbol to a schema-valid ticker
  (e.g. `BTCUSDT`) and keep the raw symbol out of the message. Maintain the mapping
  in one place.
- **Reconnect with backoff.** WebSockets drop routinely — mid-demo is exactly when
  it will happen. On disconnect, reconnect with exponential backoff (1s, 2s, 4s,
  8s, capped at 30s), re-subscribe to all symbols, and log each attempt. The
  process must never exit because of a dropped connection. Flush any complete
  in-flight window before reconnecting.
- **Reuse `producers/common.py`** for the producer factory, delivery callback,
  ISO-8601-UTC helper, and summary printer. Reuse the `BufferError` retry path from
  ticket 0007.
- **Status line**, updated once per bar interval: connection state, symbols
  subscribed, trades received this window, bars emitted cumulative, last bar
  timestamp. This doubles as the thing you point at during the demo to show data is
  live.
- **`--duration N`** flag to exit cleanly after N seconds, for tests and for a
  bounded demo run.

## Non-goals
- **No historical backfill.** yfinance (ticket 0005) owns history; this owns *now*.
  Do not add Finnhub REST candle calls.
- No replacement of the replay producer. Both ship; the README states which one
  the demo leads with.
- No persistence of raw trades. Bars are the output; individual trades are
  in-memory only and discarded after their window closes.
- No gap-filling or recovery of trades missed during a disconnect. A reconnect
  resumes from live; the gap is real and acceptable.
- No quote (bid/ask) or order book data.
- No simultaneous equities-and-crypto run. One mode per process; run two processes
  if you genuinely want both.

## Acceptance criteria
- **Market-hours independence (the one that matters):** running with
  `--symbols BINANCE:BTCUSDT --bar-interval 10 --duration 60` at *any* hour,
  including a weekend morning, produces at least three bars on
  `market.prices.v1`. This test must pass when the US market is closed — that is
  its entire purpose.
- Missing `FINNHUB_API_KEY` exits non-zero with a message naming the variable,
  before any network activity.
- Passing 51 symbols exits non-zero with a message naming the free-tier cap.
- **Bar invariants hold for every emitted bar** (assert over a recorded trade
  fixture of at least 500 trades): `low <= min(open, close)`,
  `high >= max(open, close)`, `volume == sum of window trade volumes`, and `open`
  and `close` correspond to the earliest and latest trades *by timestamp*, not by
  arrival order. Include a fixture case with deliberately out-of-order arrivals.
- **Window boundary correctness**: a trade whose timestamp falls exactly on a
  window boundary lands in the later window, consistently, proven by a unit test
  with a hand-constructed trade sequence.
- Every emitted bar validates against the amended
  `market.prices.v1.schema.json`, including the new `interval` value.
- **Source indistinguishability**: a consumer reading `market.prices.v1` cannot
  tell whether a given bar came from the replay producer or this one, other than
  by the `interval` field. Verify by consuming from both and diffing the key sets.
- **Reconnect**: forcibly closing the socket mid-run (kill the connection in a
  test, or pull the network for 10 seconds manually) results in reconnection,
  re-subscription, and resumed bar emission — with no process exit and no
  traceback reaching the terminal.
- Ctrl-C flushes the current complete window, prints the summary, and exits 0.
- A 30-minute run does not grow memory unboundedly — the per-symbol trade buffer
  must be cleared each window, not appended to.

## Files
- `producers/live_producer.py` (new — replaces the polling variant entirely)
- `producers/common.py` (extend)
- `schemas/market.prices.v1.schema.json` (amend `interval` enum)
- `tests/test_live_producer.py` (new — include a recorded trade fixture so bar
  aggregation is testable without a network connection)
- `tests/fixtures/finnhub_trades.json` (new)
- `.env.example` (add `FINNHUB_API_KEY`)
- `requirements.txt` (add `websocket-client` or `websockets`)
- `versions.md`, `README.md`, `docs/DEMO.md` (document both symbol modes and the
  market-hours constraint explicitly)

## References
Message contract and the `interval` enum from ticket 0001. Producer utilities and
`BufferError` handling from tickets 0004 and 0007. The market-hours constraint is
the reason ticket 0009's demo runbook must name a symbol mode per presentation
slot rather than hardcoding one.
