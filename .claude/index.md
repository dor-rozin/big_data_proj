# Index — Current State & Next Up

Quick-glance dashboard for the ticket backlog in this folder. The source of
truth for ticket details is each `NNNN-*.md` file's frontmatter (`status:`,
`depends_on:`); update **both** this file and the ticket's frontmatter when a
status changes.

## Status

| id | title | status | depends on |
|---|---|---|---|
| [0001](0001-freeze-message-schemas.md) | Freeze Kafka message schemas + sample fixtures | done | — |
| [0002](0002-docker-compose-infrastructure.md) | Docker Compose — Kafka (KRaft), Elasticsearch, Kibana | todo | — |
| [0003](0003-topic-provisioning.md) | Topic provisioning | todo | 0002 |
| [0004](0004-mock-producer.md) | Mock producer (synthetic, schema-valid) | todo | 0001, 0003 |
| [0005](0005-yfinance-snapshot.md) | yfinance snapshot to disk | todo | 0001 |
| [0006](0006-edgar-snapshot.md) | EDGAR XBRL snapshot to disk | todo | 0001, 0005 |
| [0007](0007-replay-producer.md) | Replay producer (snapshot → Kafka) | todo | 0003–0006 |
| [0008](0008-live-producer-finnhub-websocket.md) | Live producer — Finnhub WebSocket | todo | 0001, 0003, 0004, 0007 |
| [0009](0009-readme-and-demo-runbook.md) | README + demo runbook + cold-start check | todo | 0002–0004, 0007 |
| [0010](0010-filing-text-producer.md) | Filing text — 8-K press releases → `sec.text.v1` | todo | 0001, 0003, 0006 |

**Legend:** `todo` → `in-progress` → `done` (mirror whatever value is in the
ticket's own `status:` frontmatter field).

## Note on existing code

`producer/produce.py` already exists on `main` (merged before this ticket
backlog was written) and does an early version of yfinance-style ingestion, but
none of the tickets above are marked `done` yet — their acceptance criteria
(frozen schema fixtures, topic provisioning, etc.) haven't been verified
against it. Treat `producer/` as a starting point to reconcile against 0001/0005,
not as evidence that those tickets are complete. See [so_far.md](../so_far.md)
for the whole-repo (producer + spark + dashboard) status table.

## What's up next

Per the suggested schedule in [README.md](README.md#order-of-work):

**0001 is done** — the contract is frozen in
[`schemas/README.md`](../schemas/README.md) and both teammates can write their
`from_json` schema and index template against it now.

1. **0002** next — the only remaining unblocked P0, and it blocks 0003/0007/0009.
2. **0004** after it — unblocks teammates with real bytes to consume, no external
   deps.
3. Then **0003**, **0005**, **0006**, **0007** in dependency order.
4. **0010** straight after 0006 — it extends that same snapshot, so doing it
   while the EDGAR code is fresh is cheaper than coming back to it. It is also
   the only ticket that produces unstructured data.
5. **0009** (demo runbook) before **0008** (live WebSocket) in week 2 — 0008 is
   cuttable if time is tight.

## How to keep this file honest

When a ticket's status changes:
1. Update the `status:` field in that ticket's frontmatter.
2. Update its row in the table above.
3. If it unblocks a next step, update "What's up next".
