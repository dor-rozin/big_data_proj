---
id: 0009
title: README, demo runbook, and cold-start verification
status: todo
layer: documentation
priority: P1
depends_on: [0002, 0003, 0004, 0007]
---

## Goal
Two things get graded that are not code: whether a stranger can run the project,
and whether the live demo works. Both fail for the same reason — something that
worked on the author's machine depended on state nobody wrote down. A container
that was already running, a topic created by hand three days ago, an environment
variable set in one shell.

This ticket makes the cold-start path explicit and *proves* it by running it. It
also gives the team a rehearsed demo sequence rather than improvising in front of
the lecturer.

## Scope
- **`README.md`** covering, in this order:
  - One-paragraph description of what the whole pipeline does, with the
    architecture diagram (producers → Kafka → Spark → Elasticsearch → Kibana) and
    a sentence naming who owns which half.
  - **Prerequisites**: Docker Desktop with at least 8 GB allocated, Python 3.11,
    and the `SEC_IDENTITY` environment variable. State the memory requirement
    explicitly — Kafka plus Elasticsearch plus Kibana on a 4 GB allocation fails
    in confusing ways.
  - **Quickstart**: the shortest possible path from clone to visible data, using
    the mock producer so it needs no network and no SEC identity. Target: five
    commands.
  - **Full setup**: snapshot fetching (0005, 0006) then replay (0007).
  - **Verification commands**: how to check each layer independently — is the
    stack up, do the topics exist, are there messages, is the data in
    Elasticsearch. Include the raw `kafka-console-consumer` one-liner so anyone
    can inspect the stream without writing code.
  - **Troubleshooting**: the four failures that will actually happen — broker
    unreachable from the host (wrong port: 29092 from host, 9092 in-container),
    Elasticsearch exiting on startup (heap or vm.max_map_count), yfinance rate
    limiting (use replay mode), and `SEC_IDENTITY` unset.
  - **Design decisions**, brief, with reasoning: JSON over Avro, infinite
    retention, snapshot-and-replay over live fetching, security disabled locally,
    at-least-once delivery with consumer-side idempotency.
- **`docs/DEMO.md`** — a numbered runbook for the presentation:
  - The exact command sequence, copy-pasteable, with expected output after each
    step and rough timing.
  - Which speed flag to use (`--speed realtime --duration 300`) and when to start
    it relative to opening Kibana.
  - **The symbol-mode decision, written down against the actual presentation
    slot.** US markets are open 16:30–23:00 Israel time. If the slot falls inside
    that window, the live producer runs against equities; if outside, it runs
    against a crypto symbol. State the chosen mode and the exact command in the
    runbook rather than deciding on the day — the failure mode is silent, so
    nobody will notice the wrong choice until the dashboard sits empty.
  - A stated fallback for each risk: no internet (replay mode covers it), stack
    won't start (have it already running and pre-warmed), Kibana blank (know which
    index pattern and time range to select), live socket silent (switch to the
    crypto symbol, or fall back to replay).
  - A pre-demo checklist to run 30 minutes before: stack healthy, topics
    populated, dashboard loads, laptop on power, `docker compose down -v` *not*
    run recently.
- **`scripts/cold_start.sh`** — the whole path from nothing to data flowing:
  `docker compose down -v`, `up -d`, wait for health, create topics, run the mock
  producer, verify message counts. Exits non-zero if any step fails. This script
  *is* the executable form of the README quickstart, and it must be run and pass
  before this ticket is closed.

## Non-goals
- No API documentation, no docstring reference site, no architecture decision
  record format. A README and a runbook.
- No CI pipeline. `cold_start.sh` run manually before the demo is sufficient at
  this scale.
- No documentation of the consumer half's internals — link to their README.

## Acceptance criteria
- **The real test**: on a machine that has never run this project (or after
  `docker system prune -a` and a fresh clone), following the README quickstart
  literally, with no prior knowledge and no help, produces visible messages in
  kafka-ui. If any step required knowledge not in the README, the README is wrong.
- `bash scripts/cold_start.sh` exits 0 from a fully clean state and takes under
  five minutes.
- Every command block in the README has been executed as written, in order, on a
  clean machine. Commands that were "obviously fine" but never run are exactly the
  ones that fail during a demo.
- `docs/DEMO.md` has been rehearsed end to end at least once, and the timings in
  it are measured rather than estimated.
- A teammate from the consumer half can follow the README to get data flowing
  without asking the author a question.
- The troubleshooting section covers all four named failures with a concrete fix,
  not a description of the symptom.

## Files
- `README.md` (new)
- `docs/DEMO.md` (new)
- `scripts/cold_start.sh` (new)
- `.env.example` (ensure complete)

## References
Verification commands come from tickets 0002 (`verify_stack.sh`) and 0003
(`describe_topics.py`). The quickstart path uses ticket 0004's mock producer
specifically because it has no external dependencies.
