# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

End-to-end big data pipeline for the BIU Big Data course:
`yfinance → Kafka → Spark (features + MLlib KMeans anomaly detection + VADER sentiment) → Elasticsearch → Streamlit`

See [README.md](README.md) for the architecture diagram and run instructions,
and [so_far.md](so_far.md) for who has done what.

| Folder      | Stage                                     |
|-------------|-------------------------------------------|
| `producer/` | Ingest: yfinance → Kafka                  |
| `spark/`    | Transform + anomaly detection + ES load   |
| `dashboard/`| Streamlit dashboard                       |

## Definition of Done

**Whenever someone says a piece of work is "done" / "finished" / "ready", run this
checklist before agreeing that it is done.** Do all four steps, in order, and
report which ones actually needed a change.

### 1. Update `README.md`

Check that the README still accurately describes what the program does and how to
run it. Update it if the change added or altered:

- a service, container, or pipeline stage
- a run/setup step or its ordering
- a `.env` variable (keep the config table in sync with `.env.example`)
- a port, topic name, or Elasticsearch index name

If it is already accurate, leave it alone and say so.

### 2. Update `so_far.md`

Append an entry to the log recording what was completed. Follow the format
already in that file: date, who/which area, what changed, and current status.
Keep the "Current status" table at the top in sync — this file is how the team
knows where everyone is standing, so it must reflect reality, not intent.

### 3. Run the tests

Run every test that exists for the areas listed as complete in `so_far.md`.

- Test commands live in the "How to test" section of `so_far.md`. If that section
  says there are no tests for an area yet, there is nothing to run for it — say so
  rather than inventing a command.
- Report failures with the actual output. Do not describe work as done while a
  test for it is failing.

### 4. Update `.gitignore`

Add anything newly generated that should not be committed — new data
directories, model artifacts, container volumes, caches, local credentials.
Do not commit `.env` (only `.env.example`).

## Conventions

- No paid APIs or API keys — everything must run locally and free via Docker.
- Config goes through `.env`; every new variable must also be added to
  `.env.example` with a comment.
- Each stage passes data by a defined schema (see `spark/pipeline.py`) so the
  three parts stay independently buildable and testable.
