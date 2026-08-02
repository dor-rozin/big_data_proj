# So Far — Work Log

Running record of what is done, so everyone knows where the project stands.
Updated whenever someone declares a piece of work done (see [CLAUDE.md](CLAUDE.md)).

## Current status

| Area | Owner | Status | Notes |
|---|---|---|---|
| `producer/` — yfinance → Kafka | Person A | Code written, not verified | Not yet run end-to-end or tested |
| `spark/` — transform + KMeans anomaly detection | Person B | Code written, not verified | Not yet run end-to-end or tested |
| `spark/` — Elasticsearch load | Person C | Code written, not verified | Not yet run end-to-end or tested |
| `dashboard/` — Streamlit | Person C | Code written, not verified | Not yet run end-to-end or tested |
| Tests | — | None yet | No test framework in the repo |

**Legend:** `Not started` → `Code written, not verified` → `Runs end-to-end` → `Tested` → `Done`

## How to test

Step 3 of the Definition of Done reads this section. Add a row here whenever you
add tests for an area; if an area has no row, there is nothing to run for it.

| Area | Command |
|---|---|
| _(none yet)_ | No test framework is set up. No `tests/` directory and no test dependency in any `requirements.txt`. |

## Log

### 2026-08-02 — Baseline

Recorded the starting state of the repo. All four pipeline stages have code
written but none have been run end-to-end or tested.

- `producer/produce.py` — pulls price bars and news headlines from yfinance,
  publishes to the `prices` and `news` Kafka topics, with broker-connect retries.
- `spark/pipeline.py` — batch-reads both topics; engineers 4 price features
  (daily return, volume change, intraday range %, 10-day rolling volatility);
  StandardScaler → KMeans (k=3) → flags the top `ANOMALY_FRACTION` of days by
  distance to cluster centre; scores headlines with VADER; bulk-loads both
  datasets into Elasticsearch.
- `dashboard/app.py` — Streamlit UI on port 8501 reading from Elasticsearch.
- `docker-compose.yml` — Kafka (KRaft), Elasticsearch, and the three services.
- `.env.example` — full config template.

**Known gaps:**
- No tests anywhere.
- Pipeline is batch-only — Spark reads Kafka as a bounded batch, so the producer
  must finish before Spark starts. No streaming or refresh loop.
- Nothing is committed to git yet; all files are untracked in the working tree.
