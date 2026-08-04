# Big Data & AI — Stock Anomaly Detection + News Sentiment

End-to-end big data pipeline for the course project.

**Pipeline:** `snapshot files → Kafka → Spark (transform + MLlib anomaly detection) → Elasticsearch → Streamlit`

- **Data (semi-structured + unstructured):** daily OHLCV price bars from yfinance *and* SEC filings from EDGAR, captured once into `historical_data/` and replayed into Kafka in two passes — a year of history bulk-loaded up front, then the following year streamed in slowly as simulated live traffic. Replaying a snapshot rather than fetching live means the demo works with no internet and produces identical bytes every run.
- **Course technologies:** Docker, Apache Kafka (KRaft mode), Apache Spark, Elasticsearch, Streamlit.
- **AI capability (Spark MLlib):** KMeans-based **anomaly detection** on engineered price features — days that sit far from every cluster centre are flagged as unusual. Filing text is additionally scored for **sentiment** (VADER).
- **Insight:** which days each stock behaved abnormally, and whether the tone of what the company filed lines up with those days.

Everything runs locally in Docker and is **free** — no paid APIs, no keys.

## Architecture

```
                 ┌─────────────┐ market.prices.v1 ┌──────────────────────┐
 historical_data/│  producer   │ ───────────────▶ │        Kafka         │
 (parquet)  ───▶ │  (replay)   │  sec.filings.v1  │   (KRaft, 1 node)    │
                 └─────────────┘ ───────────────▶ └──────────┬───────────┘
                                                             │  batch read
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │        Spark         │
                                                  │  features + KMeans    │
                                                  │  anomaly detection    │
                                                  │  + VADER sentiment    │
                                                  └──────────┬───────────┘
                                                             │  bulk load
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │    Elasticsearch     │
                                                  │  stock_prices /       │
                                                  │  stock_news indices   │
                                                  └──────────┬───────────┘
                                                             │  query
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │      Streamlit        │
                                                  │  charts + insights    │
                                                  │  localhost:8501       │
                                                  └──────────────────────┘
```

## Prerequisites

- **Docker Desktop** (Mac/Windows) or Docker Engine + Compose plugin (Linux).
- Give Docker ~8 GB RAM (Settings → Resources).

## How to run

```bash
# 1. Create the local config. Defaults work as-is.
cp .env.example .env

# 2. Start the stack. This brings up Kafka, Elasticsearch, kafka-ui and the
#    dashboard, and runs `topic-init` to create the three topics.
docker compose up -d
bash scripts/verify_stack.sh   # PASS/FAIL per service and per topic

# 3. Backfill: load a year of history into Kafka in one go. Exits when done.
docker compose run --rm producer

# 4. Live: stream the remaining year in slowly, as if it were arriving now.
docker compose --profile live up -d producer-live
docker compose logs -f producer-live

# 5. Run the Spark pipeline: transform + anomaly detection + load into Elasticsearch.
#    (First run downloads the Kafka connector jar — needs internet, ~1 min.)
docker compose run --rm spark

# 6. Open the dashboard at http://localhost:8501

# When finished:
docker compose down          # stop, keep data
docker compose down -v       # stop and wipe Kafka/Elasticsearch data
```

`producer`, `producer-live`, and `spark` sit behind compose profiles, so
`docker compose up` never fires them implicitly.

### Data persists across restarts — on purpose

Kafka and Elasticsearch write to named Docker volumes, so a plain
`docker compose down` followed by `up -d` picks up right where you left off:
same topics, same messages, same indices. This is deliberate (ticket 0002) — a
routine restart shouldn't silently erase a demo. `docker compose down -v` is
the explicit "actually wipe it" command.

Note this persistence is **per machine**, not shared: each person's
`docker compose up` creates their own local volume. Pulling this repo doesn't
give you anyone else's data — everyone backfills their own copy the first time
they run `docker compose run --rm producer`.

For a one-command clean slate:

```bash
bash scripts/reset_stack.sh          # wipe, restart, leave Kafka empty
bash scripts/reset_stack.sh --seed   # wipe, restart, and re-run the backfill
```

## The two producer jobs

Both jobs are the same program (`producer/produce.py`) and the same image,
differing only in `--mode`. The snapshot is divided at **one instant** — the
first price bar plus `BACKFILL_DAYS` (365) — and each mode takes one side:

| Job | Service | Mode | Window | Speed |
|---|---|---|---|---|
| Seed history | `producer` | `--mode backfill` | before the split | `instant` |
| Simulate live | `producer-live` | `--mode live` | at/after the split | `realtime`, over `REPLAY_DURATION` |

With the current snapshot the split falls on **2025-08-05**: 3,375 events
backfilled, 2,538 streamed live.

This is one tool with a mode flag rather than two scripts for one reason: both
sides read the *same* boundary out of `.env`, so they are provably
complementary — no message is sent twice and none is skipped. Two separately
configured producers drift the moment somebody changes one and not the other.
If you change `BACKFILL_DAYS`, change it in `.env` and both jobs follow.

Within each job, price bars and filings are merged into one sequence ordered by
**event time**, so a filing lands between the price bars that surround it
chronologically. Replay speed changes *when* messages are sent, never the
timestamps inside them: `ts` and `filed_date` stay at their original event
times, and `ingested_at` records the real send time.

```bash
# Slower/faster live stream (default 300s for the whole live year).
docker compose --profile live run --rm producer-live \
    python produce.py --mode live --duration 900

# Live stream at 500x the original event spacing instead of a fixed duration.
docker compose --profile live run --rm producer-live \
    python produce.py --mode live --speed 500

# Never run dry during a demo — restarts the live window on completion.
docker compose --profile live run --rm producer-live \
    python produce.py --mode live --loop

# Move the boundary (both jobs must agree — prefer editing .env).
docker compose run --rm producer python produce.py --mode backfill --backfill-days 540

# Everything in one pass, ignoring the split.
docker compose run --rm producer python produce.py --mode all

# Smoke test with no broker: load, merge, validate every message, send nothing.
docker compose run --rm producer python produce.py --mode backfill --dry-run --validate-all
```

Every message is validated against `schemas/*.schema.json` before the first byte
goes out; `--validate-all` checks all of them instead of one per topic.

### Checking that data landed

```bash
.venv/bin/python scripts/describe_topics.py   # partition counts + message counts
```

or browse the topics at [localhost:8080](http://localhost:8080) in kafka-ui.

Infrastructure ports: Elasticsearch at `localhost:9200`, kafka-ui at
`localhost:8080` (browse topics/messages), Kafka at `localhost:29092` for
host-run clients (`kafka:9092` for other containers on the compose network —
see the listener contract comment at the top of `docker-compose.yml`). Image
tags and the security tradeoffs behind this stack are recorded in
[`versions.md`](versions.md).

## Repository layout & team split

| Folder        | Stage                              | Owner     |
|---------------|------------------------------------|-----------|
| `producer/`   | Ingest: snapshot replay → Kafka    | Person A  |
| `spark/`      | Transform + MLlib anomaly detection| Person B  |
| `spark/` (ES load) + `dashboard/` | Load + results/dashboard | Person C |
| `schemas/`    | Frozen Kafka message contract      | Person A  |
| `scripts/`    | Operational helper scripts         | Person A  |

Each stage passes data by a defined schema (see `pipeline.py`), so the three
parts can be built and tested independently.

The Kafka message contract for the reworked producer stage — field tables,
nullability, and the UTC/uppercase-ticker/zero-padded-CIK rules — is frozen in
[`schemas/README.md`](schemas/README.md).

### Local development environment

The pipeline itself runs entirely in Docker and needs no local Python. A venv is
only for working on the producer half (schemas, scripts, producers) outside a
container:

```bash
uv venv --python 3.11 .venv          # or: python3.11 -m venv .venv
source .venv/bin/activate
uv pip install -r requirements-dev.txt   # or: pip install -r requirements-dev.txt

python scripts/validate_schemas.py   # verify it works
```

Python 3.11 matches the `python:3.11-slim` base image in `producer/Dockerfile`.
`requirements-dev.txt` deliberately excludes `spark/` and `dashboard/` deps —
those run in Docker and are owned by the other half of the team.

It defines three topics:

| topic | contents | structured? |
|---|---|---|
| `market.prices.v1` | OHLCV price bars | numeric |
| `sec.filings.v1` | 19 normalised financial facts per filing | numeric |
| `sec.text.v1` | 8-K earnings press releases, plain text | **unstructured** |

`sec.text.v1` is where the project's unstructured data comes from now that
filings replaced news headlines. The text is the `EX-99.1` exhibit of an earnings
8-K, which is filed the day before the matching 10-Q — so it lands on the day the
stock actually reacts, and joins to a price anomaly by date.

`market.prices.v1` and `sec.filings.v1` are wired into the run steps above:
`topic-init` creates them and the producer writes to them. `sec.text.v1` is
created but has no producer yet — that is ticket 0010. The Spark job's news
branch still reads the old `news` topic and needs migrating to `sec.text.v1`
before step 4 works end to end; see [`.claude/index.md`](.claude/index.md).

## Configuration (`.env`)

Copy `.env.example` to `.env`. Every variable is documented inline in that file;
the ones you are most likely to change:

| Variable               | Meaning                                                              |
|------------------------|----------------------------------------------------------------------|
| `KAFKA_BOOTSTRAP`      | Broker address for clients **inside** the compose network (`kafka:9092`) |
| `KAFKA_BOOTSTRAP_HOST` | Broker address for clients on the **host** (`localhost:29092`)        |
| `PRICES_TOPIC`         | Price bar topic — `market.prices.v1`, frozen by the contract           |
| `FILINGS_TOPIC`        | Filings topic — `sec.filings.v1`, frozen by the contract               |
| `TEXT_TOPIC`           | Filing text topic — `sec.text.v1`, no producer yet                     |
| `TOPIC_PARTITIONS`     | Partitions per topic (3). Cannot be lowered after creation             |
| `TOPIC_RETENTION_MS`   | `-1` = keep forever, so replay-from-earliest always works              |
| `BACKFILL_DAYS`        | Width of the backfill window in days (365). **Read by both producer jobs** — this is the shared split |
| `REPLAY_SPLIT_AT`      | Hard override of the boundary as `YYYY-MM-DD`. Empty = derive from `BACKFILL_DAYS` |
| `REPLAY_SPEED`         | `instant` \| `realtime` \| a float multiplier. Empty = per-mode default |
| `REPLAY_DURATION`      | Wall-clock seconds the live stream is spread over (300)                |
| `REPLAY_LIMIT`         | Cap on messages produced (`0` = no cap)                                |
| `SNAPSHOT_DIR`         | Where the producer container finds the parquet files (`/snapshots`)    |
| `ANOMALY_FRACTION`     | Fraction of days flagged as anomalies (e.g. `0.05`)                    |

## The AI capability, explained

We use **Spark MLlib KMeans** as an unsupervised anomaly detector:

1. Per ticker we engineer four features: daily return, volume change, intraday
   range %, and 10-day rolling volatility.
2. Features are standardised (`StandardScaler`) so no single one dominates.
3. KMeans (k=3) learns clusters of "normal" trading behaviour.
4. For each day we compute the Euclidean distance to its assigned cluster
   centre. Days in the top `ANOMALY_FRACTION` by distance are flagged as
   anomalies — they don't fit any normal regime.

This is fully explainable (every step is a known transformation) and free.

## Data source & credit

Price data from Yahoo Finance via the open-source
[`yfinance`](https://github.com/ranaroussi/yfinance) library. Filing data from
the U.S. SEC's public [EDGAR](https://www.sec.gov/edgar) XBRL company-facts API.
Both were captured once into `historical_data/` by
`scripts/fetch_historical_data.py` and `scripts/fetch_historical_filings.py`;
the pipeline replays those files rather than calling either source at run time.
For educational use only.
