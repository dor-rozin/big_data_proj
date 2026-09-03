# Big Data & AI — Stock Anomaly Detection + LLM Analyst

End-to-end big data pipeline for the course project.

**Pipeline:** `snapshot files → Kafka → Spark (transform + MLlib anomalies) → LLM analyst → Elasticsearch → Streamlit`

- **Data (semi-structured + unstructured):** daily OHLCV price bars from yfinance *and* SEC filings from EDGAR — both the numeric facts and the narrative text — captured once into `historical_data/` and replayed into Kafka in two passes: a year of history bulk-loaded up front, then the following year streamed in slowly as simulated live traffic. Replaying a snapshot rather than fetching live means the demo works with no internet and produces identical bytes every run.
- **Course technologies:** Docker, Apache Kafka (KRaft mode), Apache Spark, Elasticsearch, Streamlit.
- **AI capability — two stages that depend on each other:** Spark MLlib **KMeans** flags trading days that sit far from an instrument's own normal behaviour, and those flagged days are then handed to an **LLM** (Groq or Gemini, switchable), which writes an analyst note and a buy/hold/sell recommendation. The model finds *where to look*; the LLM says *what it means*.
- **Insight:** which days each stock behaved abnormally, how those days line up with its filings, and what a careful reader would conclude from the two together.

The infrastructure runs entirely locally in Docker. The analyst stage calls a
hosted LLM, which needs a **free API key** — Groq or Gemini, switched with one
line of config. See [The AI capability](#the-ai-capability-explained). Set
`LLM_ENABLED=false` to run everything else without a key at all.

## Architecture

```
                 ┌─────────────┐ market.prices.v1 ┌──────────────────────┐
 historical_data/│  producer   │  sec.filings.v1  │        Kafka         │
 (parquet)  ───▶ │  (replay)   │ ───────────────▶ │   (KRaft, 1 node)    │
                 └─────────────┘    sec.text.v1   └──────────┬───────────┘
                                                             │  batch read
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │        Spark         │
                                                  │  1. tabular transform │
                                                  │  2. MLlib KMeans      │
                                                  │     anomaly flags     │
                                                  │  3. aggregate/ticker  │
                                                  └──────────┬───────────┘
                                                             │  10 rows
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │  LLM (Groq / Gemini) │
                                                  │  analyst note + JSON  │
                                                  │  recommendation       │
                                                  └──────────┬───────────┘
                                                             │  bulk upsert
                                                             ▼
                                                  ┌──────────────────────┐
                                                  │    Elasticsearch     │
                                                  │  stock_prices        │
                                                  │  stock_filings       │
                                                  │  stock_context       │
                                                  │  stock_analysis      │
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

> **Running it for the first time, or demoing it?** Use
> **[RUNBOOK.md](RUNBOOK.md)** instead of the quick-start below. It is the same
> sequence starting from a clean teardown, with the expected output and a
> validation command after every step.

## How to run

**First time on this machine:** `.env` is gitignored, so it does not come down
from git — create it and add your own free API key **before** running anything.
[RUNBOOK.md](RUNBOOK.md#first-time-on-this-machine--do-this-before-anything-else)
walks through it; the short version is step 1 below.

```bash
# 1. Create your local config, then paste in your own free API key.
#    Groq (recommended):   https://console.groq.com/keys      -> GROQ_API_KEY=gsk_...
#    Gemini (optional fallback): https://aistudio.google.com/apikey -> GEMINI_API_KEY=...
#
#    Get YOUR OWN key rather than reusing a teammate's — the free budgets are
#    metered per key. No key at all is fine too: stage 4 skips itself and
#    everything except stock_analysis is still produced.
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

# 5. Run the Spark pipeline: transform + anomaly detection + LLM analyst +
#    load into Elasticsearch. ~1 min on Groq. (First run also downloads the
#    Kafka connector jar.)
docker compose --profile jobs run --rm spark

# 6. Open the dashboard at http://localhost:8501
#    Analyst notes are written to ./llm_output/, and the exact prompts that
#    were sent to ./llm_output/_prompts/

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

With the current snapshot the split falls on **2025-09-03**: 3,435 events
backfilled, 2,598 streamed live.

This is one tool with a mode flag rather than two scripts for one reason: both
sides read the *same* boundary out of `.env`, so they are provably
complementary — no message is sent twice and none is skipped. Two separately
configured producers drift the moment somebody changes one and not the other.
If you change `BACKFILL_DAYS`, change it in `.env` and both jobs follow.

Within each job, price bars, filings, and press releases are merged into one
sequence ordered by **event time**, so a filing (or its text) lands between the
price bars that surround it chronologically. Replay speed changes *when*
messages are sent, never the timestamps inside them: `ts` and `filed_date` stay
at their original event times, and `ingested_at` records the real send time.

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

### A third, optional producer job: real live trades

`producer/live_producer.py` is a separate, optional producer that replaces
simulated "live" data with an actual Finnhub WebSocket trade feed, aggregated
into OHLCV bars on the same `market.prices.v1` topic and schema — a consumer
cannot tell it apart from `produce.py`'s output except by the `interval` field.
It does not replace `producer-live` above; run either or both.

```bash
# Needs FINNHUB_API_KEY in .env (free key: https://finnhub.io/register).
.venv/bin/python -m producer.live_producer --symbols AAPL,MSFT,NVDA --bar-interval 1m

# Market-hours-independent: crypto trades continuously, so this is the mode to
# lead with for a presentation outside 16:30-23:00 Israel time (US market hours).
.venv/bin/python -m producer.live_producer --symbols BINANCE:BTCUSDT,BINANCE:ETHUSDT --bar-interval 1m
```

Bar width is currently pinned to the existing schema-valid intervals
(`1m`/`5m`/`1h`/`1d`) — sub-minute bars would need a `market.prices.v1` schema
change, which is a frozen contract and a whole-team conversation, not done here.
Reconnects with exponential backoff and never exits on a dropped socket; Ctrl-C
flushes the in-flight bar and exits cleanly.

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

| Folder        | Stage                                          | Owner     |
|---------------|------------------------------------------------|-----------|
| `producer/`   | Ingest: snapshot replay → Kafka                | Person A  |
| `schemas/`    | Frozen Kafka message contract                  | Person A  |
| `scripts/`    | Operational helper scripts                     | Person A  |
| `spark/`      | Transform + MLlib anomalies + LLM analyst + ES load | Dor  |
| `dashboard/`  | Streamlit dashboard over the four indices      | Ohad  |

Each stage passes data by a defined schema, so the three parts can be built and
tested independently. The Spark half is split across small modules:

| File | Responsibility |
|---|---|
| `spark/schemas.py` | The three contract `StructType`s + the parse guard |
| `spark/transforms.py` | Kafka JSON → analysis-ready tables, and the per-ticker aggregate |
| `spark/anomaly.py` | MLlib KMeans anomaly detection |
| `spark/llm.py` | Gemini REST client: pacing, retries, response parsing |
| `spark/es_writer.py` | Streamed, batched, idempotent Elasticsearch load |
| `spark/prompts/analyst.md` | The analyst prompt — edit this, not the code |
| `spark/pipeline.py` | Orchestrates the five stages |

The dashboard half:

| File | Responsibility |
|---|---|
| `dashboard/es_client.py` | Reads Elasticsearch. No arithmetic, no plotting |
| `dashboard/kpis.py` | The seven fundamental KPIs. No Elasticsearch, no plotting |
| `dashboard/charts.py` | The KPI figures |
| `dashboard/indicators.py` | CCI / Stochastic / MACD maths, ported from the `infra` project |
| `dashboard/woodies_chart.py` | The four-row candles + CCI + Stoch + MACD figure |
| `dashboard/ai_analyst.py` | The fundamentals-based analyst (separate from the Spark one) |
| `dashboard/app.py` | Wiring and layout only |

### The "Refresh data" button

The sidebar has a button that re-runs the Spark batch job
(`docker compose --profile jobs run --rm spark`) without leaving the browser —
useful when a live producer has been streaming and you want the dashboard to
catch up without switching to a terminal. It works via Docker-out-of-Docker:
the dashboard container mounts the host's `/var/run/docker.sock` and the repo
at the same absolute path it lives at on the host (`${PWD}:${PWD}` in
`docker-compose.yml`), so `docker compose` run from inside the container talks
to the host daemon and launches Spark as a sibling container, resolving the
same relative volume paths a host terminal would. Spark's own re-run is a full
reprocess (no offset tracking), so the button always reflects everything
currently on the Kafka topics — it does not fetch new external data itself, it
syncs the batch layer to whatever the producer has already sent. Only works
when the dashboard is started via `docker compose up` (needs the socket mount
and `PROJECT_DIR` env var); a bare `streamlit run app.py` disables it.

The share-price chart carries a vertical crosshair. It follows the cursor
anywhere in the panel — not only along the line — and snaps to trading days,
reading out that day's date and close. Weekends and market holidays are
collapsed out of its time axis, on the same grid as the Woodies panel below,
so a date sits at the same horizontal position in both.

Under the share-price chart, **"See more details"** opens the four-row Woodies
view: candles, Woodies CCI with its trend-coloured histogram, Stochastic %K/%D
and MACD, sharing one time axis. All nine indicator periods are adjustable
there. It is collapsed by default and computes nothing until it is opened.

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

The text archive holds 1,347 press releases back to 2000, but the producer clips
to those on or after the first price bar — a release with no price bar to join
against is noise. 117 survive that clip on the current snapshot, 59 of them in
the backfill window.

`sec.text.v1` is where the project's unstructured data comes from now that
filings replaced news headlines. The text is the `EX-99.1` exhibit of an earnings
8-K, which is filed the day before the matching 10-Q — so it lands on the day the
stock actually reacts, and joins to a price anomaly by date.

All three topics are wired into the run steps above: `topic-init` creates them
and the producer writes to them. `scripts/fetch_historical_text.py` pulls the
`EX-99.1` press release from every 8-K back to each company's IPO (~1,300
messages across the 10-ticker universe) into
`historical_data/sec.text.v1.historical/`. Only the ones filed on or after the
first price bar are ever replayed into Kafka — a decade of press releases with
no price bar to join against would just be noise — so a normal run puts roughly
100-120 `sec.text.v1` messages on the topic, not 1,300. The full archive stays
on disk for anyone who wants a wider window later.

**Do not join `sec.text.v1` to `sec.filings.v1` on `accession_no`** — it matches
exactly zero rows, and it fails silently rather than erroring. The two topics
carry different documents (`8-K` press releases vs `10-K`/`10-Q`), so they never
share an accession number. Join on `cik` plus a `filed_date` window: ~61% of
earnings releases find a related filing within ±3 days, and press releases with
no match at all are normal, because most 8-Ks are not tied to a periodic filing.
Full numbers and the reasoning are in
[`schemas/README.md`](schemas/README.md#do-not-join-these-two-topics-on-accession_no).

**The Spark job reads all three topics against the frozen contract** and was
previously verified against a hand-loaded sample message; it has not yet been
re-run against the real producer output above.

## Configuration (`.env`)

Copy `.env.example` to `.env`. Every variable is documented inline in that file;
the ones you are most likely to change:

| Variable               | Meaning                                                              |
|------------------------|----------------------------------------------------------------------|
| `KAFKA_BOOTSTRAP`      | Broker address for clients **inside** the compose network (`kafka:9092`) |
| `KAFKA_BOOTSTRAP_HOST` | Broker address for clients on the **host** (`localhost:29092`)        |
| `PRICES_TOPIC`         | Price bar topic — `market.prices.v1`, frozen by the contract           |
| `FILINGS_TOPIC`        | Filings topic — `sec.filings.v1`, frozen by the contract               |
| `TEXT_TOPIC`           | Filing text topic — `sec.text.v1`, 8-K press releases                  |
| `TOPIC_PARTITIONS`     | Partitions per topic (3). Cannot be lowered after creation             |
| `TOPIC_RETENTION_MS`   | `-1` = keep forever, so replay-from-earliest always works              |
| `BACKFILL_DAYS`        | Width of the backfill window in days (365). **Read by both producer jobs** — this is the shared split |
| `REPLAY_SPLIT_AT`      | Hard override of the boundary as `YYYY-MM-DD`. Empty = derive from `BACKFILL_DAYS` |
| `REPLAY_SPEED`         | `instant` \| `realtime` \| a float multiplier. Empty = per-mode default |
| `REPLAY_DURATION`      | Wall-clock seconds the live stream is spread over (300)                |
| `REPLAY_LIMIT`         | Cap on messages produced (`0` = no cap)                                |
| `SNAPSHOT_DIR`         | Where the producer container finds the parquet files (`/snapshots`)    |
| `KMEANS_K`             | Clusters in the anomaly model (3)                                      |
| `ANOMALY_FRACTION`     | Fraction of bars flagged as anomalies, **per group** (`0.05`)          |
| `MIN_ROWS_PER_GROUP`   | Groups smaller than this are passed through unflagged (30)             |
| `LLM_PROVIDER`         | `groq` or `gemini`. Only the transport differs — everything else is shared |
| `GROQ_API_KEY`         | Free key from [Groq Console](https://console.groq.com/keys). `.env` only |
| `GROQ_MODEL`           | Model id (`llama-3.3-70b-versatile`)                                   |
| `GEMINI_API_KEY`       | Free key from [AI Studio](https://aistudio.google.com/apikey). `.env` only |
| `GEMINI_MODEL`         | Model id (`gemini-3.6-flash`)                                          |
| `LLM_ENABLED`          | `false` skips the analyst stage entirely — no key needed               |
| `LLM_MIN_INTERVAL_SECONDS` | Seconds between API calls. Empty = per-provider default (1s groq, 13s gemini) |
| `LLM_MAX_CALLS`        | Hard ceiling on API calls per run (50). Truncation is logged, never silent |
| `ANALYSIS_INDEX`       | Elasticsearch index for the LLM output (`stock_analysis`)              |
| `CONTEXT_INDEX`        | Index holding what the analyst was shown (`stock_context`)             |
| `DASHBOARD_YEARS`      | Fiscal years the seven charts start on (5). The sidebar slider overrides it per session |
| `DASHBOARD_PROMPT_PATH` | Prompt for the **dashboard's** analyst. Empty = `dashboard/prompts/analyst_fundamentals.md` |

## The AI capability, explained

Two stages inside the pipeline, where the second depends on the first — plus a
**third, independent analyst in the dashboard** that answers the same question
from different evidence. See [Two analysts, on purpose](#two-analysts-on-purpose).

### 1. Spark MLlib KMeans — finding *where to look*

1. Four features per bar: daily return, volume change, intraday range %, and
   10-day rolling volatility.
2. Features are z-scored **within each `(ticker, interval)` group**. This is what
   makes the result meaningful — on a global scale NVDA's volatility would
   define "normal" for the whole universe and JPM would never look unusual.
3. One KMeans (`k=3`) is fitted across every group at once. Fitting one model per
   ticker would be N Spark jobs and stops scaling; one fit over pre-scaled
   features is a single distributed job whatever the universe size.
4. Each bar's Euclidean distance to its assigned cluster centre becomes its
   `anomaly_score`. The top `ANOMALY_FRACTION` **within each group** are flagged,
   so every instrument contributes its own share rather than the volatile names
   crowding out the rest.

Every step is a known transformation, so any flag can be traced back to the
numbers that produced it.

### 2. The LLM — saying *what it means*

The pipeline then collapses everything to one row per `(ticker, interval)`:
price summary, the top-5 anomalies with their dates and returns, how many
anomalies landed within two days of a filing, the latest reported financials, and
filing text where available. That row goes to the LLM with the prompt in
[`spark/prompts/analyst.md`](spark/prompts/analyst.md), which returns a JSON
recommendation (`buy`/`hold`/`sell`, confidence, risks, signals) plus a prose note.

**Why the two stages need each other.** An LLM cannot scan thousands of price
bars or do reliable arithmetic over them, so without stage 1 the prompt would
carry a coarse average and produce a note that would read the same for any
company. KMeans does the scanning cheaply and deterministically, so the prompt
names *specific unusual days with specific numbers*. That is the difference
between a summariser and an analyst.

**Aggregating first is what makes it affordable.** Ten instruments means ten API
calls, not one per row.

### Seeing what the analyst was given

The aggregate is not thrown away after the call. Stage 3b persists it two ways,
**before** the API is involved and whether or not the API is called at all:

| Where | What |
|---|---|
| `stock_context` index | The aggregate as queryable fields, plus `context_json` — the literal blob embedded in the prompt |
| `llm_output/_prompts/TICKER.txt` | The exact prompt string, assembled by the same code path that sends it |

Both cost no API quota, which is the point: iterate on
[`spark/prompts/analyst.md`](spark/prompts/analyst.md) with `LLM_ENABLED=false`,
inspect the assembled prompts, and spend the daily quota only on a prompt you
have already checked.

```bash
# What the analyst saw for one ticker
curl -s "localhost:9200/stock_context/_search?pretty" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"ticker":"NVDA"}},"_source":{"excludes":["context_json"]}}'

# Which instruments had anomalies clustering near a filing date
curl -s "localhost:9200/stock_context/_search?pretty" -H 'Content-Type: application/json' \
  -d '{"_source":["ticker","anomaly_count","anomalies_near_filing"],
       "sort":[{"anomalies_near_filing":"desc"}]}'
```

### Two analysts, on purpose

The dashboard has its own analyst, and it is **not** a second view of the Spark
note. Both answer "buy, hold or sell?" and both return the identical JSON
contract, but they are grounded on different evidence and are allowed to
disagree — a disagreement is a finding, not a bug.

| | Spark's analyst | The dashboard's analyst |
|---|---|---|
| Code | `spark/llm.py`, stage 4 | `dashboard/ai_analyst.py` |
| Prompt | `spark/prompts/analyst.md` | `dashboard/prompts/analyst_fundamentals.md` |
| Grounded on | Price behaviour + the days KMeans flagged as unusual | The seven fundamentals metrics computed in `dashboard/kpis.py` |
| Runs | In batch, once per pipeline run, for every instrument | Interactively, one company at a time, when a button is clicked |
| Output | `stock_analysis` index + `llm_output/TICKER.md` | Rendered in the page; nothing is written back |

Both read `LLM_PROVIDER`, the matching key and `LLM_ENABLED`, so one switch moves
or disables both. The dashboard's ignores `LLM_FALLBACK_PROVIDERS`: it makes a
single call for a single company, so there is no mid-run point at which failing
over would help.

**The click is deliberate.** Streamlit re-runs the whole script on every widget
change, so an automatic call would fire a request each time the year slider moved
and drain a free tier of ~30 calls/day in minutes. The answer is cached against
the exact prompt, so re-reading it and interacting with the rest of the page cost
nothing.

**The reader can steer the emphasis, not the rules.** A single optional text field
is appended to the prompt as an emphasis note — "focus on leverage", "explain it
for a non-specialist", "weigh the share count trend most heavily". It arrives in
its own section, after the data, under a frame telling the model it governs
**wording and emphasis only**: it cannot change the seven metrics, the rules for
reading them, the JSON contract, or add information the model was not given.

That boundary is not caution for its own sake. The rules it sits beneath are what
stop a model reading `shares_reported` and announcing a stock split as a 900%
share issue, or reading a missing fact as a zero. An instruction able to displace
them would not produce a worse answer — it would produce a confidently wrong one.
A full prompt swap is still possible deliberately, via `DASHBOARD_PROMPT_PATH`.

**The prompt is always inspectable, key or no key** — the same principle as stage
3b above. Check the evidence and the assembled prompt without a browser, a
network call or any of the pipeline running:

```bash
python dashboard/verify_ai.py              # all 10 tickers: prompt size, gaps
python dashboard/verify_ai.py AAPL         # print the whole prompt
python dashboard/verify_ai.py AAPL --call  # ...and spend exactly one API call
python dashboard/verify_ai.py --focus "focus on leverage"   # with an emphasis note
```

### The provider chain

`LLM_PROVIDER` sets the primary and `LLM_FALLBACK_PROVIDERS` an ordered chain
after it. Only the transport differs between providers — the prompt, the context,
the JSON contract, the pacing and the retries are shared.

```
LLM_PROVIDER=groq
LLM_FALLBACK_PROVIDERS=gemini          # or: gemini,ollama
```

| | Groq | Gemini | Ollama (local) |
|---|---|---|---|
| Default model | `llama-3.3-70b-versatile` | `gemini-3.6-flash` | `llama3.2:3b` |
| Pacing / timeout | 1s / 90s | 13s / 90s | 0s / 600s |
| Limit | 100,000 tokens/day | ~5 req/min, ~30/day | none |

#### What makes it move to the next provider

Two classes of failure, and the distinction is the design:

**Provider retired for the whole run** — every remaining instrument skips it:

- **Budget exhausted** — a 429 asking for longer than `MAX_BACKOFF` (60s).
  Observed in practice as *"asked for a 1362s wait"*.
- **Auth rejected** — 401/403. A bad key fails identically for every row.
- **Unreachable** — connection refused or DNS failure. Ollama not started.
- **Two consecutive row failures** — not every exhausted provider announces
  itself with one long wait. Gemini's per-minute limit returns 25-46s delays,
  each under the ceiling, so without this rule every row retries and fails and
  the provider is never retired.

**Row-level — the provider stays in play**, only this instrument moves on:

- Malformed JSON from the model, a one-off 5xx, a timeout.

**Retries are for when there is nowhere else to go.** With a fallback still
available the client retries twice and hands the row over; when it is the last
resort it retries five times. Grinding through five attempts against a provider
that is already refusing produces the same outcome minutes later.

#### Every person needs their own key

`.env` is gitignored, so **your key never leaves your machine** — pulling this
repo gives a teammate `.env.example`, where the key fields are empty. Each person
gets their own free key; they take about a minute and there is no card involved.

Don't share one between the team. It isn't a secrecy concern for a course
project, it's a quota one: the free budgets are per key, and three people
rehearsing against a shared key exhaust it three times as fast.

**Running without a key is fully supported.** With no key configured for any
provider in the chain, stage 4 is skipped and everything else runs: `stock_prices`,
`stock_filings` and `stock_context` are all written, and the prompts are still
dumped to `llm_output/_prompts/` so the analyst context stays inspectable. Only
`stock_analysis` is absent.

```
Stage 4 - LLM analyst (groq -> gemini)
[llm] no API key set for any provider in the chain - skipping.
```

A key for *some* providers works too — one without a key is skipped and the
chain moves to the next.

#### Provenance

Every analysis records `provider_used` and `model_used`, in Elasticsearch and at
the top of each `.txt` report. A note written by a 3B local model and one written
by a 70B hosted model are not interchangeable, and the output should never leave
that ambiguous. Each run also prints a summary — `produced by: groq x9, gemini x1`.

#### The local fallback

`ollama` runs as a container, needs no key and has no quota, so the analyst stage
survives an exhausted budget or a dead network:

```bash
docker compose --profile local-llm up -d ollama
docker compose --profile local-llm exec ollama ollama pull llama3.2:3b
# then set LLM_FALLBACK_PROVIDERS=gemini,ollama
```

It is deliberately **not** the default. Docker on macOS cannot pass through the
GPU, so inference is CPU-bound at roughly 30-60s per instrument against Groq's
sub-second. It also needs ~4 GB of RAM on top of the rest of the stack — raise
Docker Desktop to 10-12 GB before enabling it. Insurance, not the everyday path.

### Choosing a provider to develop against

| | Groq | Gemini |
|---|---|---|
| Default model | `llama-3.3-70b-versatile` | `gemini-3.6-flash` |
| Free tier | **100,000 tokens/day** (~3 ten-instrument runs) | ~5 req/min **and** ~30 calls/day |
| Ten-instrument run | **~1 min** | ~2.5 min, and often quota-blocked |
| Default pacing | 1s | 13s |

**Groq is the one to develop against**, but both have a daily budget and both
will refuse you eventually. Groq meters **tokens**, not requests: 100,000/day
against ~35k per ten-instrument run, so about three runs. Gemini allows roughly
two.

That per-run cost tripled when ticket 0010 landed filing text for every
instrument — prompts went from ~4,300 to ~12,000 characters. `LLM_MAX_TEXT_CHARS`
(default 6,000) is the lever: halving it roughly halves the token cost, at the
price of giving the analyst less narrative to work with. Either way, iterate with `LLM_ENABLED=false` — the prompts are still
dumped, so the context stays inspectable — and spend the budget on runs that
matter.

When a daily budget is exhausted the provider asks for a wait measured in
minutes. The client refuses anything over 60 seconds and records those
instruments as failures instead, so the run finishes in its normal time with
partial results rather than stalling. Without that ceiling a single 606-second
`retry-after` will hang the job with no output at all.

Avoid Groq's `groq/compound*` models: they are agentic and can fetch external
data, which breaks the prompt's first rule that only the supplied data may be
used.

A run that hits a rate limit fails only the affected instruments. Their `.txt`
reports are removed rather than left stale, and no empty document is written
over a good earlier analysis.

### Keeping `recommendation` meaningful

The prompt separates two things models tend to conflate: `recommendation` is the
direction the evidence leans, and `confidence` is how much that direction can be
trusted. Without that rule stated explicitly, a model asked to analyse thin data
answers `"hold"` for everything — technically defensible, and useless as a field
you want to filter or chart on.

Making the separation explicit changed the output from 10/10 `hold` to a
7/2/1 buy-hold-sell spread on identical data, with the remaining `hold`s landing
on the two instruments whose signals genuinely conflict. `confidence` then does
the hedging: `low` wherever filing text is missing or facts are sparse.

**Honest limits.** A `k=3` KMeans over a few hundred bars finds large moves on
heavy volume; it is a feature extractor, not a market model. The generated notes
are a descriptive read of this dataset for a university project — not financial
advice — and the prompt requires the model to treat missing facts as unknown
rather than zero, and to lower its stated confidence when the data is thin.

## Data source & credit

Price data from Yahoo Finance via the open-source
[`yfinance`](https://github.com/ranaroussi/yfinance) library. Filing data and
text from the U.S. SEC's public [EDGAR](https://www.sec.gov/edgar) XBRL
company-facts and full-text search APIs. All three were captured once into
`historical_data/` by `scripts/fetch_historical_data.py`,
`scripts/fetch_historical_filings.py`, and `scripts/fetch_historical_text.py`;
the pipeline replays those files rather than calling any source at run time.

Analyst notes are generated by Google's
[Gemini API](https://ai.google.dev/) free tier. For educational use only — the
recommendations are model output over a course dataset, not investment advice.
