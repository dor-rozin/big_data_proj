# So Far — Work Log

Running record of what is done, so everyone knows where the project stands.
Updated whenever someone declares a piece of work done (see [CLAUDE.md](CLAUDE.md)).

To actually run the thing end to end with validation at every step, use
[RUNBOOK.md](RUNBOOK.md).

## Current status

| Area | Owner | Status | Notes |
|---|---|---|---|
| `producer/` — snapshot replay → Kafka | Amir | Runs end-to-end | Backfill + live modes both verified against a live broker: exact delivery counts, correct partitioning, headers intact. Automated tests / `producers/` layout from ticket 0007 still outstanding |
| Infra — topic provisioning (ticket 0003) | Amir | Runs end-to-end | `create_topics.py` + `describe_topics.py` verified live: idempotent, drift detection, correct partition/retention config. Makefile wrapper from ticket scope not yet written |
| `spark/` — transform + KMeans + LLM analyst + ES load | Dor | Runs end-to-end | Verified live 2026-08-05 against a running stack: 2,500 bars + 875 filings + 1 text doc → 130 anomalies → 10 analyst notes → **4** ES indices (`stock_prices`, `stock_filings`, `stock_context`, `stock_analysis`). Re-run is idempotent. No automated tests yet |
| `dashboard/` — Streamlit | Person C | Not started | Reads `stock_prices` / `stock_filings` / `stock_context` / `stock_analysis`. Note `stock_news` no longer exists |
| `schemas/` — Kafka message contract (tickets 0001, 0010) | Amir | Done | 3 topics frozen, samples from real data, validator passing |
| `sec.text.v1` — unstructured text producer (ticket 0010) | Amir | Contract only | Schema + sample done; producer not written |
| Infra — Docker Compose (ticket 0002) | Amir | Done | Verified live 2026-08-04: all acceptance criteria pass. Two real bugs found and fixed — see log |
| Tests | — | Schema validation only | `scripts/validate_schemas.py`; no framework for the other areas |

**Legend:** `Not started` → `Code written, not verified` → `Runs end-to-end` → `Tested` → `Done`

## How to test

Step 3 of the Definition of Done reads this section. Add a row here whenever you
add tests for an area; if an area has no row, there is nothing to run for it.

| Area | Command |
|---|---|
| `schemas/` — message contract | `.venv/bin/python scripts/validate_schemas.py` (see README for venv setup) |
| `producer/` — contract conformance | `.venv/bin/python producer/produce.py --dry-run --validate-all` — loads both snapshots, merges them, validates every message against its schema, sends nothing. Needs no broker. |
| `producer/` — backfill/live split | `.venv/bin/python producer/produce.py --mode backfill --dry-run` then `--mode live --dry-run`. The two event counts must sum to the full timeline count (3,375 + 2,538 = 5,913 on the current snapshot). |
| `producer/` — live pacing | `time .venv/bin/python producer/produce.py --mode live --dry-run --duration 4` — should take ~4s with the progress line advancing evenly, not in a burst. |
| `scripts/verify_stack.sh` | `bash scripts/verify_stack.sh` — needs a running stack. Also `bash -n scripts/verify_stack.sh` for a syntax-only check. |
| Infra — full live smoke test | `docker compose up -d && bash scripts/verify_stack.sh && docker compose run --rm producer && .venv/bin/python scripts/describe_topics.py --bootstrap localhost:29092` — brings up the whole stack, provisions topics, backfills a year, and prints message counts. |
| `scripts/create_topics.py` — drift detection | Manually set a topic's `retention.ms` (`docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name <topic> --add-config retention.ms=604800000`), re-run `create_topics.py`, confirm it reports the drift and exits 0. |
| `scripts/reset_stack.sh` | `bash scripts/reset_stack.sh --seed` — confirm the volumes are actually recreated (`docker volume ls` before/after) and the re-seed delivers the same 3,375/3,375 with 0 failed. |
| `spark/` — full pipeline, no API key | `LLM_ENABLED=false docker compose --profile jobs run --rm spark` — runs stages 1, 2, 2b, 3 and 5. Needs a running stack with the backfill loaded; needs no Gemini key. |
| `spark/` — full pipeline with the analyst | `docker compose --profile jobs run --rm spark` — as above plus the LLM stage. ~1 min on Groq. Expect 10 reports in `./llm_output/` and a spread of recommendations, not 10 identical ones. |
| `spark/` — idempotency | Run the pipeline twice, then check the counts did not change: `curl -s localhost:9200/stock_prices/_count`. Deterministic `_id`s make the load an upsert, so a second run must leave 2,500 / 875 / 10, not double them. |
| `spark/` — analyst context capture | `docker compose --profile jobs run --rm -e LLM_ENABLED=false spark` then `curl -s localhost:9200/stock_context/_count` — must be 10, and `ls llm_output/_prompts/` must hold 10 files. Costs no API quota; this is the check to run while iterating on the prompt. |
| Full stack — clean end-to-end run | Follow [RUNBOOK.md](RUNBOOK.md) top to bottom. It states the expected output for every step, so a mismatch anywhere is the failure point. Ends with an idempotency check. |
| `spark/` — provider chain | `docker compose --profile jobs run --rm spark`. The banner shows the chain (`groq -> gemini`), and the closing `produced by:` line names which model wrote each analysis. Every row in `stock_analysis` must carry `provider_used`. |
| `spark/` — runs without a key | `docker compose --profile jobs run --rm -e GROQ_API_KEY= -e GEMINI_API_KEY= spark` — must exit 0, skip stage 4 with a clear message, and still write `stock_prices`, `stock_filings` and `stock_context`. This is the path every new teammate hits first. |
| `spark/` — fallback triggers | Force a retirement and confirm the run still finishes: `-e GROQ_API_KEY=bad` (auth → retire on row 1, all rows via the fallback), or `-e LLM_FALLBACK_PROVIDERS=ollama` without starting the container (unreachable → retire, no fallback left, rows recorded as failures). Neither may hang. |
| `spark/` — schema-drift guard | Rename a field in a `StructType` in `spark/schemas.py` (e.g. `ts` → `date`) and re-run. `assert_parsed` must fail naming the topic, rather than the job exiting 0 having produced nothing. |
| everything else | No test framework is set up. No `tests/` directory and no test dependency in `producer/`, `spark/`, or `dashboard/` requirements. |

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

### 2026-08-03 — Ticket 0001: Kafka message contract frozen (Amir, producer half)

Wrote the message contract that the Spark job's `from_json` schema and the
Elasticsearch index template are written against. No runtime code — this is the
contract only.

- `schemas/market.prices.v1.schema.json` — OHLCV price bar (draft 2020-12).
  OHLCV fields nullable, `interval` a fixed enum, `additionalProperties: false`.
- `schemas/sec.filings.v1.schema.json` — SEC filing with a **closed** 10-key
  `facts` set (missing facts are `null`, never omitted) so Elasticsearch dynamic
  mapping cannot blow past its 1000-field limit.
- `schemas/samples/*.json` — one realistic sample message per topic, one JSON
  object per line so they pipe straight into `kafka-console-producer`.
- `schemas/README.md` — per-topic field tables, the Kafka message key for each
  topic (`ticker` for prices, `cik` for filings), the three cross-cutting rules
  (UTC-with-`Z` timestamps, uppercase tickers, zero-padded CIKs), and the
  plain-JSON-over-Avro tradeoff.
- `scripts/validate_schemas.py` + `scripts/requirements.txt` — validator, exits
  non-zero naming the offending field.

**Verified:** validator exits 0 on both samples. Mutating a sample to drop the
`Z` from `ts`, lowercase a ticker, add an extra key inside `facts`, un-pad a
CIK, or omit a fact each makes it exit 1 naming that field.

**Status:** `schemas/` is Done. Ticket 0001 marked `done` in `.claude/index.md`.

**Note on scope:** these topics (`market.prices.v1`, `sec.filings.v1`) are the
*new* contract from the `.claude/` ticket backlog. The pipeline currently running
still uses the original `prices` / `news` topics — `producer/produce.py` has not
been reconciled against this contract yet (that is tickets 0004–0007).

### 2026-08-03 — Ticket 0001 revised after validating against real SEC data

Before freezing, the filings contract was checked against actual companyfacts
data for AAPL, MSFT, JPM, XOM, TSLA and WMT. It would have validated cleanly
while producing wrong or missing data in four ways, all now fixed. Full detail in
the "Amendments" section of `.claude/0001-freeze-message-schemas.md`.

- **Alias table instead of raw XBRL tags.** Apple stopped tagging `Revenues` in
  2018 (ASC 606). The original contract would have emitted `null` revenue for
  every modern filing of most large issuers. 5 of 19 facts resolve via a
  non-first alias for at least one company; `shares_outstanding` needs the `dei`
  taxonomy because the `us-gaap` tag is stale for Walmart.
- **`fiscal_period` + `period_start` added.** Annual and quarterly revenue were
  previously indistinguishable. `period_start` is not derivable — Apple's FY2023
  is a 370-day (53-week) fiscal year.
- **Fact set expanded 10 → 19**, still closed.
- **Duration-selection rule added to the contract.** JPM's Q2 10-Q reports
  `NetIncomeLoss` four times (current/prior year x quarter/YTD). Rule: keep only
  facts ending at `period_end`, then the window matching `fiscal_period` (365d
  ±31 FY, 91d ±20 quarterly), else `null`. Quarterly is the discrete quarter,
  never YTD.

**Known consequence:** `operating_cash_flow` and `capex` are null on most
quarterly filings, because cash-flow statements are tagged YTD-only and a
181-day window fails the 91-day tolerance. Deliberate — a YTD number in a field
labelled "discrete quarter" is worse than a null. Deriving it by subtracting the
prior YTD filing is a deferred improvement.

**Nulls are informative, not failures.** Only `revenue`, `net_income`, `assets`,
`equity`, `cash`, `eps_basic` and `eps_diluted` resolved for all six companies.
Banks have no gross profit or classified balance sheet; Walmart doesn't tag total
`Liabilities`. The dashboard must not treat a null as an error.

**Verified:** both filing samples are now extracted from live SEC data rather
than hand-written — Apple's FY2023 10-K (19/19 facts) and JPM's Q2 2024 10-Q
(9/19). `filed_date` on both was taken from the source, not typed. Validator
exits 0 on all three samples across the two topics.

### 2026-08-03 — Local dev venv for the producer half (Amir)

Added `requirements-dev.txt` and a `.venv` on **Python 3.11**, matching the
`python:3.11-slim` base image in `producer/Dockerfile`. Covers the producer half
only — schema validation, both Kafka clients, yfinance, edgartools, pytest.
`spark/` and `dashboard/` deps are deliberately excluded: they run in Docker and
belong to the other half of the team. Setup instructions are in the README.

Rebuilt from scratch and verified: all libraries import, `confluent_kafka.admin.
AdminClient` resolves (what ticket 0003 needs), a live yfinance fetch returns
rows, and the schema validator exits 0.

**Finding — `producer/produce.py` cannot fetch data as committed.**
`producer/requirements.txt` pins `yfinance==0.2.40`, and that version now returns
zero rows with "possibly delisted" for a plain `AAPL` 5-day history: Yahoo moved
its endpoints after that release. `yfinance==1.5.2` returns data, and that is
what the venv pins. **`producer/requirements.txt` was left unchanged** — 1.x is a
major bump from 0.2.x and `produce.py` needs reconciling against it, which is
ticket 0005's job, not a silent edit here. Until then the producer container is
non-functional regardless of Kafka being up. This is the first concrete reason
the producer's "Code written, not verified" status was accurate.

Python 3.14 (the homebrew default on this machine) was rejected: the pinned
`numpy==1.26.4` / `pandas==2.2.2` in `spark/requirements.txt` have no 3.14
wheels, and matching the container matters more than being current.

### 2026-08-03 — Third topic `sec.text.v1`: unstructured data restored (Amir)

**Problem it fixes:** moving from news headlines to SEC filings left the project
with no unstructured data at all — `market.prices.v1` and `sec.filings.v1` are
both entirely numeric. No ticket in the backlog covered text.

**Source chosen: the `EX-99.1` earnings press release attached to an 8-K.**
Verified against real filings:

- It is **dated to the day the stock moves** — Apple filed 8-K
  `0000320193-26-000018` on 2026-07-30 and the matching 10-Q on 2026-07-31. The
  join between text and a price anomaly is exact, not approximate.
- It is small and unambiguous: ~11,000 characters, one exhibit, no section
  parsing. The sample message is 12 KB.
- It is opinionated prose, which is the point: "records", "strongest June quarter
  ever", "up 16 percent year over year".
- It reuses ticket 0006's EDGAR fetch — no new source to make reliable.

**Design: a separate topic, not an extension of `sec.filings.v1`.** A facts
message is ~1 KB, a press release ~12 KB, a full 10-K 357 KB. Combining them
makes every small message pay for the large ones, and the two have different
consumers (numeric join vs NLP) and different Elasticsearch mappings (numerics vs
analyzed text). They join on `accession_no`. This also keeps the 0001 contract
untouched — the change is purely additive.

**Deliberately excluded — 10-K Risk Factors and MD&A.** Splitting a real Apple
10-K on `Item N` headings finds all ten headings within 1,300 characters of each
other: that is the table of contents, not the sections, and `Item 1A` matches
twice. A naive implementation extracts a table of contents and reports success.
edgartools' own section accessor also threw `ValueError` on that filing. Both are
reserved in the `section` enum so adding them later is not a re-freeze.

**Sentiment scoring is deliberately unspecified.** The producer's contract ends
at delivering clean text; how it is scored is the consumer half's decision.

- `schemas/sec.text.v1.schema.json` (new) — includes `chunk_index`/`chunk_total`
  from day one so splitting long sections later is not a contract change.
- `schemas/samples/sec.text.v1.json` (new) — real Apple Q3 FY2026 press release.
- `.claude/0010-filing-text-producer.md` (new) — the producer work. Depends on
  0001, 0003, 0006; slots straight after 0006 while the EDGAR code is fresh.

**Verified:** validator exits 0 on all four samples across the three topics.

**Status:** contract done, producer not written. `sec.text.v1` needs adding to
ticket 0003's topic list when that lands.

### 2026-08-03 — Ad hoc historical price snapshot (Amir)

Added `scripts/fetch_historical_data.py`, a standalone script (not part of the
ticket 0001–0010 backlog) that pulls 2 years of daily bars for 10 named large-cap
tickers (NVDA, AAPL, MSFT, AMZN, GOOGL, AVGO, META, TSLA, BRK.B, JPM) via
`yfinance.Ticker(...).history()` and writes one parquet file per ticker plus a
combined `all.parquet` to `historical_data/` (gitignored — already covered by the
existing `*.parquet` rule).

**Finding — `BRK.B` fails against Yahoo as-is.** Yahoo's endpoint expects the
dash form (`BRK-B`), not the dot form; the script maps `.` → `-` before the
fetch call but keeps the original dotted ticker in the output data.

**Conforms to the frozen `market.prices.v1` contract.** Reworked to emit exactly
the 10 contract fields (`schema_version`, `ticker`, `ts`, `open`, `high`, `low`,
`close`, `volume`, `interval`, `ingested_at`) instead of raw yfinance column
names — dropped `Dividends`/`Stock Splits`, converted timestamps to UTC with a
trailing `Z`, cast numpy scalars to native Python types, and replaced NaN with
`None`.

**Verified:** all 10 tickers returned 500 rows each (5,000 rows total, zero
NaNs). Every row of the combined parquet, round-tripped through
`json.dumps`/`json.loads`, validates against
`schemas/market.prices.v1.schema.json` via `jsonschema.Draft202012Validator`
with zero errors.

**Not related to ticket 0005** (`yfinance snapshot to disk`, still `todo`) —
that ticket covers the full 30–50 ticker universe with chunked retries/backoff,
`config/tickers.txt`, and a `.meta.json` sidecar for Kafka replay. This script
is a quick one-off pull for ad hoc analysis: same contract shape, but no
retry/backoff logic and a fixed 10-ticker list instead of a config file.

Output moved to `historical_data/market.prices.v1.historical/` — subfolder named
after the `market.prices.v1` Kafka topic with a `.historical` suffix, so it
reads clearly as the offline snapshot rather than live topic traffic.

### 2026-08-03 — Ad hoc EDGAR filings snapshot for the same 10 companies (Amir)

Added `scripts/fetch_historical_filings.py` (also standalone, not part of the
0001–0010 backlog), the `sec.filings.v1` counterpart to the price snapshot
above. Pulls every `10-K`/`10-Q` on file for the same 10 tickers via
`edgartools`' `Company(ticker).get_filings()` / `.get_facts()`, resolves the
closed 19-key fact set through the alias table and duration/instant selection
rules in `schemas/README.md` (never a raw XBRL tag lookup), and writes one
parquet per ticker plus `all.parquet` to
`historical_data/sec.filings.v1.historical/` (gitignored, same as the prices
folder).

Requires `SEC_IDENTITY` (name + email) in the environment — the script exits
with an explanatory message before any network call if it's unset, per the SEC
User-Agent requirement. Not yet added to `.env.example`/README since this is an
ad hoc script exported manually, not a container-driven step; revisit if this
becomes a normal part of setup.

**Verified against real filings, matching the coverage already documented for
ticket 0001:** Apple's FY2025 10-K resolves all 19 facts; JPMorgan's FY2025 10-K
resolves 9 of 19 (the bank-shaped nulls: `gross_profit`, `cost_of_revenue`,
`operating_income`, `assets_current`, `liabilities_current`); quarterly filings
correctly null out `operating_cash_flow`/`capex` (YTD-tagged, fails the
discrete-quarter tolerance). All 913 rows across the 10 tickers (33–131 filings
each depending on how far back EDGAR's structured facts go per company)
validate against `schemas/sec.filings.v1.schema.json` with zero errors and zero
NaN; largest serialized row is 855 bytes, well under the 8 KB ceiling ticket
0006 sets to prove no raw filing text leaked in.

**Gotcha worth flagging for any future consumer of these parquet files:**
reading them back with pandas ≥3.0's default Arrow-backed string dtype and
calling `.to_dict()`/`.iterrows()` turns a true null into a Python
`float('nan')`, not `None` — `pd.isna()` still says it's missing, but
`json.dumps` on it silently emits invalid literal `NaN`. The parquet files
themselves store correct Arrow nulls (verified directly with `pyarrow`); this
is a read-side pitfall, not a write-side one. Any code that serializes these
rows to JSON must explicitly replace NA with `None` first
(`df.astype(object).where(pd.notna(df), None)`), the same "kill NaN before it
becomes JSON" lesson ticket 0005 already documents for the price data.

**Not related to ticket 0006** (`EDGAR snapshot to disk`, still `todo`) — that
ticket covers the full ticker universe, a `SEC_IDENTITY`-gated CLI with
`--forms`/`--since`/`--limit` flags, a cached `cik_map.json`, and resilience/
rate-limit tests. This script is a fixed 10-ticker one-off with no CLI flags,
no CIK-map cache, and no automated test.

### 2026-08-04 — Bulk-load older year of prices into Kafka, hold back the rest (Amir)

Added `scripts/load_historical_prices_to_kafka.py` so the Spark side has real
`market.prices.v1` messages to build against before ticket 0007 (replay
producer) exists. Splits
`historical_data/market.prices.v1.historical/all.parquet` at the 1-year mark
(snapshot start + 365 days = 2025-08-05): the **older year** (2024-08-05 →
2025-08-04, 2,500 rows) is produced to the `market.prices.v1` topic now, in
bulk; the **newer year** (2025-08-05 → 2026-08-03, 2,500 rows) is written
instead to `historical_data/market.prices.v1.historical/remaining_for_replay.parquet`
and deliberately **not** sent — that's the slice the later replay producer
(ticket 0007) will stream in slowly, at a simulated pace, once it's written.

**Not run against a live broker yet** — this dev machine has no Docker/Kafka
available. Verified everything short of the actual send: the split is an even
2,500/2,500 (250 rows/ticker each side), and every one of the 2,500 "to load"
messages round-trips through `json.dumps` with zero `NaN` and validates clean
against `schemas/market.prices.v1.schema.json`. `--dry-run` prints the split
and writes the remainder file without touching Kafka, so this can be sanity
checked with no broker at all.

**Known caveat:** ticket 0003 (topic provisioning) is still `todo`. If this
script runs against a broker where `market.prices.v1` doesn't exist yet, Kafka
auto-creates it with the default 1 partition / 7-day retention, not the 3
partitions / infinite retention ticket 0003 specifies. Run topic provisioning
first once it exists; until then, whoever runs this should create the topic
with the right config manually or accept the defaults for a first smoke test.

**Not the replay producer.** No speed control, no interleaving with filings,
no `BufferError`-driven backoff beyond a bare retry loop, no `--loop`. This is
a one-off bulk load to unblock the consumer side; ticket 0007 is still the
real deliverable for the remaining year.

### 2026-08-04 — Ticket 0002: Docker Compose infra rewritten to spec (Amir)

Rewrote `docker-compose.yml`'s `kafka` and `elasticsearch` services and added
`kafka-ui` to match the ticket 0002 contract. **Kibana explicitly dropped from
scope at the user's request** — kafka-ui + the Streamlit dashboard already
cover "is the data there," so a fifth service and port bought nothing.

- **Kafka: `apache/kafka:3.8.0` → `3.9.0`.** Rebuilt the listener config from
  scratch onto the exact names/ports the ticket specifies —
  `PLAINTEXT://kafka:9092` (containers) / `PLAINTEXT_HOST://localhost:29092`
  (host) — replacing the previous ad hoc `PLAINTEXT`/`INTERNAL` naming, which
  had the port numbers backwards relative to this contract (host was 9092,
  container-internal was 29092). This flips every `KAFKA_BOOTSTRAP` consumer:
  `.env.example`, `producer/produce.py`, `spark/pipeline.py` (all now default
  to `kafka:9092`), and `scripts/load_historical_prices_to_kafka.py`'s host
  default (now `localhost:29092`). Added `KAFKA_INTER_BROKER_LISTENER_NAME` and
  the three replication-factor-1 settings the ticket calls out as required for
  single-node internal topic creation — the old compose file was already
  setting these, carried forward unchanged.
- **Elasticsearch: `8.13.4` → `8.15.3`, heap `512m` → `1g`** per spec.
- **Named volumes added** for both `kafka-data` and `es-data` (previously
  neither had one — state didn't survive `docker compose down`).
- **`scripts/verify_stack.sh`** (new) — curls Elasticsearch's cluster health,
  curls kafka-ui's health endpoint, lists Kafka topics from the host via
  `docker compose exec`. One PASS/FAIL line per service, non-zero exit on any
  failure.
- **`versions.md`** (new) — pinned image tags, the Python 3.11 / numpy /
  pandas / yfinance version notes already scattered across earlier `so_far.md`
  entries, and the security-tradeoff statement the ticket requires.

**Not verified against a live stack — this dev machine has no Docker.**
Confirmed only what's possible without it: `docker-compose.yml` parses as
valid YAML (via `PyYAML`) with the expected six services and three volumes,
and `scripts/verify_stack.sh` passes `bash -n` syntax check. The acceptance
criteria in `.claude/0002-docker-compose-infrastructure.md` — `docker compose
up -d` + healthchecks + `verify_stack.sh` all green, host
producer/consumer round-trip, in-container reachability at `kafka:9092`,
`down -v` actually wiping data — are **all unverified**. Ticket left at
`in-progress`, not `done`, until someone with Docker runs them.

**Status:** `docker-compose.yml`/`versions.md`/`verify_stack.sh` written to
spec; `KAFKA_BOOTSTRAP` reconciled across every consumer. End-to-end run still
outstanding.

### 2026-08-04 — Config, compose, and producer rebuilt on the frozen contract (Amir)

Replaced the placeholder `.env.example`, reworked `docker-compose.yml`, and
rewrote `producer/` from a live yfinance puller into a schema-valid snapshot
replay producer. The three had drifted apart: the config named topics
(`prices` / `news`) that no schema describes, and the producer emitted a
message shape (`date`, no `schema_version`) that ticket 0001 froze out.

**Config — `.env.example` rewritten, real `.env` created.**
Dropped `TICKERS` / `PRICE_PERIOD` / `PRICE_INTERVAL` (the producer no longer
fetches, it replays what is on disk). Topics are now `market.prices.v1` /
`sec.filings.v1` / `sec.text.v1`, matching `schemas/README.md`. Added
`KAFKA_BOOTSTRAP_HOST` alongside `KAFKA_BOOTSTRAP` so nobody has to remember
which of 9092/29092 applies where, plus topic-provisioning and replay settings.
`NEWS_TOPIC` / `NEWS_INDEX` survive in a clearly marked legacy block, pointed at
`sec.text.v1`, purely so `spark/pipeline.py` keeps resolving its config.

**Topic provisioning — ticket 0003 implemented.**
`scripts/create_topics.py` creates all three topics with 3 partitions,
replication 1, `retention.ms=-1`, `cleanup.policy=delete`. Idempotent: an
existing topic is never recreated, its config is read back and drift is printed
as a warning while still exiting 0. `scripts/describe_topics.py` prints
partition counts and per-partition watermarks — the "did the data land" tool.

**`docker-compose.yml`.**
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`. This is the substantive change: a
  typo'd topic name is now an error instead of a silent 1-partition, 7-day
  topic whose data vanishes a week later.
- New `topic-init` service runs `create_topics.py` to completion; `producer` and
  `spark` gate on `service_completed_successfully`.
- `producer` and `spark` moved behind the `jobs` profile, so `docker compose up`
  no longer fires one-shot jobs implicitly — `docker compose run` is the only
  path. This is why the README's step 2 is now a bare `docker compose up -d`.
- Explicit `pipeline` bridge network and a `name:` for the project, so service
  DNS and container names don't depend on the clone directory's name.
- Producer build context moved to the repo root (`dockerfile: producer/Dockerfile`)
  so `schemas/` can be copied into the image for the pre-send contract check.
- `init: true` on both job services so Ctrl-C reaches the process and it flushes.
- `restart: unless-stopped` on the long-running services, `"no"` on the jobs.

**`producer/` rewritten.**
`produce.py` is now the ticket 0007 replay producer, with `common.py` holding
the shared client factory, delivery accounting, validation, and progress line.
- Reads both snapshots and merges them into **one sequence ordered by event
  time**, so a filing lands between the price bars surrounding it. Ties break on
  (time, topic, key), so two runs produce identical payloads in identical order
  apart from `ingested_at`.
- Keys by `ticker` / `cik`, sets the `schema_version: 1` header, and sets
  `ingested_at` to the real send time while leaving `ts` / `filed_date` at their
  original event times.
- `--speed instant | realtime | <multiplier>`, `--duration`, `--limit`,
  `--loop`, `--since`, `--prices-only`, `--dry-run`, `--validate-all`.
- Backpressure: `BufferError` polls and retries the *same* message rather than
  dropping it. `acks=all` + `enable.idempotence`. `flush()` before exit with
  attempted-vs-delivered reported, so a dropped tail can't pass silently.
- Contract self-check before the first byte: validates one message per topic
  (or all of them under `--validate-all`) and exits with the failing field path.
- `jsonable()` converts numpy scalars and turns NaN into `null` — `json.dumps`
  otherwise emits the bare token `NaN`, which is invalid JSON that consumers
  accept on read and choke on later.
- Deps: `kafka-python` → `confluent-kafka`, plus pandas/pyarrow/jsonschema.
  `yfinance` is gone from the container entirely.

**`--since` and a data caveat worth knowing.** The EDGAR snapshot starts
1994-01-26; the price snapshot starts 2024-08-05. Under `--speed realtime` that
means 30 years of sparse filings followed by a burst of price bars. The producer
detects the mismatch and prints the exact `--since` flag to fix it rather than
silently dropping data.

**What was verified (no Docker on this machine, so: offline only).**
- `python producer/produce.py --dry-run --validate-all` — all **5,913** messages
  (5,000 price bars + 913 filings) validate against the frozen schemas. Exit 0.
- `--speed realtime --duration 6 --since 2024-08-05` took **6.0s** and the
  progress line advanced ~8.7% per 0.5s — evenly spread, not bunched.
- `--speed nonsense` exits with a readable message, not a traceback.
- `scripts/validate_schemas.py` still passes (4 sample messages, exit 0).
- `docker-compose.yml` parses as valid YAML with the expected 7 services,
  1 network, 3 volumes, and the `jobs` profile on `producer`/`spark`.
- `bash -n scripts/verify_stack.sh`, and `py_compile` on all four new/rewritten
  Python files.

**What is NOT verified.** Everything requiring a broker: topic creation, actual
delivery, partition distribution by key, `service_completed_successfully`
ordering, and the whole of `verify_stack.sh`. Tickets 0002/0003/0007 stay
`in-progress`/`todo` until someone with Docker runs them.

**Left alone deliberately.** `spark/pipeline.py` still reads a `news` topic that
no producer writes; migrating that branch to `sec.text.v1` is ticket 0010 and
the consumer half's call. `scripts/load_historical_prices_to_kafka.py` is now
fully superseded by the producer and should be deleted — left in place rather
than removing untracked work without asking.

**Status:** producer/`.env`/compose consistent with the frozen contract and
verified as far as is possible without Docker. First live run is the next step.

### 2026-08-04 — Producer split into two jobs: backfill + live simulation (Amir)

Requested shape: fill Kafka with a year of data in one go so the topics look
populated at startup, then stream the remaining year in slowly as simulated live
traffic. Built as **one program with a `--mode` flag**, not two scripts.

**Why one tool.** The two jobs must agree on exactly one boundary or they
overlap (duplicate messages) or leave a gap (missing days), and neither failure
is visible until someone counts messages. `produce.py` computes the split once —
first price bar + `BACKFILL_DAYS`, default 365 — and each mode takes one side of
it. `.env` holds the value; both compose services read the same `.env`.

| mode | window | default speed | compose service |
|---|---|---|---|
| `backfill` | event time < split | `instant` | `producer` |
| `live` | event time >= split | `realtime` over `REPLAY_DURATION` | `producer-live` |
| `all` | everything | `instant` | — |

With the current snapshot the split lands on **2025-08-05**: 3,375 events
backfilled, 2,538 streamed live.

- `--backfill-days N` moves the boundary; `--split-at YYYY-MM-DD` overrides it
  outright. Both are in `.env` so the two services cannot be given different
  values by accident.
- Speed default now depends on mode (instant for backfill, realtime for live).
  `REPLAY_SPEED` in `.env` is deliberately **empty**, because a value there
  would override both modes and silently turn the live stream into a bulk dump.
- New `producer-live` compose service behind a `live` profile: same image, same
  `.env`, `--mode live`. `restart: "no"` — this job has a natural end, and
  auto-restarting it would silently replay the year forever, which is what
  `--loop` is for when actually wanted.

**Verified (offline; still no Docker on this machine).**
- Complementarity, checked programmatically rather than by eye: `3,375 + 2,538 =
  5,913`, **overlap 0**, union equals the full event set, both sides ordered.
- `--mode backfill --dry-run` → 3,375 events, speed `instant`.
- `--mode live --dry-run --duration 4` → 2,538 events in **4.0s**, advancing
  ~13% per 0.5s — evenly paced, not bunched.
- `docker-compose.yml` parses; 8 services with the expected profiles/commands.

**Deliberately not done: nothing Spark-related.** `spark/pipeline.py` was not
touched. The `spark` *service block* in `docker-compose.yml` did pick up two
changes from the earlier compose rework — a `jobs` profile and a `depends_on`
on `topic-init` — neither of which changes how `docker compose run --rm spark`
behaves. Flagged here so it is not a surprise; trivial to revert if the consumer
half wants the block left exactly as it was.

**Status:** both producer jobs written and verified as far as is possible
without a broker. `scripts/load_historical_prices_to_kafka.py` is now fully
superseded — its 365-day split is what `--backfill-days` implements — and should
be deleted.

### 2026-08-04 — Docker now available: 0002 verified live, two real bugs found and fixed (Amir)

Docker got installed on this machine. First live run against the real
`docker-compose.yml` immediately surfaced two bugs that nothing offline
(YAML parsing, `bash -n`, `py_compile`) could have caught — both in the
`kafka` service block written for ticket 0002.

**Bug 1 — Kafka crash-looped on every start.**
`apache/kafka:3.9.0`'s storage-format step (`KafkaDockerWrapper` ->
`StorageTool`) validates every listener name present in `KAFKA_LISTENERS`
against `KAFKA_ADVERTISED_LISTENERS`, **including `CONTROLLER`** — even in
combined broker+controller (KRaft, single-node) mode, where the upstream docs
say the controller listener should *not* be advertised. Without a `CONTROLLER`
entry in `KAFKA_ADVERTISED_LISTENERS`, the broker resolved it to the raw
`0.0.0.0` from `KAFKA_LISTENERS` and refused to start: `"advertised.listeners
cannot use the nonroutable meta-address 0.0.0.0"`. Diagnosed by running the
image's own `run`/`configure`/`launch` scripts directly with `docker run
--entrypoint sh` and inspecting the generated `/opt/kafka/config/server.properties`
at each stage — the docs led nowhere, the image's own scripts were the only
reliable source. Fix: added `CONTROLLER://kafka:9093` to
`KAFKA_ADVERTISED_LISTENERS`.

**Bug 2 — Kafka data was silently not persisting, even without `-v`.**
The image's baked-in default `log.dirs` is `/tmp/kraft-combined-logs`. The
compose file's `kafka-data` volume was mounted at `/var/lib/kafka/data` — a
path Kafka never wrote to. Every message was living in the container's
writable layer, gone on `docker compose down` alone, not just `down -v`. This
directly contradicts ticket 0002's stated acceptance criterion ("`docker
compose restart` preserves previously written topics and messages"). Confirmed
by `grep`-ing the image's default `server.properties` for `log.dirs`. Fix: set
`KAFKA_LOG_DIRS=/var/lib/kafka/data` explicitly.

**Everything else in 0002 was correct as written.** Once both fixes landed,
every acceptance criterion in `.claude/0002-docker-compose-infrastructure.md`
was verified against the real stack:

- `docker compose down -v && up -d` → all healthchecks green →
  `bash scripts/verify_stack.sh` → all 8 checks PASS (`.env`, Elasticsearch,
  kafka-ui, host listener, in-network listener, all 3 topics).
- Literal host round trip: `kafka-console-producer.sh` /
  `kafka-console-consumer.sh` against `localhost:29092` on a scratch topic —
  message sent and read back correctly.
- In-network reachability at `kafka:9092` — `topic-init` and `producer` both
  talk to it successfully as containers on the `pipeline` network.
- Elasticsearch responds `yellow`/`green` at `localhost:9200`, no credentials.
- `docker compose down` (no `-v`) then `up -d`: topics and all 5,913 messages
  survived. `docker compose down -v`: volumes actually removed, confirmed via
  `docker volume ls`.

**Ticket 0002 flipped to `done`** in its frontmatter and in `.claude/index.md`.

**0003 and 0007 also exercised live, but left `in-progress`** — not because
anything is broken, but because their written scope is bigger than what
exists:

- `scripts/create_topics.py`: fresh broker creates all 3 topics with the right
  config; re-running reports "config matches", exit 0; manually setting
  `sec.text.v1`'s `retention.ms` to 7 days and re-running correctly reports the
  drift (`expected=-1 actual=604800000`) without touching it, exit 0.
- `scripts/describe_topics.py`: correct partition counts, correct "infinite"
  retention display, correct per-partition message counts.
- Partition-key guarantee (ticket 0003's hardest criterion): consumed every
  `market.prices.v1` message back and grouped by ticker — all 10 tickers land
  on exactly one partition each, zero violations.
- Producer (`--mode backfill`): 3,375/3,375 delivered, 0 failed, against the
  live broker.
- Producer (`--mode live --duration 8`): 2,538/2,538 delivered, 0 failed,
  progress advancing ~13%/0.5s — evenly spread, not bunched.
- Backfill + live together on a clean broker: `2500 + 875 = 3375` then
  `+2500 + 38 = 5000` and `+913` filings → totals landed exactly on **5,000
  price bars + 913 filings = 5,913**, matching the full snapshot with zero
  duplication and zero gap — the complementarity claimed on 2026-08-04 for the
  offline check now confirmed on a real broker.
- Consumed all 5,913 messages back: every one carries the `schema_version: 1`
  header; grand total and date range (1994-01-26 → 2026-08-03) match what was
  sent.
- Missing snapshot directory produces a clean, actionable error (exit 1, no
  traceback), not a stack trace.

Left `in-progress`: ticket 0003 specifies a `Makefile`/`scripts/topics.sh`
wrapper around create+describe — not written. Ticket 0007 specifies a
`producers/replay_producer.py` + `producers/common.py` package plus a
`tests/test_replay_producer.py` suite; what exists lives at `producer/`
instead (extending the pre-existing directory rather than adding a new
package — a reasonable call, but a real deviation from the ticket text) and
has no automated tests. Also genuinely untested: the `BufferError` retry path
under an artificially shrunk `queue.buffering.max.messages`, and a Ctrl-C
mid-run flush. Both would need either a code change (to expose queue size as a
flag) or an interactive interrupt to test properly — left for next time.

**State at the end of this entry:** stack was left up
(`docker compose up -d`), all three topics existed, backfill had been run once —
Kafka held exactly one year of history (2,500 price bars + 875 filings, split
at 2025-08-05) and nothing else. The live stream (`docker compose --profile
live up -d producer-live`) was deliberately *not* left running, so the demo
experience — "looks pre-populated, then watch a year stream in live" — was
still intact for whoever ran it next. Superseded by the next entry: containers
were later stopped with `docker compose down` (no `-v`), so the data above
still exists in the `kafka-data`/`es-data` volumes, but nothing is running
until `docker compose up -d` is run again.

### 2026-08-04 — `scripts/reset_stack.sh` added after a persistence question (Amir)

Kafka/Elasticsearch persisting data across a plain `docker compose down` (not
just `down -v`) is deliberate — ticket 0002's own acceptance criterion — but
it's a real surprise if you don't know it's there: a routine restart quietly
keeps every message you've ever produced. Asked whether that default should
change; decided to keep it (it's the spec'd behavior and the safer default for
a demo) and add an explicit one-command way to opt out of it.

`scripts/reset_stack.sh [--seed]` — `down -v`, `up -d`, wait for healthchecks,
`verify_stack.sh`, optionally re-run the backfill. Verified live: ran it
against a stack that already held a full year of backfilled data, confirmed
the volumes were actually removed and recreated, `--seed` re-backfilled
cleanly (3,375/3,375 delivered again), `verify_stack.sh` all PASS throughout.

**Also clarified for the team:** persistence is per-machine, not shared.
Pulling the repo doesn't hand anyone else's Kafka data to a teammate — each
person's `docker compose up` creates its own local volume, and each person
backfills their own copy the first time they run the producer.

**State at the end of this entry:** ran `docker compose down` (no `-v`) to
stop the containers. The `kafka-data`/`es-data` volumes are untouched — one
year of backfilled history (2,500 price bars, 875 filings) is still sitting in
them, confirmed via `docker volume ls`. Next `docker compose up -d` picks up
right where this session left off; nothing is running in the meantime.

### 2026-08-05 — Spark half rebuilt on the frozen contract: tabular → MLlib → Gemini → Elasticsearch (Dor)

Replaced the yfinance/VADER-era `spark/pipeline.py` with a five-stage job built
against the frozen contract, split across small modules. Verified live against a
running stack, not just written.

**The design decision worth recording: MLlib and the LLM are not two independent
AI features, they are one pipeline.** KMeans finds *where to look*, Gemini says
*what it means*. An LLM cannot scan thousands of price bars or do arithmetic over
them, so without the anomaly stage the prompt carries a coarse average and the
note reads the same for every company. With it, the prompt names specific unusual
days with specific numbers. Anomaly detection was briefly cut from the plan as
"a dressed-up outlier filter" — true as a standalone output, wrong as a feature
extractor, which is why it came back.

- `spark/schemas.py` — the three contract `StructType`s, plus `assert_parsed`,
  which fails loudly when messages arrive but every key field is null. That is
  the `from_json` trap: a drifted schema does not raise, it nulls the column, and
  a downstream `dropna` then clears every row while the job exits 0.
- `spark/transforms.py` — prices (time-based `rangeBetween` windows, not
  row-based), filings (19 facts flattened, restatements deduped by
  `max(filed_date)` per `(cik, fiscal_period, period_end)`, null-safe ratios),
  text (chunk reassembly by `accession_no` + `section`), and the per-ticker
  aggregate.
- `spark/anomaly.py` — z-score **per `(ticker, interval)` group**, then **one**
  KMeans across all groups. Per-group scaling is what makes it meaningful (on a
  global scale NVDA's volatility would define "normal" and JPM would never look
  unusual); a single fit is what makes it scale (one model per ticker is N Spark
  jobs). This replaces MLlib's `StandardScaler`, which standardises globally and
  would undo the per-group scaling.
- `spark/llm.py` — Gemini client. Deliberately **not** a Spark UDF: a UDF runs on
  executors with no shared rate limiting, and a failed task re-runs the whole
  partition, re-issuing calls that already cost quota.
- `spark/es_writer.py` — streamed via `toLocalIterator()`, never `toPandas()`;
  create-if-missing instead of delete-and-recreate; deterministic `_id`s.
- `spark/prompts/analyst.md` — the prompt, loaded at runtime so tuning it is not
  a code change.

**Verified live 2026-08-05.** 2,500 price bars + 875 filings + 1 text document →
130 anomalies (13 per ticker, exactly 5% of 250 bars each, so the per-group
threshold is working) → 10 analyst notes → `stock_prices` 2,500 /
`stock_filings` 875 / `stock_analysis` 10. Ran twice; counts unchanged, which
confirms the deterministic ids make the load an upsert rather than an append.

**Finding — Gemini's free tier limits both rate *and* daily volume, and both
were hit.** The first run set `LLM_CONCURRENCY=4`, which exhausted the
per-minute window instantly; all four workers then retried into the same wall on
a 1–4s backoff, far too short for a per-minute quota, and 8 of 10 failed. Fixed
three ways: default concurrency dropped to 1, a cross-thread `Pacer` spaces calls
≥13s apart, and a 429 now reads the server's own `retryDelay` instead of the
generic exponential backoff. The second run got 10 of 10 in ~2.5 min.

A third run the same day then failed 6 of 10 *with the pacing applied*, which
rules out the per-minute limit as the only constraint — **there is also a daily
cap, and roughly 30 calls exhausted it.** Practical consequence for anyone
demoing this: the analyst stage is good for about two full runs per day on one
key. Iterate on the Spark stages with `LLM_ENABLED=false` and save the quota for
the real run. Switching `GEMINI_MODEL` to a different model also switches quota
bucket, which is the other way out.

**This is the argument for aggregating before the LLM stage stated in numbers** —
at one call per row instead of one per ticker, this dataset would need 3,375
calls, which is beyond the daily quota by two orders of magnitude.

**Two defects the quota exhaustion exposed, both now fixed.** Neither was
visible while every call succeeded:

- *A failed analysis upserted an empty document over a good one.* The
  Elasticsearch `_id` is `ticker|interval|as_of`, so a failure later the same day
  overwrote a successful analysis written earlier that day — the retry destroyed
  the results it was meant to add to. `write_analyses` now skips failed rows
  entirely and reports which ones it skipped, leaving the earlier document intact.
- *Stale `.txt` reports masqueraded as current.* `write_reports` skipped
  failures, so the previous run's file stayed on disk with no marker saying it was
  old, sitting indistinguishably beside fresh ones. It now removes the stale file
  for any ticker that failed, so the directory always reflects the current run.

**Finding — the model caught a real data inconsistency we had not noticed.** The
AAPL note flagged a "data timeline mismatch between price history ending August
2025 and press release filing text dated July 2026". Correct: the backfill cuts
prices at 2025-08-04, but `schemas/samples/sec.text.v1.json` is Apple's July 2026
press release. Harmless while the text topic holds one hand-loaded sample; worth
watching once ticket 0010 produces text across the whole window.

**`sec.text.v1` is still effectively empty** (ticket 0010 `todo`), so 9 of 10
notes have no narrative text. The notes handle this honestly rather than
hallucinating around it — they name the absence as a stated limitation and report
`confidence: low`. Nothing on the Spark side needs changing when the producer
lands; the text path is already built and exercised against the sample.

**Project direction changed, and the README now says so.** The AI capability is
no longer MLlib alone, `stock_news` and the VADER dependency are gone, three
indices replace two, and the "no paid APIs, no keys" claim is retired — the
infrastructure is still fully local and free, but the analyst stage calls a
hosted API. `LLM_ENABLED=false` runs everything else without a key.

**Not done:** no automated tests (the "How to test" rows above are all manual),
and `dashboard/app.py` still reads the old `stock_news` index — that is Person C's
work and they have not started, so the new index names were chosen without
needing to coordinate.

### 2026-08-05 — Stage 3b: the analyst context is now inspectable (Dor)

**Problem it fixes:** the stage-3 aggregate — the single row per instrument that
the LLM actually reasons over — was assembled in memory, handed to Gemini, and
discarded. Every other stage's output could be inspected in Elasticsearch or
kafka-ui; the one thing you could not see was what the model was shown, which is
the first thing you want when an answer looks wrong.

Two artifacts, both produced **before** the API call and **regardless of whether
one happens**:

- `stock_context` index — the aggregate as queryable fields (bar counts, the
  top-5 anomalies as a nested array, anomaly-near-filing counts, latest facts and
  ratios, `filing_text_available`), plus `context_json` holding the literal blob
  embedded in the prompt. Stored with `index: false` so it is retrievable without
  bloating the search index.
- `llm_output/_prompts/TICKER.txt` — the exact prompt string.

**Both are built by the same `build_prompt()` the API call uses.** An inspection
artifact that is merely *similar* to the real request is worse than none, because
it invites debugging text that was never sent.

**Why this matters more than it looks, given the daily quota.** Prompt assembly
costs nothing, so `spark/prompts/analyst.md` can be iterated on and verified with
`LLM_ENABLED=false` as many times as needed, and the ~2 runs/day of quota spent
only on a prompt already known to be right. Before this, checking a prompt change
meant spending a call to find out.

**Verified 2026-08-05 with `LLM_ENABLED=false`** (deliberately — the run that
proves the feature should be the run that needs no quota): 10 prompts dumped, 10
context documents indexed, `stock_prices` and `stock_filings` unchanged at
2,500 / 875. NVDA's context shows 13 anomalies with the 2025-01-27 −16.97% /
3.11× volume day ranked first, `revenue_yoy` +69.2%, and a 4,331-character prompt.

**Also surfaced as a queryable field:** `filing_text_available` is `false` for 9
of 10 instruments. Ticket 0010's absence is now a single query over
`stock_context` rather than something you notice by reading the notes.

**Four indices now**, so Person C's dashboard reads `stock_prices`,
`stock_filings`, `stock_context`, `stock_analysis`.

### 2026-08-05 — Analyst stage made provider-switchable; Groq added (Dor)

Gemini's free-tier daily cap made iteration impractical — roughly two full runs
before it starts refusing. Added `LLM_PROVIDER=groq|gemini` rather than swapping
one for the other, so the project keeps both.

**Only the transport differs.** Two small functions per provider — build the
request, extract the text — behind a dispatch table. The prompt, the context
builder, the JSON contract, the pacer, the retry policy, the report writer and
both Elasticsearch indices are shared. Groq speaks the OpenAI chat-completions
dialect, so a third provider would be a near-copy of the Groq pair.

Per-provider defaults live in `pipeline.py`: key, model, and a pacing interval
(1s for Groq, 13s for Gemini). `LLM_MIN_INTERVAL_SECONDS` overrides both; left
empty, each provider gets the right value.

| | Groq | Gemini |
|---|---|---|
| Ten instruments | **65s, 10/10** | ~2.5 min, quota-blocked at 4/10 |
| Free tier | generous | ~5 req/min *and* ~30 calls/day |

**Finding — the Cloudflare 403 that looks like an auth failure.** Groq returned
`HTTP 403: error code: 1010` on every call while the same key worked fine from
`curl`. That is Cloudflare rejecting `urllib`'s default `Python-urllib/3.11`
User-Agent, not Groq rejecting the key. Isolated it by sending one request each
way from inside the container: default UA → 403, explicit UA → 400 from Groq
itself (a real API complaint about the test payload, i.e. the request had
arrived). Fixed by setting a `User-Agent` header on every request. Worth knowing
before adding a third provider — nothing in the error names the real cause.

**Finding — the model answered `hold` for all ten instruments**, where Gemini
had produced a spread on identical data. The notes were factually sound; the
`recommendation` field was simply dead — nothing for the dashboard to filter,
group or chart on. The model said why in its own summary: *"the data does not
strongly support a buy or sell recommendation."*

The cause was a prompt gap, not a model defect: `recommendation` and
`confidence` were not distinguished, so uncertainty was being expressed as
`"hold"`. Added rule 6 making the split explicit — `recommendation` is the
direction the evidence leans, `confidence` is how much that direction can be
trusted, and thin data means `confidence: low`, never `"hold"`. `"hold"` is
reserved for genuinely conflicting signals, and the `signals` list must support
whatever recommendation was given.

**Result on identical data: 10/10 `hold` became 7 buy / 2 hold / 1 sell.** The
two remaining `hold`s are BRK.B and JPM — the two whose data genuinely
conflicts (BRK.B's net income fell 59% YoY; JPM is the bank with 10 of 19 facts
structurally null). TSLA's `sell` cites a revenue decline of 11.78%, net income
down 20.7%, and a close below its 30-day moving average, at `confidence: low`
for the missing filing text. Direction and certainty are now separate fields
doing separate jobs.

**Worth remembering for the demo:** this is the clearest example in the project
of prompt design mattering as much as model choice. The same model on the same
data produced a useless field and then a useful one, with no code change — only
`spark/prompts/analyst.md`. The prompt-dump artifacts from stage 3b are what
made testing that change free.

### 2026-08-06 — `RUNBOOK.md` added (Dor)

The project had a README describing what the pipeline *is* and a work log
describing who built what, but nothing describing how to run it from a clean
machine and confirm each stage actually did its job. Verifying a run meant
reconstructing the commands from memory.

`RUNBOOK.md` is the missing piece: teardown → up → verify → producer → Spark,
with the **expected output and a validation command after every step**, so a
mismatch localises the failure instead of surfacing as a wrong number three
stages later. It doubles as the demo script.

Two things it captures that repeatedly caused confusion:

- **kafka-ui shows an empty Messages pane on a full topic** unless Seek Type is
  set to Oldest — the backfill has finished, so a newest-first view has nothing
  to show.
- **Partitions come out uneven (1000/1500/0) and that is correct.** Messages are
  keyed by `ticker`, so each ticker hashes to one partition and ten tickers over
  three partitions cannot balance. It is the ordering guarantee working, not skew
  to fix.

It also documents the Elasticsearch/filesystem split, which is easy to
misread: **Elasticsearch holds JSON documents only.** The `.txt` files under
`llm_output/` are on the host, and duplicate nothing — `llm_output/TICKER.txt`
is `stock_analysis` rendered for reading, and `llm_output/_prompts/TICKER.txt`
is the `context_json` field of `stock_context` wrapped in the prompt template.
Files for reading, indices for querying.

**Gotcha the runbook now documents:** `_cat/indices` reports `stock_context` as
**60 documents when there are 10**. `top_anomalies` is a `nested` field, so
Elasticsearch stores each anomaly as its own hidden Lucene document — 10 parents
x (1 + 5 anomalies) = 60. `_cat/indices` counts Lucene docs, `_count` counts real
ones. Caught while writing the runbook, which had been telling readers to expect
10 from the command that returns 60; every count check now uses `_count`. The
other three indices have no nested fields, so both agree on them and nothing
looked wrong until this index existed.

Linked from `README.md` (ahead of the quick-start) and from the top of this file.

### 2026-08-06 — Two bugs found by running the runbook end to end (Dor)

Ran `RUNBOOK.md` top to bottom to verify the commands as written. Steps 0-5
matched exactly. **Step 6 hung for an hour**, and finding out why exposed two
defects that had been present since the analyst stage was written.

**Bug 1 — an unbounded `retry-after` sleep.** On a 429 the client honoured
whatever wait the provider asked for. Groq, on an exhausted daily budget, asks
for **606 seconds**. With five retries across ten instruments that is hours of
sleeping, and the job looks hung because it *is* — just deliberately.

Fixed with `MAX_BACKOFF = 60`: past that ceiling the instrument fails with a
message naming the requested wait, and the run continues. Nine analyses plus one
recorded failure beats an hour of silence. Pacing cannot help here — pacing
solves a per-minute rate, and this is a daily budget.

**Bug 2 — Python block-buffers stdout when it is a pipe**, so
`docker compose run ... | grep` held every stage banner in an 8 KB buffer until
exit. Meanwhile the JVM's log4j output kept arriving on stderr, so the console
showed *activity* while the pipeline's own progress was invisible. That is what
turned a diagnosable stall into an hour of guessing: the client was printing
"rate limited, waiting 606s" the whole time and none of it reached the terminal.
Fixed with `ENV PYTHONUNBUFFERED=1` in `spark/Dockerfile`, plus `flush=True` on
the retry message.

Bug 2 is the more instructive one. Bug 1 was a bad default; bug 2 is why it took
an hour to find a bad default.

**Finding — Groq's free tier meters TOKENS, not requests: 100,000 per day.**
Earlier notes called it "generous" against Gemini's ~30 calls/day, which was
right in spirit and wrong in units. A ten-instrument run costs ~13k tokens
(~1.2k per prompt, ~3.4k for the one carrying filing text), so the real budget is
**about seven runs per day**. Today's testing used 99,343 of 100,000.

`.env.example`, `README.md` and `RUNBOOK.md` now state the token figure rather
than describing the tier as generous.

**Correction — the jar download is not slow, and an earlier note in this log
saying so was wrong.** Two runs took far longer than expected and both were
attributed to `docker compose down -v` wiping the `spark-ivy` volume, forcing a
Kafka-connector re-download. Measured properly: a cold run with the cache wiped
takes **32s**, warm **23s**. The jar is 112 MB and costs about nine seconds.

Both slow runs were the analyst stage. The first was Gemini's 13s pacing (130s
minimum for ten instruments) plus 30s retry backoffs; the second was the 606s
`retry-after` bug above. Diagnosing by `-e LLM_ENABLED=false` separates the two
in one command: ~30s means the Spark half is fine and the time is API waits.

The runbook now carries measured timings rather than an estimate.

**Verified after the fixes:** the run that would have hung for an hour instead
completed in ~1 minute, wrote the one analysis that succeeded, removed the nine
stale reports, and left no empty documents over good ones. Stages 1, 2, 2b, 3, 3b
and 5 all produced their expected counts (2,500 / 875 / 10). Only
`stock_analysis` is short, which is the correct behaviour with no budget left.

**Not yet verified:** a clean end-to-end run with all ten analyses, because the
token budget is spent. The Spark half is unaffected — that path ran correctly
four separate times today.

### 2026-08-06 — Analyst stage given a provider chain with a local fallback (Dor)

Both hosted free tiers can be exhausted in a working day, and an exhausted
provider mid-demo previously meant no analyses at all. The stage now takes an
ordered chain rather than a single provider.

```
LLM_PROVIDER=groq
LLM_FALLBACK_PROVIDERS=gemini          # or gemini,ollama
```

Adding `ollama` was cheap because it speaks the OpenAI chat-completions dialect,
the same as Groq — the provider table takes two small functions per entry and
everything else is shared. It runs behind the `local-llm` compose profile with a
persistent volume for pulled models.

**The design question was what should make the chain move**, and the answer split
into two classes:

*Provider retired for the whole run* — every remaining row skips it:
budget exhausted (a 429 asking for longer than the 60s ceiling), auth rejected
(401/403), unreachable host, or **two consecutive row failures**.

*Row-level, provider stays in play* — malformed JSON, a one-off 5xx, a timeout.

The consecutive-failure rule exists because not every exhausted provider
announces itself with one long wait. Groq says "1362s" and is trivially
detected; Gemini's per-minute limit returns 25-46s delays, all under the ceiling,
so without the rule every row retried four times, failed, and the provider was
never retired.

**Then a second, subtler mistake, found by measuring rather than reasoning.**
With the consecutive-failure rule in place a doomed run still took over ten
minutes: two rows x five retries x up-to-60s backoffs before retirement ever
fired. The rule was right and the retry budget was wrong.

**Retries are for when there is nowhere else to go.** With a fallback still
available the client now retries twice and hands the row over; only as the last
resort does it retry five times. Grinding through five attempts against a
provider that is already refusing reaches the same outcome minutes later.

**Verified live, and better than the failure case we were testing for.** The run
that proved it was not a doomed one: Groq served 9 of 10 instruments, hit its
*per-minute* token limit on MSFT (TPM 12,000), failed over to Gemini in seconds,
and MSFT came back `buy`. **10/10 analyses in 64 seconds**, closing line
`produced by: gemini x1, groq x9`. The fallback is not just insurance against a
dead provider — it absorbs ordinary rate-limit turbulence mid-run.

**Provenance is now recorded**, because it has to be: `provider_used` and
`model_used` on every `stock_analysis` document and at the head of every `.txt`.
A note from a 3B local model and one from a 70B hosted model are not
interchangeable and the index must not imply otherwise.

**Documentation gap found while verifying:** `RUNBOOK.md` said to expect 10
documents in `stock_context` and `stock_analysis`, but their `_id` carries
`as_of`, so each day keeps its own set — 20 and 20 on day two, by design, since
Tuesday's view of an instrument does not replace Monday's. `stock_prices` and
`stock_filings` are keyed on the data itself and stay flat. The runbook now says
so and gives a date-scoped count for checking today's set.

**Answering a question that came up: Spark MLlib has no LLM.** It is classical ML
— clustering, classification, regression, and feature transformers like
`Word2Vec`, `HashingTF` and `LDA`. Those produce vectors and topics, not
generated text, and there is no LLM connector. Spark NLP (John Snow Labs) is the
third-party library that runs transformers distributed on Spark, but it targets
classification and NER rather than note generation, and it is a heavy dependency.
Keeping the LLM outside Spark is also the right call independently: ten HTTP
calls belong on the driver where pacing and retries are controllable, not spread
across executors where a failed task re-runs a partition and re-issues calls that
already cost quota.

### 2026-08-06 — API keys per person, and a crash on the path every teammate hits first (Dor)

Asked what happens to the API key when someone else runs the project. `.env` is
gitignored, so a key never leaves the machine it was created on and a fresh clone
gets `.env.example` with the key fields empty. Each person needs their own —
free, a minute to obtain, no card. Sharing one is not a secrecy problem here so
much as a quota one: the budgets are per key, so three people rehearsing on one
key exhaust it three times as fast.

**Testing that path found a crash, and it had nothing to do with keys.**
Simulating a teammate with no credentials produced
`FileNotFoundError: [Errno 2] No such file or directory: ''` and exit 1.

`.env.example` ships several variables deliberately empty, meaning "use the
built-in default" — `PROMPT_PATH=`, `REPLAY_SPEED=`,
`LLM_MIN_INTERVAL_SECONDS=`. But `env_file` passes an empty variable through as
an empty *string*, not as absent, so `os.getenv(name, default)` returns `""` and
the default never applies. `PROMPT_PATH` became `""` and reached `open("")`.

This would have hit **every teammate on their first run, with or without a key**,
because it comes straight from copying the template. It was introduced an hour
earlier while syncing `.env` to `.env.example` — before that, the variable was
simply absent locally and the default worked, so nothing showed it. `_int` and
`_float` already coerced empty to their defaults; only `_env` did not. Now it
agrees with them.

**Verified after the fix, both cases exit 0:**

- *No keys at all* — stage 4 skips with `no API key set for any provider in the
  chain`, and `stock_prices` (2,500), `stock_filings` (875) and `stock_context`
  (10) are all still written, prompts still dumped. Only `stock_analysis` is
  absent.
- *Groq key only, no Gemini* — 10/10 analyses via Groq, the keyless fallback
  quietly skipped rather than stalling the run.

**Worth noting how this was found.** It surfaced from asking what a *different
person* would experience, not from testing the code as configured on this
machine. A working `.env` had been masking a template that crashes on copy. Added
as a test row above, since it is the first thing anyone new runs into.
