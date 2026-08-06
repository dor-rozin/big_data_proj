# Runbook — full run from scratch, with validation at every step

Every step has a command, what you should see, and how to check it. Follow it
top to bottom for a clean end-to-end run — this is also the demo script.

For what the pipeline *is*, see [README.md](README.md). For who built what and
what is still outstanding, see [so_far.md](so_far.md).

Expected end state:

| Kafka topic | messages | | Elasticsearch index | docs |
|---|---|---|---|---|
| `market.prices.v1` | 2,500 | | `stock_prices` | 2,500 |
| `sec.filings.v1` | 875 | | `stock_filings` | 875 |
| `sec.text.v1` | 60 | | `stock_context` | 10 |
| **backfill total** | **3,435** | | `stock_analysis` | 10 |

---

# First time on this machine — do this before anything else

Skip to step 0 if you have already done it once.

## A · Create your `.env`

```bash
cd big_data_proj
cp .env.example .env
```

**`.env` is gitignored and never comes down from git.** It is where API keys
live, so each person creates their own. The copy fills in 37 values that are
already correct — Kafka addresses, topic names, index names, snapshot paths — and
you change none of them.

## B · Get your own API key and paste it in

The analyst stage calls a hosted LLM. Keys are **free**, take about a minute, and
need no card. **Get your own — do not reuse a teammate's.** The free budgets are
metered per key, so three people sharing one exhaust it three times as fast.

| Provider | Where | Put it in `.env` as |
|---|---|---|
| **Groq** (primary, recommended) | https://console.groq.com/keys | `GROQ_API_KEY=gsk_...` |
| **Gemini** (fallback, optional) | https://aistudio.google.com/apikey | `GEMINI_API_KEY=...` |

Groq alone is enough. Adding Gemini gives the chain somewhere to fail over when
Groq's daily budget runs out.

Write the value bare — no quotes, no spaces around the `=`:

```
GROQ_API_KEY=gsk_abc123...
```

Check it took, without printing the whole thing:

```bash
export $(grep "^GROQ_API_KEY=" .env | xargs)
curl -s -H "Authorization: Bearer $GROQ_API_KEY" \
  https://api.groq.com/openai/v1/models | grep -c '"id"'
```

A number back (a dozen or so) means the key works. Nothing back means it does
not — check for stray quotes or a truncated paste.

### Leave the other blanks alone

`.env.example` also ships `REPLAY_SPLIT_AT`, `REPLAY_SPEED`, `REPLAY_MODE`,
`LLM_MIN_INTERVAL_SECONDS` and `PROMPT_PATH` with **no value on purpose** — empty
means "use the built-in default". Don't fill them in unless you mean to override
something.

### No key? It still runs.

Stage 4 skips itself and the run exits 0. You get `stock_prices`,
`stock_filings` and `stock_context`, plus the prompts in `llm_output/_prompts/`.
Only `stock_analysis` is missing.

## C · Give Docker enough memory

Docker Desktop → Settings → Resources → **at least 8 GB** (10-12 GB if you plan
to enable the local `ollama` fallback).

---

## 0 · Tear down

```bash
cd ~/big_data_proj
docker compose --profile jobs --profile live down -v
```

**Validate — nothing left behind:**

```bash
docker ps -a | grep -E "kafka|elastic|spark|dashboard" || echo "no containers"
docker volume ls | grep big-data-proj || echo "no volumes"
```

Both should print the fallback message. If a stray `kafka` container survives a
`down`, it belongs to an older compose project name and Docker enforces
container names globally — remove it directly with `docker rm -f kafka`.

---

## 1 · Bring the stack up

```bash
docker compose up -d
```

**Validate:**

```bash
docker compose ps
```

`kafka` and `elasticsearch` must reach `(healthy)`, not merely `Up`. Allow ~15s.
`kafka-ui` and `dashboard` start alongside them.

---

## 2 · Verify infrastructure

```bash
bash scripts/verify_stack.sh
```

Expect **8 PASS lines**: `.env`, Elasticsearch, kafka-ui, Kafka from the host,
Kafka in-network, and each of the three topics.

**See how the topics were provisioned:**

```bash
docker compose logs topic-init
```

Three `CREATED ... partitions=3 replication=1 retention.ms=-1` lines. Retention
of `-1` is deliberate: Kafka's 7-day default would make data written on Monday
vanish the following Tuesday, and the failure would look like a consumer bug.

---

## 3 · Record the "before" state

```bash
for t in market.prices.v1 sec.filings.v1 sec.text.v1; do
  echo -n "$t: "
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic $t | awk -F: '{s+=$3} END {print s+0}'
done
curl -s "localhost:9200/_cat/indices?h=index" | grep -v "^\." || echo "no indices"
```

**Expect all three topics at `0` and no indices.** Worth capturing before
claiming anything downstream was created.

**Visual:** [localhost:8080](http://localhost:8080) → Topics.

---

## 4 · Producer → Kafka

```bash
docker compose run --rm producer
```

**Expect:** `delivered : 3435`, `failed : 0`, about a second.

**Validate the split:**

```bash
for t in market.prices.v1 sec.filings.v1 sec.text.v1; do
  echo -n "$t: "
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic $t | awk -F: '{s+=$3} END {print s+0}'
done
```

**Expect `2500`, `875` and `60`** — together the 3,435 delivered.

The producer also prints why the text count is small:

```
text: 116 of 1,324 press releases ... land on or after the first price bar
      (2024-08-05); the rest predate it and are skipped
```

The archive goes back to 2000, but a press release with no price bar to join
against is noise. 116 survive the clip; 60 of those fall in the backfill window
and the rest go to the live stream.

**See a RAW message. This is the "before" half of the comparison in step 7:**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.prices.v1 \
  --from-beginning --max-messages 1
```

One flat JSON object with **10 fields**.

**Visual:** [localhost:8080](http://localhost:8080) → `market.prices.v1` →
Messages → set **Seek Type: Oldest** → Submit. On the default "Newest" the pane
looks empty, because the backfill has finished and nothing new is arriving — the
topic is full regardless.

### Uneven partitions are correct

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.prices.v1
```

Expect something like `1000 / 1500 / 0` across the three partitions. Messages are
keyed by `ticker`, so each ticker hashes to exactly one partition; ten tickers
over three partitions cannot come out even. That is the contract working —
per-company ordering is guaranteed — not a bug.

---

## 5 · (nothing to do — text arrives with the producer)

Earlier versions of this runbook hand-loaded a single sample message here,
because `sec.text.v1` had no producer. **Ticket 0010 landed and it does now** —
step 4 already delivered 60 press releases. Nothing to run.

## 6 · Spark pipeline

```bash
docker compose --profile jobs run --rm spark
```

**Measured timings** (Apple Silicon, images already built):

| | Time |
|---|---|
| Spark stages only (`LLM_ENABLED=false`), jar cached | **23s** |
| Spark stages only, after `down -v` wiped the jar cache | **32s** |
| Full run including the Groq analyst stage | **~60s** |

The Kafka connector jar that `down -v` wipes is 112 MB and costs about nine
seconds to re-fetch — not worth engineering around.

**If a run takes minutes rather than seconds, the analyst stage is the cause,
not Spark.** Gemini paces at 13s per call (130s minimum for ten instruments)
and adds 30s backoffs on a 429. Groq paces at 1s. Confirm by running with
`-e LLM_ENABLED=false`: if that finishes in ~30s, the Spark half is healthy and
the time is going into API waits.

**If the analyst stage reports failures**, check the message: Groq's free tier is
capped at **100,000 tokens per day** and a ten-instrument run costs ~13k, so
about seven runs exhaust it. The client refuses any wait over 60s and records
those instruments as failures, so the run still finishes on time with partial
results. Everything except `stock_analysis` is unaffected. Re-run tomorrow, or
set `LLM_PROVIDER=gemini`, or `LLM_ENABLED=false` to skip the stage.

**Expected stage output:**

| Stage | Line |
|---|---|
| 1 | `2500 messages parsed`, `875`, `60` |
| 2 | `prices: 2500` · `filings: 875` · `text: 60` · `groups: 10` |
| 2b | `10 groups, 130 bars flagged at top 5% per group` |
| 3 | `aggregated to 10 row(s)` |
| 3b | `dumped 10 prompt(s)` |
| 4 | `Stage 4 - LLM analyst (groq)`, then 10 results |
| 5 | `2500` · `875` · `10` · `10` |

If the daily token budget is gone, stage 4 shows failures and stage 5 writes
fewer than 10 analyses. Stages 1-3b and the other three indices are unaffected —
that is the intended degradation, not a broken run.

**Two things that should look wrong if they appear:**

- **130 anomalies means exactly 13 per ticker** (5% of 250 bars). A different
  distribution means the per-group threshold is not being applied per group.
- **The 10 recommendations should be a mix.** Ten identical values means the
  `recommendation`/`confidence` separation in `spark/prompts/analyst.md`
  regressed, and the field is carrying no information.

---

## 7 · Before → after

```bash
echo "=== BEFORE (raw, off Kafka) ==="
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.prices.v1 \
  --from-beginning --max-messages 1 2>/dev/null | python3 -m json.tool

echo "=== AFTER (transformed, in Elasticsearch) ==="
curl -s "localhost:9200/stock_prices/_search?size=1" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"is_anomaly":true}},"sort":[{"anomaly_score":"desc"}]}' \
  | python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin)['hits']['hits'][0]['_source'],indent=2))"
```

**10 fields in, 21 out.** `schema_version` is dropped on the way through (it
belongs to the wire contract, not the analysis table) and 12 are added.

Stage 2 adds nine: `date`, `daily_return`, `volume_change`,
`intraday_range_pct`, `ma_7`, `ma_30`, `volatility_10`, `avg_volume_20`,
`volume_ratio`. Stage 2b adds three: `cluster`, `anomaly_score`, `is_anomaly`.

**Filings — the nested struct flattened:**

```bash
curl -s "localhost:9200/stock_filings/_search?size=1" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"ticker":"JPM"}},"sort":[{"filed_date":"desc"}]}' | python3 -m json.tool
```

JPM is the case worth reading. The `facts` object became 19 columns plus 10
derived ratios, and **8 of those facts are legitimately absent** — a bank reports
no gross profit and no classified balance sheet. Every ratio depending on a
missing input is absent rather than `0`. A zero there would be a wrong number
presented as a real one.

---

## 8 · The four tables

```bash
for i in stock_prices stock_filings stock_context stock_analysis; do
  echo -n "  $i: "
  curl -s "localhost:9200/$i/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"
done
```

| Index | Docs | Contents |
|---|---|---|
| `stock_prices` | 2,500 | bars + derived columns + anomaly flags |
| `stock_filings` | 875 | 19 facts + 10 ratios, restatements deduped |
| `stock_context` | 10 | exactly what the analyst was shown |
| `stock_analysis` | 10 | recommendation, confidence, risks, signals, summary |

### `stock_context` and `stock_analysis` grow by one set per day

Their `_id` is `ticker|interval|as_of`, so re-running **today** overwrites
today's documents while **yesterday's are preserved as history**. On day two you
will see 20 and 20, not 10 and 10, and that is correct — the analyst's view of
an instrument on Tuesday is not a replacement for its view on Monday.

To check today's set specifically:

```bash
TODAY=$(date +%F)
curl -s "localhost:9200/stock_analysis/_count" -H 'Content-Type: application/json' \
  -d "{\"query\":{\"term\":{\"as_of\":\"$TODAY\"}}}"
```

`stock_prices` and `stock_filings` are keyed on the data itself
(`ticker|interval|ts`, `accession_no`), so they stay flat at 2,500 and 875 no
matter how many days you run.

### Use `_count`, not `_cat/indices`

`_cat/indices` reports **`stock_context` as 60**, and that is not an error.
`top_anomalies` is mapped as a `nested` field, so Elasticsearch stores each
anomaly as its own hidden Lucene document: 10 parents x (1 + 5 anomalies) = 60.
`_cat/indices` counts Lucene documents; `_count` counts real ones.

```bash
curl -s "localhost:9200/stock_context/_count"   # 10  <- the real answer
curl -s "localhost:9200/_cat/indices/stock_context?h=docs.count"   # 60
```

The other three indices have no nested fields, so both agree on them. Check
counts with `_count` and this never comes up.

**Recommendations:**

```bash
curl -s "localhost:9200/stock_analysis/_search?size=20" -H 'Content-Type: application/json' \
  -d '{"_source":["ticker","recommendation","confidence"],"sort":[{"ticker":"asc"}]}' \
  | python3 -c "
import sys,json
for h in json.load(sys.stdin)['hits']['hits']:
    s=h['_source']; print(f\"  {s['ticker']:6} {s['recommendation']:5} ({s['confidence']})\")"
```

**What the analyst was given, and what it produced:**

```bash
curl -s "localhost:9200/stock_context/_search?pretty" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"ticker":"NVDA"}},"_source":{"excludes":["context_json"]}}'

cat llm_output/_prompts/NVDA.txt   # the literal prompt that was sent
cat llm_output/NVDA.txt            # the note that came back
```

### Elasticsearch holds JSON, not files

Everything in Elasticsearch is a JSON document. The `.txt` files live on your
host in `llm_output/`, mounted into the container — they are **not** stored in
Elasticsearch.

There is no data in the files that is missing from Elasticsearch: `llm_output/`
is `stock_analysis` rendered for a human to read, and `llm_output/_prompts/` is
the `context_json` field of `stock_context` wrapped in the prompt template. The
files are for reading; the indices are for querying and for the dashboard.

| | Elasticsearch | Host filesystem |
|---|---|---|
| Analyst note | `stock_analysis.summary` (+ structured fields) | `llm_output/TICKER.txt` |
| Prompt sent | `stock_context.context_json` | `llm_output/_prompts/TICKER.txt` |
| Price / filing tables | `stock_prices`, `stock_filings` | — |

---

## 9 · Idempotency

```bash
docker compose --profile jobs run --rm spark
for i in stock_prices stock_filings stock_context stock_analysis; do
  echo -n "  $i: "
  curl -s "localhost:9200/$i/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"
done
```

**`stock_prices` and `stock_filings` must be unchanged** — 2,500 and 875, not
doubled. `stock_context` and `stock_analysis` are unchanged too *within a day*;
across a day boundary they gain one set each, by design (see above). Every document
id is derived from its natural key (`ticker|interval|ts`, `accession_no`,
`ticker|interval|as_of`), so a re-run upserts rather than appends. This is what
makes stage 1's `earliest → latest` read safe: the job is a full reprocess with
no offset state to track, and running it twice equals running it once.

---

## Known conditions, not faults

**The dashboard at :8501 does not work yet.** `dashboard/app.py` still reads the
retired `stock_news` index. That is Person C's stage and it has not been started.
Validate through Elasticsearch and kafka-ui.

**All 10 instruments should now report `filing_text_available: true`.** Ticket
0010 landed the text producer, so every prompt carries a real press release.
Prompts grew from ~4,300 to ~12,000 characters as a result — roughly 3,000 tokens
each, which is why the daily budget now stretches to about three runs rather than
seven.

```bash
curl -s "localhost:9200/stock_context/_search?pretty" -H 'Content-Type: application/json' \
  -d '{"_source":["ticker","filing_text_available","anomalies_near_filing"],"size":20}'
```

**Running without an API key.** Nothing special is required — if no provider in
the chain has a key, stage 4 skips itself and the run exits 0. Stages 1, 2, 2b, 3,
3b and 5 all complete, and the prompts are still dumped so the context stays
inspectable. `-e LLM_ENABLED=false` does the same thing explicitly.

**Each person needs their own key.** `.env` is gitignored, so a fresh clone has
`.env.example` with the key fields empty. Free keys:
[Groq](https://console.groq.com/keys), [Gemini](https://aistudio.google.com/apikey).
Don't share one across the team — the free budgets are per key, so three people
rehearsing on one exhaust it three times as fast.
