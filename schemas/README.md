# Message contract

The byte shape of everything this project writes to Kafka. Two topics, two
schemas, sample messages for each. **This contract is frozen.** Changing a field
here changes the Spark job and the Elasticsearch mapping downstream, so it is a
conversation with the whole team, not a commit.

| topic | schema | sample | Kafka message key |
|---|---|---|---|
| `market.prices.v1` | [`market.prices.v1.schema.json`](market.prices.v1.schema.json) | [`samples/market.prices.v1.json`](samples/market.prices.v1.json) | `ticker` |
| `sec.filings.v1` | [`sec.filings.v1.schema.json`](sec.filings.v1.schema.json) | [`samples/sec.filings.v1.json`](samples/sec.filings.v1.json) | `cik` |
| `sec.text.v1` | [`sec.text.v1.schema.json`](sec.text.v1.schema.json) | [`samples/sec.text.v1.json`](samples/sec.text.v1.json) | `cik` |

Keying by `ticker` / `cik` puts every message about one company on one partition,
so per-company ordering holds without an ordering guarantee across the topic.

`sec.filings.v1` and `sec.text.v1` are the numbers and the words from the same
filings, split across two topics rather than combined into one. A facts message is
~1 KB and a press release is ~12 KB; a full 10-K is 357 KB. Putting them together
would make every small message pay for the large ones, and they have different
consumers (a numeric join vs NLP) and different Elasticsearch mappings (numerics
vs analyzed text). Join them on `accession_no`, or on `cik` + `filed_date`.

## Cross-cutting rules

These three hold on **every** message on **both** topics:

1. **Timestamps are UTC with a trailing `Z`.** `2024-01-02T00:00:00Z`, never
   `2024-01-02T00:00:00+02:00` and never a naive local time. Enforced by pattern,
   not just by `format`, because `format` alone accepts offsets.
2. **Tickers are uppercase.** `A`–`Z` and dots only, so `BRK.B` is valid and
   `brk-b` is not.
3. **CIKs are zero-padded to exactly 10 characters.** `0000320193`, never `320193`
   — the SEC API returns both forms and they will not join otherwise.

Both schemas set `"additionalProperties": false` at the top level, and
`sec.filings.v1` sets it inside `facts` as well. An unexpected key is a failure,
not a passthrough.

## `market.prices.v1`

One OHLCV bar for one ticker over one interval.

| field | type | nullable | meaning |
|---|---|---|---|
| `schema_version` | integer, const `1` | no | Contract version. Always `1` on this topic. |
| `ticker` | string | no | Uppercase symbol, dots allowed (`BRK.B`). |
| `ts` | string (ISO 8601 UTC) | no | **Start** of the bar's period. |
| `open` | number | yes | Opening price. |
| `high` | number | yes | Highest price in the period. |
| `low` | number | yes | Lowest price in the period. |
| `close` | number | yes | Closing price. |
| `volume` | integer ≥ 0 | yes | Shares traded in the period. |
| `interval` | string enum | no | One of `1d`, `1h`, `5m`, `1m`. |
| `ingested_at` | string (ISO 8601 UTC) | no | When the producer emitted the message. |

`ts` is the period **start**, so a `1d` bar for 2 January is
`2024-01-02T00:00:00Z`. The OHLCV fields are nullable because sources do emit
gap bars; a null is a real "no value here", never a `0` and never a `NaN`.

## `sec.filings.v1`

One SEC filing with a fixed set of normalised financial facts.

| field | type | nullable | meaning |
|---|---|---|---|
| `schema_version` | integer, const `1` | no | Contract version. Always `1` on this topic. |
| `cik` | string | no | Central Index Key, zero-padded to 10 digits. |
| `ticker` | string | yes | Uppercase ticker. Null when the CIK maps to no listed ticker. |
| `accession_no` | string | no | Accession number **with dashes**: `0000320193-23-000106`. Unique per filing. |
| `form_type` | string | no | `10-K`, `10-Q`, `10-K/A`, `8-K`, … |
| `filed_date` | string `YYYY-MM-DD` | no | Date the SEC accepted the filing. |
| `fiscal_period` | string enum | no | `FY`, `Q1`–`Q4`. What the duration facts cover. |
| `period_start` | string `YYYY-MM-DD` | yes | First day of the reported period. Duration facts only. |
| `period_end` | string `YYYY-MM-DD` | yes | Last day of the period; also the as-of date for instant facts. |
| `facts` | object | no | Closed 19-key set, see below. |
| `ingested_at` | string (ISO 8601 UTC) | no | When the producer emitted the message. |

### Period semantics

This is the part that is easy to get silently wrong, so it is spelled out.

- **Duration facts** (income statement, cash flow) cover `period_start` →
  `period_end`. **Instant facts** (balance sheet) are as of `period_end`;
  `period_start` does not apply to them.
- **`fiscal_period` is mandatory** because an annual revenue and a quarterly
  revenue are otherwise indistinguishable — same field, ~4x magnitude apart.
- **Quarterly means the discrete quarter (~91 days), not year-to-date.** A `Q2`
  message covers April–June alone, so quarters are comparable to each other.
- **`period_start` is not derivable from `period_end`.** Apple's FY2023 is 370
  days — a 53-week fiscal year — and many retailers use 52/53-week years too.

### Which value the producer picks

A single filing carries the same tag several times over different windows. JPM's
Q2 2024 10-Q reports `NetIncomeLoss` four times:

```
2023-01-01 -> 2023-06-30   27,094,000,000   prior-year year-to-date
2023-04-01 -> 2023-06-30   14,472,000,000   prior-year quarter
2024-01-01 -> 2024-06-30   31,568,000,000   current year-to-date
2024-04-01 -> 2024-06-30   18,149,000,000   current quarter   <- emitted
```

The selection rule, in order:

1. **Drop every fact whose `end` is not the filing's `period_end`.** This removes
   the prior-year comparatives, which are the same tag reporting a different year.
2. **Keep the window matching `fiscal_period`** — target 365 days for `FY`
   (tolerance ±31), 91 days for `Q1`–`Q4` (tolerance ±20).
3. **Outside tolerance, emit `null`.** Never substitute a differently-based
   number for the one that was asked for.

Rule 3 has a visible consequence: **`operating_cash_flow` and `capex` are null on
most quarterly filings**, because cash-flow statements are tagged year-to-date
only and a 181-day window fails the 91-day target. That is deliberate — a YTD
figure sitting in a field labelled "discrete quarter" is worse than a null.
Deriving the discrete quarter by subtracting the prior quarter's YTD filing is a
possible later improvement; it is not done today.

### `facts` is a closed key set

Exactly these 19 keys, all nullable numbers, all **always present**. A fact the
filing does not report is `null` — never omitted, and no other tag is ever added.

**Why:** an open key set means Elasticsearch dynamic mapping creates a new field
per XBRL tag it sees. One Apple 10-K alone carries ~570 facts across 503 distinct
tags, so the index blows past the default 1000-field limit and the consumer
breaks — after the demo has started, on a machine that is not yours.

### The alias table

Canonical names are **ours**, not raw XBRL tags. The producer resolves each one
against an ordered list of tags, **first match wins**. This matters more than it
sounds: Apple stopped tagging `Revenues` in 2018 when ASC 606 landed, so a
literal lookup returns `null` for every modern Apple filing — the most important
number in the dataset, missing, with the pipeline reporting success.

| canonical | basis | XBRL tags, in priority order |
|---|---|---|
| `revenue` | duration | `RevenueFromContractWithCustomerExcludingAssessedTax`, `RevenueFromContractWithCustomerIncludingAssessedTax`, `Revenues`, `SalesRevenueNet` |
| `cost_of_revenue` | duration | `CostOfGoodsAndServicesSold`, `CostOfRevenue`, `CostOfGoodsSold` |
| `gross_profit` | duration | `GrossProfit` |
| `operating_income` | duration | `OperatingIncomeLoss` |
| `net_income` | duration | `NetIncomeLoss`, `ProfitLoss`, `NetIncomeLossAvailableToCommonStockholdersBasic` |
| `rnd_expense` | duration | `ResearchAndDevelopmentExpense` |
| `eps_basic` | duration | `EarningsPerShareBasic` |
| `eps_diluted` | duration | `EarningsPerShareDiluted` |
| `shares_diluted` | duration | `WeightedAverageNumberOfDilutedSharesOutstanding` |
| `assets` | instant | `Assets` |
| `assets_current` | instant | `AssetsCurrent` |
| `liabilities` | instant | `Liabilities` |
| `liabilities_current` | instant | `LiabilitiesCurrent` |
| `equity` | instant | `StockholdersEquity`, `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` |
| `cash` | instant | `CashAndCashEquivalentsAtCarryingValue`, `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents` |
| `long_term_debt` | instant | `LongTermDebtNoncurrent`, `LongTermDebt` |
| `shares_outstanding` | instant | `dei:EntityCommonStockSharesOutstanding`, `us-gaap:CommonStockSharesOutstanding` |
| `operating_cash_flow` | duration | `NetCashProvidedByUsedInOperatingActivities`, `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations` |
| `capex` | duration | `PaymentsToAcquirePropertyPlantAndEquipment` |

Note `shares_outstanding` crosses taxonomies: the `dei` cover-page tag resolves
for all six companies checked, while the `us-gaap` one is stale for Walmart. A
tag reference without a taxonomy prefix means `us-gaap`.

The messages themselves carry only the resolved numbers, not which tag supplied
them. Provenance lives in this table, which is version-controlled — that keeps
the message ~1 KB and the Elasticsearch mapping at 19 fact fields instead of 38.

### Nulls are informative, not failures

Coverage measured against real filings for AAPL, MSFT, JPM, XOM, TSLA, WMT:

- **Banks (JPM)** have no `gross_profit`, `cost_of_revenue`, `operating_income`,
  `assets_current` or `liabilities_current` — a bank's balance sheet is
  unclassified and it reports no gross margin. Nine of 19 facts populate.
- **Walmart** does not tag total `Liabilities` at all.
- **XOM** reports no `gross_profit` or `shares_diluted`.

So the dashboard must not assume any fact except `revenue`, `net_income`,
`assets`, `equity`, `cash`, `eps_basic` and `eps_diluted`, which resolved for all
six. Nothing downstream should treat a null as an error.

### Amendments and restatements

`10-K/A` restates a period already filed, under a **new** accession number. Both
messages are emitted; neither is suppressed. Consumers deduplicate by keeping
`max(filed_date)` per `(cik, fiscal_period, period_end)`. Because messages are
keyed by `cik`, all candidates for a given company land on one partition.

### Form types are not filtered

Do not assume `8-K` means "no financials" — Apple's earnings-release 8-Ks carry
569 facts. The producer emits whatever resolves, whatever the form.

## `sec.text.v1`

One block of narrative text from a filing. This is the project's unstructured
data: `market.prices.v1` and `sec.filings.v1` are both entirely numeric.

| field | type | nullable | meaning |
|---|---|---|---|
| `schema_version` | integer, const `1` | no | Contract version. Always `1` on this topic. |
| `cik` | string | no | Central Index Key, zero-padded to 10 digits. Joins to `sec.filings.v1`. |
| `ticker` | string | yes | Uppercase ticker. |
| `accession_no` | string | no | Accession of the parent filing. The join key to `sec.filings.v1`. |
| `form_type` | string | no | Form type of the parent filing. |
| `filed_date` | string `YYYY-MM-DD` | no | Date the SEC accepted the filing. |
| `section` | string enum | no | `press_release`, `risk_factors`, `mda`. |
| `source_document` | string | yes | Attachment the text came from, e.g. `EX-99.1`. |
| `title` | string | yes | Document headline, for display only. |
| `text` | string, non-empty | no | Plain text, HTML stripped, whitespace collapsed. |
| `chunk_index` | integer ≥ 0 | no | Position of this chunk within the section. |
| `chunk_total` | integer ≥ 1 | no | Number of chunks the section was split into. |
| `ingested_at` | string (ISO 8601 UTC) | no | When the producer emitted the message. |

### Only `press_release` is produced today

The `section` enum lists three values but the producer emits only
`press_release`: the `EX-99.1` exhibit of an earnings 8-K. `risk_factors` and
`mda` are **reserved** so that adding them later is an additive change rather
than a re-freeze — every consumer's Spark schema and Elasticsearch mapping would
otherwise need revisiting. Nothing emits them yet; do not wait for them.

Why press releases are the right first source:

- **They are dated to the day the stock moves.** An earnings 8-K lands the day
  before the matching 10-Q — Apple filed an 8-K on 2026-07-30 and the 10-Q on
  2026-07-31. That makes the join against a price anomaly meaningful rather than
  approximate.
- **They are small and clean.** ~11,000 characters, one exhibit, no parsing
  ambiguity. The sample message is 12 KB.
- **They are dense with opinion.** "Records", "strongest June quarter ever",
  "up 16 percent year over year" — management prose, not accounting boilerplate.

### Why not the 10-K narrative sections, yet

Risk Factors (Item 1A) and MD&A (Item 7) are richer, but extracting them is a
genuine parsing problem, not a lookup. Splitting a real Apple 10-K on `Item N`
headings finds all ten headings within 1,300 characters of each other — that is
the **table of contents**, not the sections. `Item 1A` matches twice. A naive
implementation extracts a table of contents and reports success. Doing it
properly means taking the last occurrence and requiring a minimum gap, then
chunking 357 KB of text. That is its own ticket.

### Chunking

`chunk_index` and `chunk_total` exist from day one and are `0` and `1` for
everything currently produced. They are in the contract now precisely so that
splitting long sections later does not change it.

When splitting becomes necessary, split on paragraph boundaries, keep each
message under ~900 KB (Kafka's default `max.message.bytes` is 1 MB), and keep
`accession_no` + `section` + `chunk_index` unique. Reassemble by ordering on
`chunk_index`.

### Text normalisation

`text` is plain text: HTML stripped, runs of spaces and tabs collapsed to one,
runs of blank lines collapsed to one, leading `Exhibit 99.1` boilerplate removed.
Unicode is preserved as-is — company names carry `®` and press releases use
typographic quotes, and mangling them into ASCII would corrupt the text for no
benefit. `text` is never empty; a section with nothing in it is not emitted.

Scoring this text is the consumer half's decision and is deliberately not
specified here. The producer's contract ends at delivering clean text.

## Why plain JSON

No Avro, no Protobuf, no Confluent Schema Registry. That is a deliberate tradeoff
for a two-week project, not an omission:

- A registry is a fourth container to run, wire, and debug on every teammate's
  laptop, and it is the piece most likely to fail in a live demo.
- Every consumer here (`spark.read.json`, `kafka-console-consumer`, `jq`) reads
  JSON natively, so a broken message can be diagnosed by eye.
- The cost is no wire-level enforcement — which is what these schemas and
  `scripts/validate_schemas.py` buy back.

If this project outlived the course, Avro plus a registry would be the right call.

## Where the data comes from

Filings are read from `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`,
which returns facts the SEC has **already extracted**. We never parse a filing
document. That file is ~3–7 MB per company for its entire history, and we keep 19
facts per filing out of ~570 — a ~30:1 projection.

Snapshot the raw companyfacts JSON to disk (gitignored) and project on the way to
Kafka, rather than projecting at fetch time. SEC rate-limits at 10 requests/sec,
and changing the fact list should be a local re-run, not a new crawl. Requests
need a `User-Agent` header identifying you, or the SEC returns 403.

## Validating

```bash
pip install -r scripts/requirements.txt
python scripts/validate_schemas.py
```

Validates every file in `samples/` against its matching schema and prints one
line per sample. Exits non-zero, naming the offending field, if anything drifts.

Producers do **not** validate on the hot path — validation lives in tests and in
this script. Sample files are one JSON message per line, so they pipe straight
into a broker with no editing:

```bash
kafka-console-producer --bootstrap-server localhost:29092 \
  --topic market.prices.v1 --property "parse.key=false" \
  < schemas/samples/market.prices.v1.json
```

The two filing samples are real: Apple's FY2023 10-K (all 19 facts populate) and
JPM's Q2 2024 10-Q (9 of 19, the rest genuinely not reported). They are extracted
from SEC data, not hand-written, so they are safe to assert against in tests.
