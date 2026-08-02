# Big Data & AI — Stock Anomaly Detection + News Sentiment

End-to-end big data pipeline for the course project.

**Pipeline:** `yfinance → Kafka → Spark (transform + MLlib anomaly detection) → Elasticsearch → Streamlit`

- **Data (semi-structured + unstructured):** daily price bars *and* free-text news headlines, pulled live from yfinance as JSON.
- **Course technologies:** Docker, Apache Kafka (KRaft mode), Apache Spark, Elasticsearch, Streamlit.
- **AI capability (Spark MLlib):** KMeans-based **anomaly detection** on engineered price features — days that sit far from every cluster centre are flagged as unusual. News headlines are additionally scored for **sentiment** (VADER).
- **Insight:** which days each stock behaved abnormally, and whether news sentiment lines up with those days.

Everything runs locally in Docker and is **free** — no paid APIs, no keys.

## Architecture

```
                 ┌─────────────┐   prices topic   ┌──────────────────────┐
  yfinance  ───▶ │  producer   │ ───────────────▶ │        Kafka         │
 (prices+news)   │  (Python)   │   news topic     │   (KRaft, 1 node)    │
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
# 1. Copy the config and (optionally) edit the tickers.
cp .env.example .env

# 2. Start the infrastructure and wait for it to be healthy.
docker compose up -d kafka elasticsearch

# 3. Pull data from yfinance into Kafka (runs once, then exits).
docker compose run --rm producer

# 4. Run the Spark pipeline: transform + anomaly detection + load into Elasticsearch.
#    (First run downloads the Kafka connector jar — needs internet, ~1 min.)
docker compose run --rm spark

# 5. Start the dashboard and open http://localhost:8501
docker compose up -d dashboard

# When finished:
docker compose down
```

## Repository layout & team split

| Folder        | Stage                              | Owner     |
|---------------|------------------------------------|-----------|
| `producer/`   | Ingest: yfinance → Kafka           | Person A  |
| `spark/`      | Transform + MLlib anomaly detection| Person B  |
| `spark/` (ES load) + `dashboard/` | Load + results/dashboard | Person C |

Each stage passes data by a defined schema (see `pipeline.py`), so the three
parts can be built and tested independently.

## Configuration (`.env`)

| Variable           | Meaning                                          |
|--------------------|--------------------------------------------------|
| `TICKERS`          | Comma-separated tickers to pull                  |
| `PRICE_PERIOD`     | History length (e.g. `2y`)                        |
| `ANOMALY_FRACTION` | Fraction of days flagged as anomalies (e.g. 0.05)|

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

Price and news data from Yahoo Finance via the open-source
[`yfinance`](https://github.com/ranaroussi/yfinance) library. For educational
use only.
