"""
Elasticsearch read layer for the dashboard.

Read-only by design. Nothing in this module creates, updates or deletes an
index — the Spark job (`spark/es_writer.py`) owns every write, and the dashboard
must never be able to corrupt what it is displaying.

Index names come from the same environment variables the pipeline writes with,
so renaming an index in `.env` moves both sides together and they cannot drift:

    stock_prices    PRICES_INDEX     one document per (ticker, interval, ts)
    stock_filings   FILINGS_INDEX    one document per accession_no
    stock_context   CONTEXT_INDEX    one per (ticker, interval, as_of)
    stock_analysis  ANALYSIS_INDEX   one per (ticker, interval, as_of)

One behaviour of the writer shapes everything here: `es_writer._clean()` drops
null fields from the document entirely, so a fact a company never reported is an
**absent key**, not a null. Rebuilding hits into a DataFrame therefore produces
missing columns rather than columns of null, and every consumer downstream has
to tolerate that. `kpis._num()` is the counterpart that does.
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
from elasticsearch import Elasticsearch

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
PRICES_INDEX = os.getenv("PRICES_INDEX") or "stock_prices"
FILINGS_INDEX = os.getenv("FILINGS_INDEX") or "stock_filings"
CONTEXT_INDEX = os.getenv("CONTEXT_INDEX") or "stock_context"
ANALYSIS_INDEX = os.getenv("ANALYSIS_INDEX") or "stock_analysis"

# `or` rather than a default argument on getenv: docker-compose passes variables
# through `env_file`, which turns a deliberately blank entry into an empty
# string rather than leaving it absent. `os.getenv(name, default)` then returns
# "" and the default never applies. This is the same trap that crashed the Spark
# job on every fresh `.env` copy (see spark/pipeline.py:40-54).

# The whole snapshot is ~2,500 price bars and ~900 filings, so a single search
# returns everything for one company comfortably. This ceiling exists to fail
# visibly rather than silently truncate if the universe grows.
MAX_HITS = 10_000


def connect(host: str | None = None) -> Elasticsearch:
    return Elasticsearch(host or ES_HOST, request_timeout=30)


def _hits_to_df(resp) -> pd.DataFrame:
    """Search response -> DataFrame, one row per document.

    Columns are the union of keys present across the hits. A field absent from
    every hit yields no column at all, which is exactly what the KPI layer is
    written to tolerate.
    """
    rows = [h["_source"] for h in resp["hits"]["hits"]]
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def index_status(es: Elasticsearch) -> dict[str, int | None]:
    """Document count per index, or None where the index does not exist.

    Surfaced in the sidebar so an empty dashboard immediately says which stage
    has not been run, instead of showing blank charts and leaving the reader to
    guess. `_count` rather than `_cat/indices` on purpose: `stock_context` has a
    `nested` field (`top_anomalies`), and `_cat/indices` counts hidden Lucene
    documents — it reports 60 where there are 10.
    """
    out = {}
    for name in (PRICES_INDEX, FILINGS_INDEX, CONTEXT_INDEX, ANALYSIS_INDEX):
        try:
            out[name] = (es.count(index=name)["count"]
                         if es.indices.exists(index=name) else None)
        except Exception:                                   # noqa: BLE001
            out[name] = None
    return out


def list_tickers(es: Elasticsearch) -> list[str]:
    """Every ticker that has at least one filing, derived from the data itself.

    A terms aggregation rather than a hardcoded list: the pipeline's rule is that
    adding an instrument upstream needs no code change downstream, and a
    hardcoded selector would break that at the last step.
    """
    if not es.indices.exists(index=FILINGS_INDEX):
        return []
    resp = es.search(
        index=FILINGS_INDEX, size=0,
        aggs={"tickers": {"terms": {"field": "ticker", "size": 500,
                                    "order": {"_key": "asc"}}}},
    )
    return [b["key"] for b in resp["aggregations"]["tickers"]["buckets"]]


def fetch_filings(es: Elasticsearch, ticker: str) -> pd.DataFrame:
    """Every filing for one company, newest period first.

    Annual and quarterly are both returned; `kpis.annual_frame` does the FY
    filter. Quarterly rows are kept because the raw-data table in the UI shows
    them, and because a future quarterly view needs no new query.
    """
    if not es.indices.exists(index=FILINGS_INDEX):
        return pd.DataFrame()
    resp = es.search(
        index=FILINGS_INDEX, size=MAX_HITS,
        query={"term": {"ticker": ticker}},
        sort=[{"period_end": {"order": "desc"}}],
    )
    return _hits_to_df(resp)


def fetch_prices(es: Elasticsearch, ticker: str,
                 interval: str = "1d") -> pd.DataFrame:
    """Daily price bars for one company, oldest first.

    Filtered to a single interval because every window and ratio in this project
    is keyed on (ticker, interval) — mixing a daily and an hourly bar in one
    series produces wrong numbers with no error to notice.
    """
    if not es.indices.exists(index=PRICES_INDEX):
        return pd.DataFrame()
    resp = es.search(
        index=PRICES_INDEX, size=MAX_HITS,
        query={"bool": {"filter": [{"term": {"ticker": ticker}},
                                   {"term": {"interval": interval}}]}},
        sort=[{"ts": {"order": "asc"}}],
    )
    df = _hits_to_df(resp)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def fetch_latest_analysis(es: Elasticsearch, ticker: str,
                          source: str | None = None) -> dict | None:
    """The most recent LLM analyst note produced by the Spark stage, if any.

    Returns None when the analyst stage was skipped (no API key, or
    `LLM_ENABLED=false`) — a supported and documented way to run the pipeline,
    so the dashboard treats its absence as normal rather than as an error.

    This is the note written by `spark/llm.py` and grounded on the price and
    anomaly context. It is displayed separately from the dashboard's own
    recommendation, which is grounded on the seven fundamentals KPIs instead.
    """
    if not es.indices.exists(index=ANALYSIS_INDEX):
        return None
    must = [{"term": {"ticker": ticker}}]
    if source:
        # Two analysts write to this index. Without filtering, a panel would
        # happily render the other one's note as its own.
        must.append({"term": {"source": source}})
    resp = es.search(
        index=ANALYSIS_INDEX, size=1,
        query={"bool": {"filter": must}},
        sort=[{"as_of": {"order": "desc"}}],
    )
    hits = resp["hits"]["hits"]
    return hits[0]["_source"] if hits else None


def fetch_latest_context(es: Elasticsearch, ticker: str) -> dict | None:
    """The stage-3 aggregate the Spark analyst was shown: anomalies, price summary.

    Used to enrich the dashboard's own AI panel with the MLlib anomaly findings,
    so the recommendation can reference both the fundamentals computed here and
    the unusual trading days KMeans flagged upstream.
    """
    if not es.indices.exists(index=CONTEXT_INDEX):
        return None
    resp = es.search(
        index=CONTEXT_INDEX, size=1,
        query={"term": {"ticker": ticker}},
        sort=[{"as_of": {"order": "desc"}}],
    )
    hits = resp["hits"]["hits"]
    return hits[0]["_source"] if hits else None


# ---------------------------------------------------------------------------
# Writing back
# ---------------------------------------------------------------------------
# The only write in this module. Everything else here reads, because the Spark
# job owned every index until the analyst stage moved to being on demand — a
# note is now produced when a reader asks for one, and it has to survive the
# browser session that asked.
ANALYSIS_MAPPING = {
    "properties": {
        "ticker": {"type": "keyword"},
        "interval": {"type": "keyword"},
        "as_of": {"type": "date"},
        "recommendation": {"type": "keyword"},
        "confidence": {"type": "keyword"},
        "provider_used": {"type": "keyword"},
        "model_used": {"type": "keyword"},
        # Which path produced this note. The Spark batch stage and this
        # dashboard reason over different evidence — prices plus anomalies plus
        # filing text there, the seven fundamentals plus an anomaly summary here
        # — so a reader comparing two notes needs to know which is which.
        "source": {"type": "keyword"},
        # The reader's emphasis, when one was given. A note shown without the
        # instruction that shaped it is not reproducible.
        "focus_used": {"type": "text"},
        "key_risks": {"type": "text"},
        "signals": {"type": "text"},
        "summary": {"type": "text"},
        "error": {"type": "keyword"},
    }
}


def write_analysis(es: Elasticsearch, ticker: str, parsed: dict,
                   provider: str, model: str, interval: str = "1d",
                   source: str = "dashboard") -> str:
    """Store one analyst note, keyed exactly as the Spark stage keys its own.

    Keyed `ticker|interval|as_of|source`. The source is part of the id because
    two analysts now write here over different evidence -- the fundamentals
    panel and the anomaly panel -- and without it the second click of the day
    would overwrite the first analyst's note with the other's. Asking the SAME
    analyst twice in a day still overwrites, which is what you want.

    Returns the document id. Raises whatever Elasticsearch raises: a failed
    write must not be reported to the reader as a successful one.
    """
    as_of = date.today().isoformat()
    doc = {
        "ticker": ticker,
        "interval": interval,
        "as_of": as_of,
        "source": source,
        "provider_used": provider,
        "model_used": model,
        "recommendation": parsed.get("recommendation"),
        "confidence": parsed.get("confidence"),
        "key_risks": parsed.get("key_risks") or [],
        "signals": parsed.get("signals") or [],
        "summary": parsed.get("summary"),
    }
    if parsed.get("focus_used"):
        doc["focus_used"] = parsed["focus_used"]
    # Drop empties rather than storing explicit nulls, matching how the Spark
    # writer treats a missing value.
    doc = {k: v for k, v in doc.items() if v not in (None, "", [])}

    if not es.indices.exists(index=ANALYSIS_INDEX):
        es.indices.create(index=ANALYSIS_INDEX, mappings=ANALYSIS_MAPPING)

    doc_id = f"{ticker}|{interval}|{as_of}|{source}"
    es.index(index=ANALYSIS_INDEX, id=doc_id, document=doc, refresh=True)
    return doc_id
