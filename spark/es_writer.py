"""
Elasticsearch load: streamed, batched, idempotent.

Three properties matter here, and the previous implementation had none of them.

  * **Streamed, not collected.** `df.toPandas()` pulls the entire DataFrame into
    driver memory. That is invisible at a few thousand rows and fatal at a few
    million. `toLocalIterator()` walks the DataFrame one partition at a time, so
    driver memory is bounded by the largest partition rather than by the dataset.

  * **Upsert, not delete-and-recreate.** Dropping the index at the start of every
    run makes an incremental load a full rebuild, and leaves an empty index
    behind if the run dies halfway. Indices are created if missing and documents
    are indexed by id.

  * **Deterministic ids.** Every document's `_id` is derived from its natural
    key, so re-running the pipeline overwrites the same documents instead of
    duplicating them. This is what lets stage 1 read `earliest -> latest` every
    time and stay correct: the job is a full reprocess with no offset state to
    track, and running it twice is the same as running it once.
"""
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import functions as F

# NaN reaches Elasticsearch as invalid JSON. Spark nulls survive `Row.asDict()`
# as None, but a float NaN produced by a division does not, so it is filtered on
# the way out — the same "kill NaN before it becomes JSON" rule the producer
# side documents for the parquet snapshots.
import math


PRICE_MAPPING = {
    "properties": {
        "ticker": {"type": "keyword"},
        "interval": {"type": "keyword"},
        "ts": {"type": "date"},
        "date": {"type": "date"},
        "open": {"type": "float"}, "high": {"type": "float"},
        "low": {"type": "float"}, "close": {"type": "float"},
        "volume": {"type": "long"},
        "daily_return": {"type": "float"},
        "volume_change": {"type": "float"},
        "intraday_range_pct": {"type": "float"},
        "ma_7": {"type": "float"}, "ma_30": {"type": "float"},
        "volatility_10": {"type": "float"},
        "avg_volume_20": {"type": "float"},
        "volume_ratio": {"type": "float"},
        "cluster": {"type": "integer"},
        "anomaly_score": {"type": "float"},
        "is_anomaly": {"type": "boolean"},
    }
}

FILING_MAPPING = {
    "properties": {
        "cik": {"type": "keyword"}, "ticker": {"type": "keyword"},
        "accession_no": {"type": "keyword"}, "form_type": {"type": "keyword"},
        "fiscal_period": {"type": "keyword"},
        "filed_date": {"type": "date"},
        "period_start": {"type": "date"}, "period_end": {"type": "date"},
        "gross_margin": {"type": "float"}, "operating_margin": {"type": "float"},
        "net_margin": {"type": "float"}, "return_on_equity": {"type": "float"},
        "current_ratio": {"type": "float"}, "debt_to_equity": {"type": "float"},
        "rnd_intensity": {"type": "float"},
        "revenue_yoy": {"type": "float"}, "net_income_yoy": {"type": "float"},
    }
}

# The stage-3 aggregate, stored so that what the analyst was shown is
# inspectable rather than built in memory and discarded. `context_json` is the
# literal blob embedded in the prompt; the flattened fields beside it exist so
# the same document can be filtered and charted without parsing that string.
CONTEXT_MAPPING = {
    "properties": {
        "ticker": {"type": "keyword"},
        "interval": {"type": "keyword"},
        "as_of": {"type": "date"},
        "bar_count": {"type": "long"},
        "first_date": {"type": "date"}, "last_date": {"type": "date"},
        "latest_close": {"type": "float"}, "latest_ma_30": {"type": "float"},
        "avg_daily_return": {"type": "float"},
        "return_volatility": {"type": "float"},
        "avg_volume": {"type": "float"},
        "anomaly_count": {"type": "long"},
        "anomalies_near_filing": {"type": "long"},
        "top_anomalies": {
            "type": "nested",
            "properties": {
                "date": {"type": "date"},
                "daily_return": {"type": "float"},
                "volume_ratio": {"type": "float"},
                "anomaly_score": {"type": "float"},
            },
        },
        "latest_form_type": {"type": "keyword"},
        "latest_fiscal_period": {"type": "keyword"},
        "latest_filed_date": {"type": "date"},
        "revenue": {"type": "double"}, "net_income": {"type": "double"},
        "eps_diluted": {"type": "float"},
        "gross_margin": {"type": "float"}, "net_margin": {"type": "float"},
        "return_on_equity": {"type": "float"},
        "current_ratio": {"type": "float"}, "debt_to_equity": {"type": "float"},
        "revenue_yoy": {"type": "float"}, "net_income_yoy": {"type": "float"},
        "filing_text_available": {"type": "boolean"},
        "filing_text_truncated": {"type": "boolean"},
        "filing_text_chars": {"type": "long"},
        "filing_text_section": {"type": "keyword"},
        "prompt_chars": {"type": "long"},
        "context_json": {"type": "text", "index": False},
    }
}

ANALYSIS_MAPPING = {
    "properties": {
        "ticker": {"type": "keyword"},
        "interval": {"type": "keyword"},
        "as_of": {"type": "date"},
        "recommendation": {"type": "keyword"},
        "confidence": {"type": "keyword"},
        # Which model actually wrote this note. A 3B local fallback and a 70B
        # hosted model are not interchangeable, and the index should never leave
        # that ambiguous.
        "provider_used": {"type": "keyword"},
        "model_used": {"type": "keyword"},
        "key_risks": {"type": "text"},
        "signals": {"type": "text"},
        "summary": {"type": "text"},
        "error": {"type": "keyword"},
    }
}


def connect(host, retries=5, delay=3):
    import time
    last = None
    for attempt in range(retries):
        try:
            es = Elasticsearch(host, request_timeout=60)
            if es.ping():
                return es
            last = RuntimeError("ping returned False")
        except Exception as exc:                       # noqa: BLE001
            last = exc
        if attempt < retries - 1:
            print(f"[es] not ready ({last}), retrying in {delay}s")
            time.sleep(delay)
    raise RuntimeError(f"Elasticsearch at {host} unreachable: {last}")


def ensure_index(es, index, mapping):
    """Create the index with an explicit mapping if it does not already exist.

    Explicit mappings rather than dynamic ones: the filings documents carry 19
    numeric fact fields that are frequently null, and dynamic mapping would infer
    a type from whichever document happened to arrive first.
    """
    if not es.indices.exists(index=index):
        es.indices.create(index=index, mappings=mapping)
        print(f"[es] created index '{index}'")


def _clean(d):
    """Drop NaN/inf and empty-string dates; keep real nulls out of the document.

    A null fact is meaningful — "the company did not report this" — but there is
    no value in storing an explicit null in every document, so absent keys carry
    that meaning and the mapping keeps the field queryable regardless.
    """
    out = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            continue
        out[k] = v
    return out


def write_df(es, df, index, mapping, id_cols, batch_size=1000,
             date_cols=("ts", "date", "filed_date", "period_start", "period_end")):
    """Stream a DataFrame into an index, batching the bulk requests.

    `id_cols` is the natural key. Its values are joined with '|' to form `_id`,
    which is what makes the write an upsert rather than an append.
    """
    ensure_index(es, index, mapping)

    # Dates leave Spark as date/timestamp objects; Elasticsearch wants strings.
    casted = df
    for c in date_cols:
        if c in casted.columns:
            casted = casted.withColumn(c, F.col(c).cast("string"))

    def actions():
        for row in casted.toLocalIterator():
            d = _clean(row.asDict(recursive=True))
            key = "|".join(str(d.get(c, "")) for c in id_cols)
            yield {"_index": index, "_id": key, "_source": d}

    ok, errors = helpers.bulk(es, actions(), chunk_size=batch_size,
                              raise_on_error=False)
    es.indices.refresh(index=index)
    if errors:
        print(f"[es] '{index}': {ok} indexed, {len(errors)} FAILED")
        for e in errors[:3]:
            print(f"[es]   {e}")
    else:
        print(f"[es] '{index}': {ok} documents indexed")
    return ok, errors


def write_contexts(es, contexts, index, as_of, batch_size=100):
    """Index the exact context each instrument's analyst prompt was built from.

    Written before the API is called, and independently of whether it is called
    at all, so the record of what the analyst was given survives a quota failure
    or a run with `LLM_ENABLED=false`.
    """
    ensure_index(es, index, CONTEXT_MAPPING)

    def actions():
        for c in contexts:
            ctx = c["context"]
            price, anom = ctx["price_history"], ctx["anomalies"]
            filing, textinfo = ctx["latest_filing"], ctx["filing_text"]
            body = c["context"].get("filing_text", {}).get("text") or ""
            doc = _clean({
                "ticker": c["ticker"], "interval": c["interval"], "as_of": as_of,
                "bar_count": price.get("bars"),
                "first_date": price.get("from"), "last_date": price.get("to"),
                "latest_close": price.get("latest_close"),
                "latest_ma_30": price.get("latest_ma_30"),
                "avg_daily_return": price.get("avg_daily_return"),
                "return_volatility": price.get("return_volatility"),
                "avg_volume": price.get("avg_volume"),
                "anomaly_count": anom.get("total_flagged"),
                "anomalies_near_filing": anom.get("near_a_filing_date"),
                "top_anomalies": anom.get("most_extreme") or [],
                "latest_form_type": filing.get("form_type"),
                "latest_fiscal_period": filing.get("fiscal_period"),
                "latest_filed_date": filing.get("filed_date"),
                "revenue": filing.get("revenue"),
                "net_income": filing.get("net_income"),
                "eps_diluted": filing.get("eps_diluted"),
                "gross_margin": filing.get("gross_margin"),
                "net_margin": filing.get("net_margin"),
                "return_on_equity": filing.get("return_on_equity"),
                "current_ratio": filing.get("current_ratio"),
                "debt_to_equity": filing.get("debt_to_equity"),
                "revenue_yoy": filing.get("revenue_yoy"),
                "net_income_yoy": filing.get("net_income_yoy"),
                "filing_text_available": textinfo.get("available"),
                "filing_text_truncated": textinfo.get("truncated"),
                "filing_text_chars": len(body),
                "filing_text_section": textinfo.get("section"),
                "prompt_chars": c.get("prompt_chars"),
                "context_json": c["context_json"],
            })
            yield {"_index": index,
                   "_id": f"{c['ticker']}|{c['interval']}|{as_of}",
                   "_source": doc}

    ok, errors = helpers.bulk(es, actions(), chunk_size=batch_size,
                              raise_on_error=False)
    es.indices.refresh(index=index)
    print(f"[es] '{index}': {ok} analyst contexts indexed"
          + (f", {len(errors)} FAILED" if errors else ""))
    if errors:
        for e in errors[:2]:
            print(f"[es]   {e}")
    return ok, errors


def write_analyses(es, results, index, as_of, batch_size=100):
    """Write the LLM output. Keyed by ticker|interval|as_of.

    Including the run date in the id keeps one analysis per ticker per day —
    re-running today overwrites today's note rather than appending a duplicate,
    while yesterday's is preserved as history.

    **Failed analyses are not written.** Because the id is stable within a day, a
    failure would upsert an empty document over a good one produced by an earlier
    run the same day — a rate-limited retry would destroy the very results it was
    meant to add to. Skipping them leaves the good document in place. The
    failures are not hidden: they are logged per ticker as they happen, counted
    here, and their .txt reports are removed so nothing stale is left behind.
    """
    ensure_index(es, index, ANALYSIS_MAPPING)

    good = [r for r in results if not r.get("error")]
    failed = [r["ticker"] for r in results if r.get("error")]

    if not good:
        print(f"[es] '{index}': nothing written - all "
              f"{len(results)} analyses failed")
        return 0, []

    def actions():
        for r in good:
            doc = _clean({**r, "as_of": as_of})
            yield {"_index": index,
                   "_id": f"{r['ticker']}|{r['interval']}|{as_of}",
                   "_source": doc}

    ok, errors = helpers.bulk(es, actions(), chunk_size=batch_size,
                              raise_on_error=False)
    es.indices.refresh(index=index)
    msg = f"[es] '{index}': {ok} analyses indexed"
    if failed:
        msg += (f"; {len(failed)} not written (call failed, any previous "
                f"document left intact): {', '.join(failed)}")
    if errors:
        msg += f"; {len(errors)} REJECTED by Elasticsearch"
    print(msg)
    return ok, errors
