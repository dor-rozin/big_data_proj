"""
Spark pipeline: Kafka -> tabular -> MLlib -> LLM analyst -> Elasticsearch.

    Stage 1   read the three frozen topics from Kafka (bounded batch)
    Stage 2   parse and transform into analysis-ready tables
    Stage 2b  MLlib KMeans flags statistically unusual bars
    Stage 3   aggregate to one row per (ticker, interval)
    Stage 4   an LLM writes an analyst note per instrument (LLM_PROVIDER)
    Stage 5   write the tables and the notes to Elasticsearch (+ .txt reports)

The division of labour between 2b and 4 is the point of the design. An LLM
cannot scan thousands of price bars or do reliable arithmetic over them; KMeans
can, cheaply and deterministically. So the model finds *where to look* and the
LLM says *what it means*. Without stage 2b the prompt would carry a coarse
summary and produce a generic note.

Everything is keyed on (ticker, interval) and derived from whatever arrived on
the topics — no ticker list, no date range and no interval is hardcoded, so the
job absorbs new instruments and new timeframes without a code change.

Run:  docker compose --profile jobs run --rm spark
"""
import os
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schemas import (PRICE_SCHEMA, FILING_SCHEMA, TEXT_SCHEMA,
                     read_topic, assert_parsed)
from transforms import (transform_prices, transform_filings, transform_text,
                        aggregate_for_llm)
from anomaly import detect_anomalies
import es_writer
import llm


# ---- config ---------------------------------------------------------------
def _env(name, default):
    return os.getenv(name, default)


def _int(name, default):
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


def _float(name, default):
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return default


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka:9092")
PRICES_TOPIC = _env("PRICES_TOPIC", "market.prices.v1")
FILINGS_TOPIC = _env("FILINGS_TOPIC", "sec.filings.v1")
TEXT_TOPIC = _env("TEXT_TOPIC", "sec.text.v1")

ES_HOST = _env("ES_HOST", "http://elasticsearch:9200")
PRICES_INDEX = _env("PRICES_INDEX", "stock_prices")
FILINGS_INDEX = _env("FILINGS_INDEX", "stock_filings")
ANALYSIS_INDEX = _env("ANALYSIS_INDEX", "stock_analysis")
CONTEXT_INDEX = _env("CONTEXT_INDEX", "stock_context")
ES_BATCH_SIZE = _int("ES_BATCH_SIZE", 1000)

KMEANS_K = _int("KMEANS_K", 3)
ANOMALY_FRACTION = _float("ANOMALY_FRACTION", 0.05)
MIN_ROWS_PER_GROUP = _int("MIN_ROWS_PER_GROUP", 30)

# Ordered provider chain. The first entry is the primary; each subsequent one
# takes over when the previous is retired (budget gone, bad key, unreachable).
LLM_PROVIDER = _env("LLM_PROVIDER", "groq").strip().lower()
LLM_FALLBACKS = [p.strip().lower()
                 for p in _env("LLM_FALLBACK_PROVIDERS", "").split(",")
                 if p.strip()]
GEMINI_API_KEY = _env("GEMINI_API_KEY", "")
GEMINI_MODEL = _env("GEMINI_MODEL", "gemini-3.6-flash")
GROQ_API_KEY = _env("GROQ_API_KEY", "")
GROQ_MODEL = _env("GROQ_MODEL", "llama-3.3-70b-versatile")
OLLAMA_MODEL = _env("OLLAMA_MODEL", "llama3.2:3b")
OLLAMA_URL = _env("OLLAMA_URL", "http://ollama:11434")

# Per-provider key, model, pacing and timeout.
#
# Pacing differs because the constraints differ: Gemini's free tier is ~5
# requests/minute, Groq's is generous per-minute but capped on daily tokens, and
# Ollama is a local container with no limit at all. The timeouts differ for the
# same reason — 90s means "dead" for a hosted API and "still thinking" for
# CPU-bound local inference, which on this hardware takes 30-60s per call.
_PROVIDER_DEFAULTS = {
    "gemini": {"api_key": GEMINI_API_KEY, "model": GEMINI_MODEL,
               "min_interval": 13.0, "timeout": 90},
    "groq": {"api_key": GROQ_API_KEY, "model": GROQ_MODEL,
             "min_interval": 1.0, "timeout": 90},
    "ollama": {"api_key": "", "model": OLLAMA_MODEL,
               "min_interval": 0.0, "timeout": 600, "base_url": OLLAMA_URL},
}


def _build_chain():
    """Primary first, then each fallback, skipping unknown or duplicate names."""
    chain, seen = [], set()
    for name in [LLM_PROVIDER] + LLM_FALLBACKS:
        if name in seen:
            continue
        cfg = _PROVIDER_DEFAULTS.get(name)
        if cfg is None:
            print(f"[llm] ignoring unknown provider {name!r}; expected one of "
                  f"{sorted(_PROVIDER_DEFAULTS)}")
            continue
        seen.add(name)
        chain.append({"name": name, **cfg})
    return chain


LLM_CHAIN = _build_chain()
_default_interval = LLM_CHAIN[0]["min_interval"] if LLM_CHAIN else 1.0
LLM_ENABLED = _env("LLM_ENABLED", "true").lower() not in {"false", "0", "no"}
LLM_CONCURRENCY = _int("LLM_CONCURRENCY", 1)
LLM_MIN_INTERVAL = _float("LLM_MIN_INTERVAL_SECONDS", _default_interval)
LLM_MAX_CALLS = _int("LLM_MAX_CALLS", 50)
LLM_MAX_TEXT_CHARS = _int("LLM_MAX_TEXT_CHARS", 6000)
LLM_MAX_COLLECT_ROWS = _int("LLM_MAX_COLLECT_ROWS", 500)
LLM_OUTPUT_DIR = _env("LLM_OUTPUT_DIR", "/app/llm_output")
PROMPT_PATH = _env("PROMPT_PATH", os.path.join(os.path.dirname(__file__),
                                               "prompts", "analyst.md"))

SHUFFLE_PARTITIONS = _env("SHUFFLE_PARTITIONS", "8")
KAFKA_PACKAGE = _env("KAFKA_PACKAGE",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")


def banner(text):
    print(f"\n{'=' * 70}\n  {text}\n{'=' * 70}")


def main():
    spark = (SparkSession.builder
             .appName("stock-analysis-pipeline")
             .master("local[*]")
             .config("spark.jars.packages", KAFKA_PACKAGE)
             .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---- Stage 1: read Kafka ---------------------------------------------
    banner("Stage 1 - read Kafka")
    raw_prices = read_topic(spark, KAFKA_BOOTSTRAP, PRICES_TOPIC, PRICE_SCHEMA)
    raw_filings = read_topic(spark, KAFKA_BOOTSTRAP, FILINGS_TOPIC, FILING_SCHEMA)
    raw_text = read_topic(spark, KAFKA_BOOTSTRAP, TEXT_TOPIC, TEXT_SCHEMA)

    n_prices = assert_parsed(raw_prices, PRICES_TOPIC, "ticker")
    assert_parsed(raw_filings, FILINGS_TOPIC, "accession_no")
    assert_parsed(raw_text, TEXT_TOPIC, "accession_no")

    if n_prices == 0:
        print(f"\n[spark] {PRICES_TOPIC} is empty - nothing to do.\n"
              f"        Load it first:  docker compose run --rm producer")
        spark.stop()
        return 1

    # ---- Stage 2: transform ----------------------------------------------
    banner("Stage 2 - transform to tabular")
    prices = transform_prices(raw_prices).cache()
    filings = transform_filings(raw_filings).cache()
    text = transform_text(raw_text).cache()
    print(f"[spark] prices  : {prices.count()} bars")
    print(f"[spark] filings : {filings.count()} filings (after restatement dedup)")
    print(f"[spark] text    : {text.count()} documents")

    groups = prices.select("ticker", "interval").distinct().collect()
    print(f"[spark] groups  : {len(groups)} (ticker, interval) combinations")

    # ---- Stage 2b: MLlib anomaly detection -------------------------------
    banner("Stage 2b - MLlib KMeans anomaly detection")
    prices = detect_anomalies(prices, k=KMEANS_K, fraction=ANOMALY_FRACTION,
                              min_rows=MIN_ROWS_PER_GROUP).cache()

    # ---- Stage 3: aggregate ----------------------------------------------
    banner("Stage 3 - aggregate for the analyst")
    agg = aggregate_for_llm(prices, filings, text).cache()
    n_agg = agg.count()
    print(f"[spark] aggregated to {n_agg} row(s), one per (ticker, interval)")

    # ---- Stage 3b: materialise the analyst context ------------------------
    # Built and persisted before the API is involved, and regardless of whether
    # the API is called at all. Otherwise the one thing you cannot inspect is
    # exactly what the model was shown — which is the first thing you want when
    # an answer looks wrong, and the last thing you want to spend a scarce
    # free-tier call to discover.
    banner("Stage 3b - materialise the analyst context")
    rows, contexts = [], []
    if n_agg > LLM_MAX_COLLECT_ROWS:
        # The aggregate is collected to the driver. It is small by construction,
        # but a guard beats an OOM if the grain ever changes.
        print(f"[llm] aggregate has {n_agg} rows, above "
              f"LLM_MAX_COLLECT_ROWS={LLM_MAX_COLLECT_ROWS} - skipping the "
              f"analyst stages to avoid collecting a large DataFrame")
    else:
        rows = agg.orderBy("ticker", "interval").collect()
        contexts = llm.dump_prompts(
            rows,
            prompt_template=llm.load_prompt(PROMPT_PATH),
            out_dir=os.path.join(LLM_OUTPUT_DIR, "_prompts"),
            max_text_chars=LLM_MAX_TEXT_CHARS,
        )

    # ---- Stage 4: Gemini --------------------------------------------------
    banner(f"Stage 4 - LLM analyst ({' -> '.join(p['name'] for p in LLM_CHAIN)})")
    results = []
    if not rows:
        print("[llm] no context rows - skipping")
    elif not LLM_ENABLED:
        print("[llm] LLM_ENABLED=false - skipping the API call.\n"
              f"      The prompts that would have been sent are in "
              f"{LLM_OUTPUT_DIR}/_prompts/")
    elif not LLM_CHAIN:
        print("[llm] no usable provider configured - skipping")
    elif not any(p.get("api_key") or p["name"] == "ollama" for p in LLM_CHAIN):
        print("[llm] no API key set for any provider in the chain - skipping.\n"
              "      Groq:   https://console.groq.com/keys\n"
              "      Gemini: https://aistudio.google.com/apikey\n"
              "      Or add 'ollama' to LLM_FALLBACK_PROVIDERS and start it:\n"
              "        docker compose --profile local-llm up -d ollama")
    else:
        chain = [dict(p, min_interval=(LLM_MIN_INTERVAL
                                       if os.getenv("LLM_MIN_INTERVAL_SECONDS")
                                       else p["min_interval"]))
                 for p in LLM_CHAIN]
        for i, p in enumerate(chain):
            role = "primary" if i == 0 else f"fallback {i}"
            print(f"[llm] {role}: {p['name']}/{p['model']} "
                  f"(pacing {p['min_interval']:g}s, timeout {p['timeout']}s)")
        results = llm.analyse_rows(
            rows,
            prompt_template=llm.load_prompt(PROMPT_PATH),
            providers=chain,
            max_calls=LLM_MAX_CALLS,
            max_text_chars=LLM_MAX_TEXT_CHARS,
            concurrency=LLM_CONCURRENCY,
        )
        llm.write_reports(results, LLM_OUTPUT_DIR)
        failed = [r["ticker"] for r in results if r.get("error")]
        if failed:
            print(f"[llm] {len(failed)} of {len(results)} failed: "
                  f"{', '.join(failed)}")

    # ---- Stage 5: Elasticsearch ------------------------------------------
    banner("Stage 5 - load Elasticsearch")
    es = es_writer.connect(ES_HOST)

    price_cols = [c for c in prices.columns if c not in {"event_ts", "schema_version"}]
    es_writer.write_df(es, prices.select(*price_cols), PRICES_INDEX,
                       es_writer.PRICE_MAPPING,
                       id_cols=["ticker", "interval", "ts"],
                       batch_size=ES_BATCH_SIZE)

    if filings.count() > 0:
        filing_cols = [c for c in filings.columns
                       if c not in {"schema_version", "filed_date_d",
                                    "period_end_d", "period_start_d"}]
        es_writer.write_df(es, filings.select(*filing_cols), FILINGS_INDEX,
                           es_writer.FILING_MAPPING,
                           id_cols=["accession_no"],
                           batch_size=ES_BATCH_SIZE)

    as_of = date.today().isoformat()
    if contexts:
        es_writer.write_contexts(es, contexts, CONTEXT_INDEX, as_of=as_of)
    if results:
        es_writer.write_analyses(es, results, ANALYSIS_INDEX, as_of=as_of)

    banner("Pipeline complete")
    print(f"  Elasticsearch : {ES_HOST}")
    print(f"  Dashboard     : http://localhost:8501")
    print(f"  Reports       : {LLM_OUTPUT_DIR}/")
    print(f"  Prompts sent  : {LLM_OUTPUT_DIR}/_prompts/")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
