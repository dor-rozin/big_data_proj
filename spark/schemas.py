"""
Spark StructTypes for the three frozen Kafka topics.

These mirror `schemas/*.schema.json` field for field. That contract is frozen
(see schemas/README.md) — if a field changes there it changes here, and the
change is a conversation with the whole team.

Why this matters more than it looks: `from_json` does not raise on a field that
is absent from the JSON, it returns null. A schema that drifts from the contract
produces a DataFrame with the right row count and a silently all-null column,
and the job reports success. `assert_parsed` below is the guard against that.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, LongType, IntegerType)

# --------------------------------------------------------------------------
# market.prices.v1 — one OHLCV bar for one ticker over one interval
# --------------------------------------------------------------------------
PRICE_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("ticker", StringType()),
    StructField("ts", StringType()),          # period START, ISO-8601 UTC
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("interval", StringType()),    # 1d | 1h | 5m | 1m
    StructField("ingested_at", StringType()),
])

# --------------------------------------------------------------------------
# sec.filings.v1 — one filing with a closed 19-key fact set
# --------------------------------------------------------------------------
# The key set is closed on purpose: an open set means Elasticsearch dynamic
# mapping creates a field per XBRL tag it sees, and one Apple 10-K alone carries
# ~570 facts across 503 distinct tags — past the default 1000-field limit.
FACT_KEYS = [
    "revenue", "cost_of_revenue", "gross_profit", "operating_income",
    "net_income", "rnd_expense", "eps_basic", "eps_diluted", "shares_diluted",
    "assets", "assets_current", "liabilities", "liabilities_current",
    "equity", "cash", "long_term_debt", "shares_outstanding",
    "operating_cash_flow", "capex",
]

FACTS_SCHEMA = StructType([StructField(k, DoubleType()) for k in FACT_KEYS])

FILING_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("cik", StringType()),
    StructField("ticker", StringType()),
    StructField("accession_no", StringType()),
    StructField("form_type", StringType()),
    StructField("filed_date", StringType()),   # YYYY-MM-DD
    StructField("fiscal_period", StringType()),  # FY | Q1..Q4
    StructField("period_start", StringType()),
    StructField("period_end", StringType()),
    StructField("facts", FACTS_SCHEMA),
    StructField("ingested_at", StringType()),
])

# --------------------------------------------------------------------------
# sec.text.v1 — one block of narrative text from a filing
# --------------------------------------------------------------------------
TEXT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("cik", StringType()),
    StructField("ticker", StringType()),
    StructField("accession_no", StringType()),
    StructField("form_type", StringType()),
    StructField("filed_date", StringType()),
    StructField("section", StringType()),      # press_release | risk_factors | mda
    StructField("source_document", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("chunk_index", IntegerType()),
    StructField("chunk_total", IntegerType()),
    StructField("ingested_at", StringType()),
])

TOPIC_SCHEMAS = {
    "market.prices.v1": PRICE_SCHEMA,
    "sec.filings.v1": FILING_SCHEMA,
    "sec.text.v1": TEXT_SCHEMA,
}


def read_topic(spark, bootstrap, topic, schema):
    """Batch-read everything currently in a Kafka topic and parse the JSON.

    Bounded batch (earliest -> latest), not a stream: Spark reads whatever is in
    the topic at this instant and stops. Combined with the deterministic
    document ids in es_writer.py this makes the whole job idempotent — re-running
    it upserts the same rows rather than duplicating them, so there is no offset
    or checkpoint state to keep.
    """
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    return (raw
            .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
            .select("d.*"))


def assert_parsed(df, topic, key_field="ticker"):
    """Fail loudly when messages arrived but the schema did not match them.

    The failure this catches: rename a contract field (`ts` -> `date`) and
    `from_json` silently nulls the column instead of raising. Downstream
    `dropna` then clears every row and the job exits 0 having produced nothing.
    Better to stop here, naming the topic.
    """
    total = df.count()
    if total == 0:
        print(f"[spark] {topic}: 0 messages (topic is empty)")
        return 0
    non_null = df.filter(F.col(key_field).isNotNull()).count()
    if non_null == 0:
        raise ValueError(
            f"{topic}: parsed {total} messages but every '{key_field}' is null. "
            f"The StructType in spark/schemas.py has drifted from "
            f"schemas/{topic}.schema.json."
        )
    print(f"[spark] {topic}: {total} messages parsed ({non_null} with {key_field})")
    return total
