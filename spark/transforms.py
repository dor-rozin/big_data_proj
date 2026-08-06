"""
Tabular transformations: Kafka JSON -> analysis-ready DataFrames.

Everything here is Spark DataFrame API rather than Python. That is the point of
the stage — the transformations are declarative, lazy, and distributed, so they
hold as the snapshot grows. Python appears only in llm.py, where the work is
network I/O over a handful of aggregated rows.

Two rules run through all of it:

  * Nothing is keyed on `ticker` alone. Every window and every group is keyed on
    `(ticker, interval)`. The contract allows 1d/1h/5m/1m; today everything is
    1d, but a rolling average that mixes an hourly and a daily bar produces
    wrong numbers with no error to notice.

  * Nothing is hardcoded to the current universe. Tickers, intervals and date
    ranges are derived from whatever arrived. Adding a ticker upstream needs no
    change here.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from schemas import FACT_KEYS

SECONDS_PER_DAY = 86400


# ---------------------------------------------------------------------------
# PRICES
# ---------------------------------------------------------------------------
def transform_prices(prices):
    """OHLCV bars -> bars plus derived per-instrument columns.

    Windows are time-based (`rangeBetween` over a unix timestamp), not row-based.
    `rowsBetween(-6, 0)` would mean "the previous 6 rows", which silently stops
    meaning "the previous 6 days" the moment bars are missing, a backfill is
    partial, or a second interval arrives on the topic.
    """
    df = (prices
          .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull())
          .withColumn("event_ts", F.to_timestamp("ts"))
          .withColumn("date", F.to_date("event_ts"))
          .withColumn("ts_unix", F.col("event_ts").cast("long")))

    # Deduplicate replays: the same bar can legitimately arrive twice if the
    # producer is re-run. (ticker, interval, ts) is the natural key.
    df = df.dropDuplicates(["ticker", "interval", "ts"])

    grp = ["ticker", "interval"]
    ordered = Window.partitionBy(*grp).orderBy("ts_unix")

    def days(n):
        return Window.partitionBy(*grp).orderBy("ts_unix") \
                     .rangeBetween(-n * SECONDS_PER_DAY, 0)

    df = (df
          .withColumn("prev_close", F.lag("close").over(ordered))
          .withColumn("prev_volume", F.lag("volume").over(ordered))
          .withColumn("daily_return",
                      F.when(F.col("prev_close") > 0,
                             (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
                       .otherwise(F.lit(None)))
          .withColumn("volume_change",
                      F.when(F.col("prev_volume") > 0,
                             (F.col("volume") - F.col("prev_volume")) / F.col("prev_volume"))
                       .otherwise(F.lit(None)))
          .withColumn("intraday_range_pct",
                      F.when(F.col("close") > 0,
                             (F.col("high") - F.col("low")) / F.col("close"))
                       .otherwise(F.lit(None)))
          .withColumn("ma_7", F.avg("close").over(days(7)))
          .withColumn("ma_30", F.avg("close").over(days(30)))
          .withColumn("volatility_10", F.stddev("daily_return").over(days(10)))
          .withColumn("avg_volume_20", F.avg("volume").over(days(20)))
          .withColumn("volume_ratio",
                      F.when(F.col("avg_volume_20") > 0,
                             F.col("volume") / F.col("avg_volume_20"))
                       .otherwise(F.lit(None))))

    # The first bar of each group has no predecessor, so its derived columns are
    # null by definition. Zero-filling would invent a flat day and drag it into
    # the "normal" cluster; leaving null lets the anomaly stage skip it.
    return df.drop("ts_unix", "prev_close", "prev_volume")


# ---------------------------------------------------------------------------
# FILINGS
# ---------------------------------------------------------------------------
def transform_filings(filings):
    """Filings -> flat 19 fact columns plus derived ratios.

    Nulls are data here, not failures. Coverage across the current snapshot runs
    30-70% per fact: banks report no gross profit and no classified balance
    sheet, Walmart does not tag total Liabilities, and cash-flow facts are
    year-to-date tagged so they null out on quarterly filings by design. Every
    ratio below is therefore null-safe — a missing input yields a null ratio,
    never a zero and never a divide-by-zero.
    """
    df = (filings
          .filter(F.col("accession_no").isNotNull())
          .withColumn("filed_date_d", F.to_date("filed_date"))
          .withColumn("period_end_d", F.to_date("period_end"))
          .withColumn("period_start_d", F.to_date("period_start")))

    # Flatten the nested struct into real columns.
    for k in FACT_KEYS:
        df = df.withColumn(k, F.col(f"facts.{k}"))
    df = df.drop("facts")

    # Amendments and restatements: a 10-K/A restates an already-filed period
    # under a new accession number. The contract says consumers keep the latest
    # filing per (cik, fiscal_period, period_end) rather than suppressing either.
    dedup = (Window
             .partitionBy("cik", "fiscal_period", "period_end")
             .orderBy(F.col("filed_date_d").desc(), F.col("accession_no").desc()))
    df = (df.withColumn("_rn", F.row_number().over(dedup))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

    def ratio(num, den):
        n, d = F.col(num), F.col(den)
        return F.when(n.isNotNull() & d.isNotNull() & (d != 0), n / d) \
                .otherwise(F.lit(None))

    df = (df
          .withColumn("gross_margin", ratio("gross_profit", "revenue"))
          .withColumn("operating_margin", ratio("operating_income", "revenue"))
          .withColumn("net_margin", ratio("net_income", "revenue"))
          .withColumn("return_on_equity", ratio("net_income", "equity"))
          .withColumn("current_ratio", ratio("assets_current", "liabilities_current"))
          .withColumn("debt_to_equity", ratio("long_term_debt", "equity"))
          .withColumn("rnd_intensity", ratio("rnd_expense", "revenue"))
          .withColumn("free_cash_flow",
                      F.when(F.col("operating_cash_flow").isNotNull()
                             & F.col("capex").isNotNull(),
                             F.col("operating_cash_flow") - F.col("capex"))
                       .otherwise(F.lit(None))))

    # Year-on-year growth compares like with like: same ticker, same fiscal
    # period (an FY against the previous FY, a Q2 against the previous Q2).
    yoy = Window.partitionBy("cik", "fiscal_period").orderBy("period_end_d")
    df = (df
          .withColumn("_prev_revenue", F.lag("revenue").over(yoy))
          .withColumn("_prev_net_income", F.lag("net_income").over(yoy))
          .withColumn("revenue_yoy", ratio_growth("revenue", "_prev_revenue"))
          .withColumn("net_income_yoy", ratio_growth("net_income", "_prev_net_income"))
          .drop("_prev_revenue", "_prev_net_income"))

    return df


def ratio_growth(cur, prev):
    """(cur - prev) / |prev|, null-safe.

    The denominator is an absolute value on purpose: a company moving from a
    loss to a profit has a negative `prev`, and a plain division would flip the
    sign and report growth as a decline.
    """
    c, p = F.col(cur), F.col(prev)
    return F.when(c.isNotNull() & p.isNotNull() & (p != 0),
                  (c - p) / F.abs(p)).otherwise(F.lit(None))


# ---------------------------------------------------------------------------
# TEXT
# ---------------------------------------------------------------------------
def transform_text(text):
    """Narrative text -> one row per (accession_no, section), chunks reassembled.

    Everything on the topic today is chunk 0 of 1, but the contract carries
    chunk_index/chunk_total precisely so that long sections can be split later
    without a contract change. Reassembling from the start means the day a 10-K
    Risk Factors section arrives in 40 pieces, nothing here changes.
    """
    if text.rdd.isEmpty():
        return text.limit(0)

    ordered = (F.collect_list(F.struct("chunk_index", "text"))
                .alias("_chunks"))

    grouped = (text
               .filter(F.col("text").isNotNull() & (F.length("text") > 0))
               .dropDuplicates(["accession_no", "section", "chunk_index"])
               .groupBy("cik", "ticker", "accession_no", "form_type",
                        "filed_date", "section")
               .agg(ordered,
                    F.first("title", ignorenulls=True).alias("title"),
                    F.first("source_document", ignorenulls=True).alias("source_document"),
                    F.max("chunk_total").alias("chunk_total")))

    return (grouped
            .withColumn("_sorted", F.array_sort("_chunks"))
            .withColumn("text",
                        F.concat_ws("\n", F.transform("_sorted", lambda c: c["text"])))
            .withColumn("filed_date_d", F.to_date("filed_date"))
            .withColumn("text_chars", F.length("text"))
            .drop("_chunks", "_sorted"))


# ---------------------------------------------------------------------------
# AGGREGATION FOR THE LLM
# ---------------------------------------------------------------------------
def aggregate_for_llm(prices, filings, text, top_anomalies=5,
                      filing_proximity_days=2):
    """Collapse everything to one row per (ticker, interval).

    This is what makes the LLM stage affordable: the model is asked about ten
    instruments, not about 3,375 individual messages. It is also what makes the
    answer better — the model gets the specific days that were unusual and the
    latest reported fundamentals, rather than a wall of bars it cannot do
    arithmetic over.
    """
    grp = ["ticker", "interval"]

    summary = (prices.groupBy(*grp).agg(
        F.count("*").alias("bar_count"),
        F.min("date").alias("first_date"),
        F.max("date").alias("last_date"),
        F.avg("daily_return").alias("avg_return"),
        F.stddev("daily_return").alias("return_volatility"),
        F.avg("volume").alias("avg_volume"),
        F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
    ))

    # Latest close per group, via a window rather than a second aggregation +
    # join, so the whole thing stays one shuffle.
    last = Window.partitionBy(*grp).orderBy(F.col("event_ts").desc())
    latest = (prices
              .withColumn("_rn", F.row_number().over(last))
              .filter(F.col("_rn") == 1)
              .select(*grp,
                      F.col("close").alias("latest_close"),
                      F.col("ma_30").alias("latest_ma_30")))

    # Top-N anomalies per group, ordered strongest first. Bounded on purpose:
    # a ticker with ten years of history must not blow up the prompt.
    rank = Window.partitionBy(*grp).orderBy(F.col("anomaly_score").desc())
    top = (prices
           .filter(F.col("is_anomaly"))
           .withColumn("_rn", F.row_number().over(rank))
           .filter(F.col("_rn") <= top_anomalies)
           .groupBy(*grp)
           .agg(F.collect_list(F.struct(
               F.col("date").cast("string").alias("date"),
               F.round("daily_return", 4).alias("daily_return"),
               F.round("volume_ratio", 2).alias("volume_ratio"),
               F.round("anomaly_score", 3).alias("anomaly_score"),
           )).alias("top_anomalies")))

    agg = (summary
           .join(latest, grp, "left")
           .join(top, grp, "left"))

    # Latest filing per ticker, plus how many anomalies landed near a filing
    # date. That proximity number is the reason both topics exist in one
    # pipeline rather than two unrelated tables.
    if not filings.rdd.isEmpty():
        fl = Window.partitionBy("ticker").orderBy(F.col("filed_date_d").desc())
        latest_filing = (filings
                         .filter(F.col("ticker").isNotNull())
                         .withColumn("_rn", F.row_number().over(fl))
                         .filter(F.col("_rn") == 1)
                         .select("ticker",
                                 F.col("form_type").alias("latest_form_type"),
                                 F.col("fiscal_period").alias("latest_fiscal_period"),
                                 F.col("filed_date").alias("latest_filed_date"),
                                 "revenue", "net_income", "eps_diluted",
                                 "gross_margin", "net_margin", "return_on_equity",
                                 "current_ratio", "debt_to_equity",
                                 "revenue_yoy", "net_income_yoy"))
        agg = agg.join(latest_filing, "ticker", "left")

        near = (prices.filter(F.col("is_anomaly")).select(*grp, "date")
                .join(filings.select("ticker", "filed_date_d"), "ticker")
                .filter(F.abs(F.datediff("date", "filed_date_d")) <= filing_proximity_days)
                .groupBy(*grp)
                .agg(F.countDistinct("date").alias("anomalies_near_filing")))
        agg = agg.join(near, grp, "left")
    else:
        agg = agg.withColumn("anomalies_near_filing", F.lit(None).cast("long"))

    # Most recent narrative text per ticker. Empty until ticket 0010 lands, and
    # a left join keeps every ticker in the output regardless.
    if not text.rdd.isEmpty():
        tw = Window.partitionBy("ticker").orderBy(F.col("filed_date_d").desc())
        latest_text = (text
                       .filter(F.col("ticker").isNotNull())
                       .withColumn("_rn", F.row_number().over(tw))
                       .filter(F.col("_rn") == 1)
                       .select("ticker",
                               F.col("text").alias("filing_text"),
                               F.col("title").alias("filing_text_title"),
                               F.col("section").alias("filing_text_section"),
                               F.col("filed_date").alias("filing_text_date")))
        agg = agg.join(latest_text, "ticker", "left")
    else:
        for c in ["filing_text", "filing_text_title",
                  "filing_text_section", "filing_text_date"]:
            agg = agg.withColumn(c, F.lit(None).cast("string"))

    return agg.withColumn("anomalies_near_filing",
                          F.coalesce(F.col("anomalies_near_filing"), F.lit(0)))
