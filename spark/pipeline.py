"""
Spark pipeline (TRANSFORM + AI + LOAD stages).

  Person B owns: read prices from Kafka -> feature engineering -> MLlib KMeans
                 anomaly detection.
  Person C owns: news sentiment + writing both result tables to Elasticsearch.

Flow:
  1. Read the `prices` and `news` topics from Kafka (batch read: earliest->latest).
  2. Prices: compute per-ticker features, then use MLlib KMeans to model
     "normal" behaviour and flag days that sit far from every cluster centre
     as anomalies.
  3. News: score each headline's sentiment with VADER.
  4. Write both enriched tables to Elasticsearch.
"""
import os

import numpy as np
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, LongType)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.clustering import KMeans
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ---- config ---------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
PRICES_TOPIC = os.getenv("PRICES_TOPIC", "prices")
NEWS_TOPIC = os.getenv("NEWS_TOPIC", "news")
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
PRICES_INDEX = os.getenv("PRICES_INDEX", "stock_prices")
NEWS_INDEX = os.getenv("NEWS_INDEX", "stock_news")
ANOMALY_FRACTION = float(os.getenv("ANOMALY_FRACTION", "0.05"))
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

PRICE_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("date", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
])
NEWS_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("title", StringType()),
    StructField("publisher", StringType()),
    StructField("published", StringType()),
])


def read_topic(spark, topic, schema):
    """Batch-read every message currently in a Kafka topic and parse the JSON."""
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    return (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
            .select("d.*"))


# ---------------------------------------------------------------------------
# PRICES: feature engineering + KMeans anomaly detection
# ---------------------------------------------------------------------------
def process_prices(prices):
    prices = (prices.dropna(subset=["close"])
              .withColumn("date", F.to_date("date"))
              .dropDuplicates(["ticker", "date"]))

    w = Window.partitionBy("ticker").orderBy("date")
    w_vol = w.rowsBetween(-9, 0)  # trailing 10-day window

    feat = (prices
            .withColumn("prev_close", F.lag("close").over(w))
            .withColumn("prev_volume", F.lag("volume").over(w))
            .withColumn("daily_return",
                        (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
            .withColumn("volume_change",
                        (F.col("volume") - F.col("prev_volume")) / F.col("prev_volume"))
            .withColumn("range_pct",
                        (F.col("high") - F.col("low")) / F.col("open"))
            .withColumn("volatility_10d", F.stddev("daily_return").over(w_vol))
            .dropna(subset=["daily_return", "volume_change", "range_pct", "volatility_10d"]))

    feature_cols = ["daily_return", "volume_change", "range_pct", "volatility_10d"]

    # Standardise features so no single column dominates the distance metric.
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                            withMean=True, withStd=True)
    feat = assembler.transform(feat)
    scaler_model = scaler.fit(feat)
    feat = scaler_model.transform(feat)

    # KMeans models "normal" regimes; distance to the nearest centre = how
    # unusual a day is. Far-away days become our anomalies.
    kmeans = KMeans(featuresCol="features", predictionCol="cluster",
                    k=3, seed=42)
    model = kmeans.fit(feat)
    feat = model.transform(feat)

    centers = model.clusterCenters()
    b_centers = feat.rdd.context.broadcast([c.tolist() for c in centers])

    @F.udf(DoubleType())
    def distance_to_center(features, cluster):
        c = np.array(b_centers.value[cluster])
        return float(np.linalg.norm(np.array(features) - c))

    feat = feat.withColumn("anomaly_score",
                           distance_to_center(vector_to_array("features"), F.col("cluster")))

    # Flag the top ANOMALY_FRACTION most distant days as anomalies.
    threshold = feat.approxQuantile("anomaly_score", [1 - ANOMALY_FRACTION], 0.01)[0]
    feat = feat.withColumn("is_anomaly", F.col("anomaly_score") >= F.lit(threshold))

    print(f"[spark] anomaly score threshold (top {ANOMALY_FRACTION:.0%}) = {threshold:.4f}")

    return feat.select(
        "ticker", "date", "open", "high", "low", "close", "volume",
        "daily_return", "volume_change", "range_pct", "volatility_10d",
        "cluster", "anomaly_score", "is_anomaly",
    )


# ---------------------------------------------------------------------------
# NEWS: VADER sentiment on each headline
# ---------------------------------------------------------------------------
def process_news(news):
    analyzer = SentimentIntensityAnalyzer()

    @F.udf(DoubleType())
    def sentiment_score(text):
        if not text:
            return 0.0
        return float(analyzer.polarity_scores(text)["compound"])

    news = news.dropna(subset=["title"]).dropDuplicates(["ticker", "title"])
    news = news.withColumn("sentiment", sentiment_score(F.col("title")))
    news = news.withColumn(
        "sentiment_label",
        F.when(F.col("sentiment") >= 0.05, "positive")
         .when(F.col("sentiment") <= -0.05, "negative")
         .otherwise("neutral"),
    )
    return news.select("ticker", "title", "publisher", "published",
                       "sentiment", "sentiment_label")


# ---------------------------------------------------------------------------
# LOAD: write a Spark DataFrame to an Elasticsearch index
# ---------------------------------------------------------------------------
def load_to_es(df, index, mapping):
    pdf = df.toPandas()
    # dates -> ISO strings so json serialisation and ES date mapping are happy.
    for col in pdf.columns:
        if str(pdf[col].dtype).startswith("datetime"):
            pdf[col] = pdf[col].dt.strftime("%Y-%m-%d")

    es = Elasticsearch(ES_HOST)
    if es.indices.exists(index=index):
        es.indices.delete(index=index)
    es.indices.create(index=index, mappings=mapping)

    actions = ({"_index": index, "_source": row.dropna().to_dict()}
               for _, row in pdf.iterrows())
    helpers.bulk(es, actions)
    es.indices.refresh(index=index)
    print(f"[spark] loaded {len(pdf)} docs into '{index}'")


def main():
    spark = (SparkSession.builder
             .appName("stock-anomaly-pipeline")
             .master("local[*]")
             .config("spark.jars.packages", KAFKA_PACKAGE)
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    prices = process_prices(read_topic(spark, PRICES_TOPIC, PRICE_SCHEMA))
    news = process_news(read_topic(spark, NEWS_TOPIC, NEWS_SCHEMA))

    load_to_es(prices, PRICES_INDEX, mapping={
        "properties": {
            "ticker": {"type": "keyword"},
            "date": {"type": "date"},
            "close": {"type": "float"},
            "daily_return": {"type": "float"},
            "anomaly_score": {"type": "float"},
            "is_anomaly": {"type": "boolean"},
            "cluster": {"type": "integer"},
        }
    })
    load_to_es(news, NEWS_INDEX, mapping={
        "properties": {
            "ticker": {"type": "keyword"},
            "title": {"type": "text"},
            "publisher": {"type": "keyword"},
            "sentiment": {"type": "float"},
            "sentiment_label": {"type": "keyword"},
        }
    })

    spark.stop()
    print("[spark] pipeline complete.")


if __name__ == "__main__":
    main()
