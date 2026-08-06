"""
Unsupervised anomaly detection with Spark MLlib KMeans.

What this is for. It is not the product — as a standalone output, "days with
large moves on heavy volume" is a dressed-up outlier filter. Its job is to be a
feature extractor for the LLM stage: Gemini cannot scan thousands of price bars
and do reliable arithmetic over them, so this stage finds the handful of days
that were unusual and hands them over as specific dates with specific numbers.
The model finds *where to look*; the LLM says *what it means*.

Design: scale per group, cluster globally.

  Features are z-scored within each (ticker, interval) group using window
  functions, then a single KMeans is fitted across every group at once.

  Scaling per group is what makes the result meaningful: on a global scale
  NVDA's volatility would define "normal" for the whole universe and JPM would
  never look unusual. After per-group scaling, every instrument is judged
  against its own history.

  Fitting one model rather than one per group is what makes it scale: a driver
  loop of `for ticker in tickers: KMeans().fit(...)` is N Spark jobs and stops
  being viable somewhere around a few hundred instruments. One fit over
  pre-scaled features is a single distributed job whatever the universe size.
  This replaces MLlib's StandardScaler, which standardises globally and would
  undo the per-group scaling.

The anomaly threshold is then applied per group, so each instrument contributes
its own share of anomalies instead of the most volatile names crowding out the
rest.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array

FEATURES = ["daily_return", "volume_change", "intraday_range_pct", "volatility_10"]

GROUP = ["ticker", "interval"]


def detect_anomalies(prices, k=3, fraction=0.05, min_rows=30, seed=42):
    """Add `cluster`, `anomaly_score` and `is_anomaly` to every price row.

    Groups with fewer than `min_rows` bars are passed through unflagged: a
    3-cluster fit over a dozen points describes noise, and a newly added ticker
    should not arrive pre-labelled with fake anomalies on its first day.
    """
    complete = prices.filter(
        F.greatest(*[F.col(c).isNull().cast("int") for c in FEATURES]) == 0
    )

    counts = complete.groupBy(*GROUP).agg(F.count("*").alias("_group_rows"))
    complete = complete.join(counts, GROUP, "left")

    eligible = complete.filter(F.col("_group_rows") >= min_rows).drop("_group_rows")
    skipped = complete.filter(F.col("_group_rows") < min_rows).drop("_group_rows")

    if eligible.rdd.isEmpty():
        print(f"[spark] anomaly: no group reached min_rows={min_rows}, "
              f"skipping detection")
        return _unflagged(prices)

    # ---- scale within each (ticker, interval) -----------------------------
    scaled = eligible
    w = Window.partitionBy(*GROUP)
    for c in FEATURES:
        mean, std = F.avg(c).over(w), F.stddev(c).over(w)
        # A zero/null stddev means the feature is constant across the group;
        # it carries no information there, so it contributes 0 rather than NaN.
        scaled = scaled.withColumn(
            f"_z_{c}",
            F.when(std.isNotNull() & (std > 0), (F.col(c) - mean) / std)
             .otherwise(F.lit(0.0)))

    z_cols = [f"_z_{c}" for c in FEATURES]
    assembled = VectorAssembler(inputCols=z_cols, outputCol="_features") \
        .transform(scaled)

    # ---- one KMeans over the whole scaled universe ------------------------
    model = KMeans(k=k, seed=seed, featuresCol="_features",
                   predictionCol="cluster").fit(assembled)
    clustered = model.transform(assembled)

    # ---- distance from each point to its assigned centre ------------------
    # Done as a Spark expression rather than a UDF: the centres are a handful of
    # small arrays, so they can be inlined as literals and the distance stays in
    # the JVM instead of paying per-row Python serialisation.
    centres = F.array(*[
        F.array(*[F.lit(float(v)) for v in centre])
        for centre in model.clusterCenters()
    ])
    scored = (clustered
              .withColumn("_point", vector_to_array("_features"))
              .withColumn("_centre", centres.getItem(F.col("cluster")))
              .withColumn("anomaly_score",
                          F.sqrt(F.aggregate(
                              F.zip_with("_point", "_centre",
                                         lambda a, b: (a - b) * (a - b)),
                              F.lit(0.0),
                              lambda acc, x: acc + x))))

    # ---- flag the top `fraction` within each group ------------------------
    thresholds = (scored.groupBy(*GROUP)
                  .agg(F.expr(f"percentile_approx(anomaly_score, {1 - fraction})")
                       .alias("_threshold")))

    flagged = (scored.join(thresholds, GROUP, "left")
               .withColumn("is_anomaly",
                           F.col("anomaly_score") >= F.col("_threshold"))
               .drop("_features", "_point", "_centre", "_threshold", *z_cols))

    # Rows excluded from the fit still belong in the output table — they are
    # real bars, they simply carry no verdict.
    passthrough = _unflagged(
        skipped.unionByName(
            prices.join(eligible.select(*GROUP, "ts").withColumn("_hit", F.lit(True)),
                        GROUP + ["ts"], "left_anti"),
            allowMissingColumns=True))

    out = flagged.unionByName(passthrough, allowMissingColumns=True)

    n_flagged = flagged.filter(F.col("is_anomaly")).count()
    n_groups = thresholds.count()
    print(f"[spark] anomaly: k={k}, {n_groups} (ticker, interval) groups, "
          f"{n_flagged} bars flagged at top {fraction:.0%} per group")
    return out


def _unflagged(df):
    """Give a DataFrame the three anomaly columns with no verdict attached."""
    return (df
            .withColumn("cluster", F.lit(None).cast("int"))
            .withColumn("anomaly_score", F.lit(None).cast("double"))
            .withColumn("is_anomaly", F.lit(False)))
