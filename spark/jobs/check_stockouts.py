from aggregate import inventory_delta
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "lake.raw.events"
POLICY = "lake.ref.policy"
PARTS = "lake.ref.parts"
EPISODES = "lake.gold.stockout_episodes"
DC = "central_dc"


def batch_recoveries(spark: SparkSession):
    net = (
        spark.table(RAW)
        .withColumn("delta", inventory_delta(DC))
        .filter(F.col("delta") != 0)
        .groupBy("site", "part_id", "event_time")
        .agg(F.sum("delta").alias("change"))
        .filter(F.col("change") != 0)
    )
    opening = spark.table(POLICY).select("site", "part_id", F.col("order_up_to").alias("opening"))
    ordered = Window.partitionBy("site", "part_id").orderBy("event_time")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    series = (
        net.join(opening, ["site", "part_id"])
        .withColumn("on_hand", F.col("opening") + F.sum("change").over(running))
        .withColumn("before", F.coalesce(F.lag("on_hand").over(ordered), F.col("opening")))
    )
    return series.filter((F.col("before") <= 0) & (F.col("on_hand") > 0))


def main() -> None:
    spark = SparkSession.builder.appName("check_stockouts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    episodes = spark.table(EPISODES)

    streamed = episodes.groupBy("site").agg(F.count("*").alias("streamed"))
    expected = batch_recoveries(spark).groupBy("site").agg(F.count("*").alias("batch"))
    print("episodes per site, streaming vs batch recomputation:")
    expected.join(streamed, "site", "full").orderBy("site").show()

    print("episode duration in days:")
    episodes.groupBy("site").agg(
        F.count("*").alias("episodes"),
        F.round(F.avg("duration_days"), 1).alias("mean"),
        F.round(F.percentile_approx("duration_days", 0.5), 1).alias("p50"),
        F.round(F.percentile_approx("duration_days", 0.9), 1).alias("p90"),
    ).orderBy("site").show()

    print("by supplier:")
    level = F.when(F.col("site") == DC, "dc").otherwise("region").alias("level")
    episodes.join(spark.table(PARTS).select("part_id", "supplier"), "part_id").groupBy(
        "supplier", level
    ).agg(
        F.count("*").alias("episodes"),
        F.round(F.avg("duration_days"), 1).alias("mean_days"),
        F.round(F.percentile_approx("duration_days", 0.9), 1).alias("p90_days"),
    ).orderBy("level", "supplier").show()


if __name__ == "__main__":
    main()
