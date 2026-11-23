import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "lake.raw.events"
EPISODES = "lake.gold.stockout_episodes"
PARTS = "lake.md.parts"
SUPPLIER_HISTORY = "lake.md.suppliers_history"
DC = "central_dc"


def lead_times_as_of(spark: SparkSession) -> None:
    raw = spark.table(RAW).filter(F.col("site") == DC)
    orders = raw.filter(F.col("event_type") == "replenishment_order").select(
        F.col("ref_id").alias("po"), "part_id", F.col("event_time").alias("ordered_at")
    )
    receipts = raw.filter(F.col("event_type") == "receipt").select(
        F.col("ref_id").alias("po"), F.col("event_time").alias("received_at")
    )
    purchase_orders = orders.join(receipts, "po").join(
        spark.table(PARTS).select("part_id", "supplier"), "part_id"
    )

    versions = (
        spark.table(SUPPLIER_HISTORY)
        .filter(~F.col("deleted"))
        .select(F.col("name").alias("supplier"), "lead_time_mean", "effective_at", "source_lsn")
    )
    newest_first = Window.partitionBy("po").orderBy(F.desc("effective_at"), F.desc("source_lsn"))
    as_of = (
        purchase_orders.join(versions, "supplier")
        .filter(F.col("effective_at") <= F.col("ordered_at"))
        .withColumn("rank", F.row_number().over(newest_first))
        .filter("rank = 1")
    )
    realized = (F.col("received_at").cast("long") - F.col("ordered_at").cast("long")) / 86400

    print("DC purchase orders by supplier and the lead time in effect when ordered:")
    as_of.groupBy("supplier", F.col("lead_time_mean").alias("mean_in_effect")).agg(
        F.count("*").alias("orders"),
        F.round(F.avg(realized), 1).alias("realized_days"),
    ).orderBy("supplier", "mean_in_effect").show()


def stockout_impact(spark: SparkSession, start: str, end: str) -> None:
    episodes = (
        spark.table(EPISODES)
        .filter(F.col("site") != DC)
        .join(spark.table(PARTS).select("part_id", "supplier"), "part_id")
        .withColumn("group", F.when(F.col("supplier") == "asia", "asia").otherwise("other"))
        .withColumn(
            "period",
            F.when(F.col("out_start") < start, "1_before")
            .when(F.col("out_start") < end, "2_congestion")
            .otherwise("3_after"),
        )
    )
    summary = episodes.groupBy("group", "period").agg(
        F.count("*").alias("episodes"), F.avg("duration_days").alias("mean_days")
    )
    print("regional stockout length by period:")
    summary.withColumn("mean_days", F.round("mean_days", 2)).orderBy("group", "period").show()

    means = {(r["group"], r["period"]): r["mean_days"] for r in summary.collect()}
    asia_change = means[("asia", "2_congestion")] - means[("asia", "1_before")]
    other_change = means[("other", "2_congestion")] - means[("other", "1_before")]
    print(
        f"asia change {asia_change:+.2f} days, other change {other_change:+.2f} days, "
        f"difference-in-differences {asia_change - other_change:+.2f} days"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Report on a scenario run.")
    parser.add_argument("--congestion-start", default="1999-08-24")
    parser.add_argument("--congestion-end", default="2001-07-24")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("scenario_report").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    lead_times_as_of(spark)
    stockout_impact(spark, args.congestion_start, args.congestion_end)


if __name__ == "__main__":
    main()
