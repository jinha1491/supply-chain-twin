from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "lake.raw.events"
DAILY_TABLES = {
    "2-day watermark": "lake.gold.daily_service",
    "7-day watermark": "lake.gold.daily_service_wm7",
    "30-day watermark": "lake.gold.daily_service_wm30",
}


def count_of(event_type: str):
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))


def show_lateness(spark: SparkSession) -> None:
    earlier = (
        Window.partitionBy("kafka_partition")
        .orderBy("kafka_offset")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    events = spark.table(RAW).withColumn("seen_max", F.max("event_time").over(earlier))
    late_days = (F.col("seen_max").cast("long") - F.col("event_time").cast("long")) / 86400
    late = events.withColumn("late_days", late_days).filter(F.col("late_days") > 0)

    print(f"events that arrived after later events: {late.count():,}")
    bucket = (F.floor(F.col("late_days") / 5) * 5).alias("days_late_from")
    late.groupBy(bucket).count().orderBy("days_late_from").show()


def compare(spark: SparkSession, label: str, table: str) -> None:
    expected = (
        spark.table(RAW)
        .filter(F.col("event_type").isin("demand", "backorder"))
        .groupBy(F.window("event_time", "1 day")["start"].alias("day"), "site")
        .agg(
            count_of("demand").alias("expected_orders"), count_of("backorder").alias("expected_bo")
        )
    )
    closed = expected.join(spark.table(table), ["day", "site"])
    missing = (F.col("expected_orders") - F.col("orders")) + (
        F.col("expected_bo") - F.col("backorders")
    )
    result = closed.agg(
        F.count("*").alias("windows"),
        F.sum(F.when(missing > 0, 1).otherwise(0)).alias("short_windows"),
        F.sum(missing).alias("dropped_events"),
    ).first()
    print(
        f"{label}: {result['windows']:,} closed windows, "
        f"{result['short_windows']:,} short, {result['dropped_events']:,} events dropped"
    )


def main() -> None:
    spark = SparkSession.builder.appName("check_late").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    show_lateness(spark)
    for label, table in DAILY_TABLES.items():
        compare(spark, label, table)


if __name__ == "__main__":
    main()
