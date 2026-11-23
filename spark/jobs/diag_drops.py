from aggregate import count_of
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "lake.raw.events"
DAILY = "lake.gold.daily_service"


def main() -> None:
    spark = SparkSession.builder.appName("diag_drops").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = spark.table(RAW)
    day = F.window("event_time", "1 day")["start"].alias("day")

    expected = (
        raw.filter(F.col("event_type").isin("demand", "backorder"))
        .groupBy(day, "site")
        .agg(
            count_of("demand").alias("expected_orders"), count_of("backorder").alias("expected_bo")
        )
    )
    diff = (F.col("orders") - F.col("expected_orders")) + (
        F.col("backorders") - F.col("expected_bo")
    )
    bad = expected.join(spark.table(DAILY), ["day", "site"]).filter(diff != 0).select("day", "site")

    earlier = (
        Window.partitionBy("kafka_partition")
        .orderBy("kafka_offset")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    late_days = (
        F.max("event_time").over(earlier).cast("long") - F.col("event_time").cast("long")
    ) / 86400
    late = (
        raw.filter(F.col("event_type").isin("demand", "backorder"))
        .withColumn("late_days", late_days)
        .filter(F.col("late_days") > 0)
        .withColumn("day", day)
    )

    print("most-delayed late event in each mismatched window:")
    late.join(bad, ["day", "site"]).groupBy("day", "site").agg(
        F.round(F.max("late_days"), 2).alias("max_late_days")
    ).orderBy("day").show(20, truncate=False)

    print("late events delayed more than 29 days, across the whole stream:")
    print(late.filter(F.col("late_days") > 29).count())


if __name__ == "__main__":
    main()
