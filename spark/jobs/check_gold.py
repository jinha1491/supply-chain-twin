from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "lake.raw.events"
FLOW = "lake.gold.inventory_flow"
DAILY = "lake.gold.daily_service"
POLICY = "lake.ref.policy"


def count_of(event_type: str):
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))


def check_daily(spark: SparkSession) -> None:
    expected = (
        spark.table(RAW)
        .filter(F.col("event_type").isin("demand", "backorder"))
        .groupBy(F.window("event_time", "1 day")["start"].alias("day"), "site")
        .agg(
            count_of("demand").alias("expected_orders"), count_of("backorder").alias("expected_bo")
        )
    )
    streamed = spark.table(DAILY)
    joined = expected.join(streamed, ["day", "site"], "left")

    pending = joined.filter(F.col("orders").isNull())
    mismatched = joined.filter(
        F.col("orders").isNotNull()
        & (
            (F.col("orders") != F.col("expected_orders"))
            | (F.col("backorders") != F.col("expected_bo"))
        )
    )
    print(f"daily windows written: {streamed.count():,}")
    print(f"windows that disagree with raw: {mismatched.count()}")
    print("windows not yet closed by the watermark:")
    pending.agg(F.count("*").alias("windows"), F.min("day"), F.max("day")).show(truncate=False)

    print("fill rate by region from the gold table:")
    streamed.groupBy("site").agg(
        F.round(1 - F.sum("backorders") / F.sum("orders"), 3).alias("fill_rate")
    ).orderBy("site").show()


def check_positions(spark: SparkSession) -> None:
    opening = spark.table(POLICY).select("site", "part_id", F.col("order_up_to").alias("opening"))
    positions = (
        opening.join(spark.table(FLOW), ["site", "part_id"], "left")
        .fillna({"net_flow": 0})
        .withColumn("on_hand", F.col("opening") + F.col("net_flow"))
    )
    print("current positions by site (on_hand must never be negative):")
    positions.groupBy("site").agg(
        F.sum("on_hand").alias("units"),
        F.min("on_hand").alias("min_on_hand"),
        F.sum(F.when(F.col("on_hand") == 0, 1).otherwise(0)).alias("parts_at_zero"),
    ).orderBy("site").show()


def main() -> None:
    spark = SparkSession.builder.appName("check_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    check_daily(spark)
    check_positions(spark)


if __name__ == "__main__":
    main()
