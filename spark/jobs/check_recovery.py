from aggregate import count_of, inventory_delta
from ingest_raw import KAFKA, TOPIC
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "lake.raw.events"
FLOW = "lake.gold.inventory_flow"
DAILY = "lake.gold.daily_service"
DC = "central_dc"


def check_raw(spark: SparkSession) -> None:
    kafka_total = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .count()
    )
    raw = spark.table(RAW)
    duplicates = raw.groupBy("kafka_partition", "kafka_offset").count().filter("count > 1").count()
    gaps = (
        raw.groupBy("kafka_partition")
        .agg((F.max("kafka_offset") - F.min("kafka_offset") + 1 - F.count("*")).alias("gap"))
        .agg(F.sum("gap"))
        .first()[0]
    )
    print(f"kafka messages: {kafka_total:,}  raw rows: {raw.count():,}")
    print(f"duplicate offsets: {duplicates}  missing offsets: {gaps}")

    epochs = spark.sql(
        f"""
        SELECT summary['spark.sql.streaming.queryId'] AS query_id,
               summary['spark.sql.streaming.epochId'] AS epoch_id
        FROM {RAW}.snapshots
        WHERE summary['spark.sql.streaming.epochId'] IS NOT NULL
        """
    )
    repeated = epochs.groupBy("query_id", "epoch_id").count().filter("count > 1").count()
    print(f"iceberg commits: {epochs.count()}  batches committed twice: {repeated}")


def check_flow(spark: SparkSession) -> None:
    expected = (
        spark.table(RAW)
        .withColumn("delta", inventory_delta(DC))
        .filter(F.col("delta") != 0)
        .groupBy("site", "part_id")
        .agg(F.sum("delta").alias("expected"))
    )
    joined = expected.join(spark.table(FLOW), ["site", "part_id"], "full")
    mismatched = joined.filter(
        F.coalesce(F.col("expected"), F.lit(0)) != F.coalesce(F.col("net_flow"), F.lit(0))
    ).count()
    print(f"inventory positions: {joined.count():,} site-parts, {mismatched} mismatched")


def check_daily(spark: SparkSession) -> None:
    expected = (
        spark.table(RAW)
        .filter(F.col("event_type").isin("demand", "backorder"))
        .groupBy(F.window("event_time", "1 day")["start"].alias("day"), "site")
        .agg(
            count_of("demand").alias("expected_orders"), count_of("backorder").alias("expected_bo")
        )
    )
    closed = expected.join(spark.table(DAILY), ["day", "site"])
    mismatched = closed.filter(
        (F.col("orders") != F.col("expected_orders"))
        | (F.col("backorders") != F.col("expected_bo"))
    ).count()
    print(f"daily windows: {closed.count():,} closed, {mismatched} mismatched")


def main() -> None:
    spark = SparkSession.builder.appName("check_recovery").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    check_raw(spark)
    check_flow(spark)
    check_daily(spark)


if __name__ == "__main__":
    main()
