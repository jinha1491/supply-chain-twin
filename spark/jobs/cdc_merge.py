import argparse

from ingest_raw import KAFKA
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TABLES = {
    "suppliers": (
        ["name"],
        "name string, lead_time_mean double, lead_time_std double, carrier string, "
        "effective_at timestamp",
    ),
    "sites": (
        ["name"],
        "name string, kind string, capacity_units int, transit_days double, "
        "demand_share double, carrier string, effective_at timestamp",
    ),
    "parts": (
        ["part_id"],
        "part_id string, supplier string, pattern string, effective_at timestamp",
    ),
    "policy": (
        ["site", "part_id"],
        "site string, part_id string, reorder_point int, order_up_to int, effective_at timestamp",
    ),
}
META = "__op string, __source_ts_ms long, __source_lsn long, __deleted string"


def column_names(columns: str) -> list[str]:
    return [c.strip().split()[0] for c in columns.split(",")]


def create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.md")
    for table, (_, columns) in TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS lake.md.{table} ({columns}) USING iceberg")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS lake.md.{table}_history ({columns}, op string, "
            "source_lsn bigint, source_ts timestamp, deleted boolean, kafka_offset bigint) "
            "USING iceberg"
        )


def merge_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    batch.persist()
    for table, (keys, columns) in TABLES.items():
        names = column_names(columns)
        rows = (
            batch.filter((F.col("topic") == f"md.public.{table}") & F.col("value").isNotNull())
            .select(
                F.from_json(F.col("value").cast("string"), f"{columns}, {META}").alias("r"),
                "offset",
            )
            .select("r.*", "offset")
        )
        if rows.isEmpty():
            continue

        newest_first = Window.partitionBy(*keys).orderBy(F.desc("__source_lsn"), F.desc("offset"))
        latest = rows.withColumn("rank", F.row_number().over(newest_first)).filter("rank = 1")
        latest.createOrReplaceTempView("changes")

        match = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        spark.sql(
            f"""
            MERGE INTO lake.md.{table} t
            USING changes s
            ON {match}
            WHEN MATCHED AND s.__deleted = 'true' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in names)}
            WHEN NOT MATCHED AND s.__deleted <> 'true'
                THEN INSERT ({", ".join(names)}) VALUES ({", ".join(f"s.{c}" for c in names)})
            """
        )

        rows.select(
            *names,
            F.col("__op").alias("op"),
            F.col("__source_lsn").alias("source_lsn"),
            (F.col("__source_ts_ms") / 1000).cast("timestamp").alias("source_ts"),
            (F.col("__deleted") == "true").alias("deleted"),
            F.col("offset").alias("kafka_offset"),
        ).createOrReplaceTempView("history_changes")
        spark.sql(
            f"""
            MERGE INTO lake.md.{table}_history t
            USING history_changes s
            ON {match} AND t.kafka_offset = s.kafka_offset
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    batch.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge Debezium changes into Iceberg.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--checkpoint", default="/checkpoints/cdc_merge")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("cdc_merge").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    changes = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribePattern", r"md\.public\..*")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    writer = changes.writeStream.foreachBatch(merge_batch).option(
        "checkpointLocation", args.checkpoint
    )
    writer = (
        writer.trigger(availableNow=True)
        if args.once
        else writer.trigger(processingTime="10 seconds")
    )
    query = writer.start()
    query.awaitTermination()

    rows = sum(p["numInputRows"] for p in query.recentProgress)
    print(f"cdc_merge: {len(query.recentProgress)} batches, {rows:,} change messages")


if __name__ == "__main__":
    main()
