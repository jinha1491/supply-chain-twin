import argparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

RAW = "lake.raw.events"
FLOW = "lake.gold.inventory_flow"
DAILY = "lake.gold.daily_service"


def inventory_delta(dc_name: str) -> Column:
    event_type = F.col("event_type")
    quantity = F.col("quantity")
    return (
        F.when(event_type == "receipt", quantity)
        .when(event_type == "fulfilled", -quantity)
        .when((event_type == "shipment_dispatched") & (F.col("site") == dc_name), -quantity)
        .otherwise(0)
    )


def count_of(event_type: str) -> Column:
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))


def start(frame: DataFrame, table: str, mode: str, checkpoint: str, once: bool):
    writer = (
        frame.writeStream.format("iceberg")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
    )
    writer = (
        writer.trigger(availableNow=True) if once else writer.trigger(processingTime="10 seconds")
    )
    return writer.toTable(table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream raw events into gold tables.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dc", default="central_dc")
    parser.add_argument("--watermark-days", type=int, default=2)
    parser.add_argument("--files-per-batch", type=int, default=0, help="0 means no limit")
    parser.add_argument("--checkpoint-root", default="/checkpoints")
    parser.add_argument("--run-seconds", type=int, default=0)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("aggregate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")

    reader = spark.readStream.format("iceberg")
    if args.files_per_batch > 0:
        reader = reader.option("streaming-max-files-per-micro-batch", args.files_per_batch)
    events = reader.load(RAW)

    flow = (
        events.withColumn("delta", inventory_delta(args.dc))
        .filter(F.col("delta") != 0)
        .groupBy("site", "part_id")
        .agg(F.sum("delta").alias("net_flow"), F.max("event_time").alias("as_of"))
    )

    daily = (
        events.filter(F.col("event_type").isin("demand", "backorder"))
        .withWatermark("event_time", f"{args.watermark_days} days")
        .groupBy(F.window("event_time", "1 day"), "site")
        .agg(count_of("demand").alias("orders"), count_of("backorder").alias("backorders"))
        .select(F.col("window.start").alias("day"), "site", "orders", "backorders")
    )

    queries = {
        "inventory_flow": start(
            flow, FLOW, "complete", f"{args.checkpoint_root}/inventory_flow", args.once
        ),
        "daily_service": start(
            daily, DAILY, "append", f"{args.checkpoint_root}/daily_service", args.once
        ),
    }

    if args.run_seconds:
        spark.streams.awaitAnyTermination(args.run_seconds)
        for query in queries.values():
            query.stop()

    for name, query in queries.items():
        query.awaitTermination()
        progress = query.recentProgress
        rows = sum(p["numInputRows"] for p in progress)
        print(f"{name}: {len(progress)} batches, {rows:,} input rows")
        if progress:
            source = progress[-1]["sources"][0]
            print(f"  start={source['startOffset']} end={source['endOffset']}")


if __name__ == "__main__":
    main()
