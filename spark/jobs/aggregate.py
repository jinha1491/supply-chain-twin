import argparse
import time

from ingest_raw import kafka_events
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


def daily_counts(events: DataFrame, watermark_days: int) -> DataFrame:
    return (
        events.filter(F.col("event_type").isin("demand", "backorder"))
        .withWatermark("event_time", f"{watermark_days} days")
        .groupBy(F.window("event_time", "1 day"), "site")
        .agg(count_of("demand").alias("orders"), count_of("backorder").alias("backorders"))
        .select(F.col("window.start").alias("day"), "site", "orders", "backorders")
    )


def start(frame: DataFrame, table: str, mode: str, checkpoint: str, trigger: str):
    writer = (
        frame.writeStream.format("iceberg")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
    )
    if trigger == "once":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger)
    return writer.toTable(table)


def drain(queries: dict, idle_polls: int, poll_seconds: float = 3.0) -> dict:
    rows: dict[str, dict[int, int]] = {name: {} for name in queries}
    idle = 0
    while idle < idle_polls:
        time.sleep(poll_seconds)
        busy = False
        for name, query in queries.items():
            error = query.exception()
            if error is not None:
                raise error
            for progress in query.recentProgress:
                rows[name][progress["batchId"]] = progress["numInputRows"]
            status = query.status
            if status["isDataAvailable"] or status["isTriggerActive"]:
                busy = True
        started = all(sum(batches.values()) > 0 for batches in rows.values())
        idle = idle + 1 if started and not busy else 0

    for query in queries.values():
        query.stop()
    return {name: (len(batches), sum(batches.values())) for name, batches in rows.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream raw events into gold tables.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--drain", action="store_true", help="run until caught up, then stop")
    parser.add_argument("--idle-polls", type=int, default=3)
    parser.add_argument("--dc", default="central_dc")
    parser.add_argument("--watermark-days", default="2", help="one or more, like 2,7")
    parser.add_argument("--files-per-batch", type=int, default=0, help="0 means no limit")
    parser.add_argument("--checkpoint-root", default="/checkpoints")
    parser.add_argument("--daily-table", default=DAILY)
    parser.add_argument("--skip-flow", action="store_true")
    parser.add_argument("--source", choices=["iceberg", "kafka"], default="iceberg")
    parser.add_argument("--max-offsets", type=int, default=20_000)
    args = parser.parse_args()

    if args.once and args.files_per_batch:
        parser.error(
            "--files-per-batch does not work with --once: Iceberg's streaming source stops "
            "after one limited batch under availableNow. Use --drain instead."
        )
    trigger = "once" if args.once else ("1 second" if args.drain else "10 seconds")
    watermarks = [int(w) for w in args.watermark_days.split(",")]

    spark = SparkSession.builder.appName("aggregate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")

    if args.source == "kafka":
        events = kafka_events(spark, args.max_offsets)
    else:
        reader = spark.readStream.format("iceberg")
        if args.files_per_batch > 0:
            reader = reader.option("streaming-max-files-per-micro-batch", args.files_per_batch)
        events = reader.load(RAW)

    queries = {}
    if not args.skip_flow:
        flow = (
            events.withColumn("delta", inventory_delta(args.dc))
            .filter(F.col("delta") != 0)
            .groupBy("site", "part_id")
            .agg(F.sum("delta").alias("net_flow"), F.max("event_time").alias("as_of"))
        )
        queries["inventory_flow"] = start(
            flow, FLOW, "complete", f"{args.checkpoint_root}/inventory_flow", trigger
        )
    for i, days in enumerate(watermarks):
        table = args.daily_table if i == 0 else f"{args.daily_table}_wm{days}"
        queries[f"daily_wm{days}"] = start(
            daily_counts(events, days),
            table,
            "append",
            f"{args.checkpoint_root}/daily_wm{days}",
            trigger,
        )

    if args.drain:
        results = drain(queries, args.idle_polls)
    else:
        for query in queries.values():
            query.awaitTermination()
        results = {
            name: (len(q.recentProgress), sum(p["numInputRows"] for p in q.recentProgress))
            for name, q in queries.items()
        }
    for name, (batches, rows) in results.items():
        print(f"{name}: {batches} batches, {rows:,} input rows")


if __name__ == "__main__":
    main()
