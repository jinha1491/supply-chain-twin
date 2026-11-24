import argparse
import json
import os
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQueryListener

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.environ.get("TWIN_TOPIC", "twin.events")
TABLE = "lake.raw.events"


def get_json(path: str):
    with urllib.request.urlopen(f"{REGISTRY}{path}") as response:
        return json.load(response)


def registered_schemas(subject: str, max_version: int | None = None) -> dict[int, str]:
    schemas = {}
    for version in get_json(f"/subjects/{subject}/versions"):
        if max_version is not None and version > max_version:
            continue
        info = get_json(f"/subjects/{subject}/versions/{version}")
        schemas[info["id"]] = info["schema"]
    return schemas


def kafka_events(
    spark: SparkSession, max_offsets: int, max_version: int | None = None
) -> DataFrame:
    schemas = registered_schemas(f"{TOPIC}-value", max_version)
    reader_schema = list(schemas.values())[-1]
    known_ids = list(schemas)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )
    unknown = F.raise_error(
        F.concat(
            F.lit("unknown schema id "),
            F.col("schema_id").cast("string"),
            F.lit(": restart the job to load the new schema"),
        )
    )
    checked = (
        raw.withColumn(
            "schema_id", F.conv(F.hex(F.expr("substring(value, 2, 4)")), 16, 10).cast("int")
        )
        .withColumn("payload", F.expr("substring(value, 6, length(value) - 5)"))
        .filter(F.when(F.col("schema_id").isin(known_ids), F.lit(True)).otherwise(unknown))
    )

    event = None
    for schema_id, writer_schema in schemas.items():
        decoded = from_avro("payload", writer_schema, {"avroSchema": reader_schema})
        matches = F.col("schema_id") == schema_id
        event = F.when(matches, decoded) if event is None else event.when(matches, decoded)

    return checked.select(
        event.alias("event"),
        "schema_id",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        "event.*",
        "schema_id",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
    )


def create_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            event_id STRING,
            event_time TIMESTAMP,
            event_type STRING,
            part_id STRING,
            site STRING,
            quantity INT,
            ref_id STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(event_time))
        """
    )


def evolve_table(spark: SparkSession, table: str, events: DataFrame) -> list[str]:
    existing = set(spark.table(table).columns)
    added = []
    for field in events.schema.fields:
        if field.name not in existing:
            column_type = field.dataType.simpleString()
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {field.name} {column_type}")
            added.append(field.name)
    return added


class ProgressTotals(StreamingQueryListener):
    def __init__(self) -> None:
        self.batches = 0
        self.rows = 0
        self.durations: list[float] = []

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        if progress.numInputRows > 0:
            self.batches += 1
            self.rows += progress.numInputRows
            self.durations.append(progress.durationMs.get("triggerExecution", 0) / 1000)

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        pass


def summarize(totals: ProgressTotals, started: float) -> None:
    elapsed = time.perf_counter() - started
    print(
        f"ingest: {totals.batches} batches, {totals.rows:,} rows in {elapsed:.1f}s "
        f"({totals.rows / elapsed:,.0f} rows/s)",
        flush=True,
    )
    durations = sorted(totals.durations)
    if durations:
        p50 = durations[len(durations) // 2]
        p95 = durations[int(0.95 * (len(durations) - 1))]
        print(f"ingest: batch duration p50 {p50:.2f}s, p95 {p95:.2f}s", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Kafka events into Iceberg.")
    parser.add_argument("--once", action="store_true", help="ingest what is in Kafka, then stop")
    parser.add_argument("--table", default=TABLE)
    parser.add_argument("--fresh", action="store_true", help="drop the table first")
    parser.add_argument("--checkpoint", default="/checkpoints/ingest_raw")
    parser.add_argument("--max-offsets", type=int, default=500_000)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--run-seconds", type=int, default=0)
    parser.add_argument("--max-schema-version", type=int, default=None)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ingest_raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if args.fresh:
        spark.sql(f"DROP TABLE IF EXISTS {args.table} PURGE")
    create_table(spark, args.table)

    events = kafka_events(spark, args.max_offsets, args.max_schema_version)
    for column in evolve_table(spark, args.table, events):
        print(f"added column {column} to {args.table}", flush=True)
    events = events.select(*spark.table(args.table).columns)

    totals = ProgressTotals()
    spark.streams.addListener(totals)
    writer = (
        events.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .option("fanout-enabled", "true")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.toTable(args.table)
    started = time.perf_counter()
    print("ingest: query started", flush=True)
    if args.run_seconds:
        query.awaitTermination(args.run_seconds)
        query.stop()
    else:
        query.awaitTermination()
    time.sleep(2)
    summarize(totals, started)


if __name__ == "__main__":
    main()
