import argparse
import json
import os
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

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
    ).select(
        "event.*",
        "schema_id",
        "kafka_partition",
        "kafka_offset",
        F.current_timestamp().alias("ingested_at"),
    )


def create_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.raw")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
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


def evolve_table(spark: SparkSession, events: DataFrame) -> list[str]:
    existing = set(spark.table(TABLE).columns)
    added = []
    for field in events.schema.fields:
        if field.name not in existing:
            spark.sql(
                f"ALTER TABLE {TABLE} ADD COLUMN {field.name} {field.dataType.simpleString()}"
            )
            added.append(field.name)
    return added


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Kafka events into Iceberg.")
    parser.add_argument("--once", action="store_true", help="ingest what is in Kafka, then stop")
    parser.add_argument("--checkpoint", default="/checkpoints/ingest_raw")
    parser.add_argument("--max-offsets", type=int, default=500_000)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--max-schema-version", type=int, default=None)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ingest_raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_table(spark)

    events = kafka_events(spark, args.max_offsets, args.max_schema_version)
    for column in evolve_table(spark, events):
        print(f"added column {column} to {TABLE}")
    events = events.select(*spark.table(TABLE).columns)

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

    writer.toTable(TABLE).awaitTermination()


if __name__ == "__main__":
    main()
