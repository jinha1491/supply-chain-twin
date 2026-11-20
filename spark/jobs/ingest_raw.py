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


def latest_schema(subject: str) -> str:
    with urllib.request.urlopen(f"{REGISTRY}/subjects/{subject}/versions/latest") as response:
        return json.load(response)["schema"]


def kafka_events(spark: SparkSession, max_offsets: int) -> DataFrame:
    schema = latest_schema(f"{TOPIC}-value")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    return raw.select(
        from_avro(avro_payload, schema).alias("event"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select(
        "event.*", "kafka_partition", "kafka_offset", F.current_timestamp().alias("ingested_at")
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Kafka events into Iceberg.")
    parser.add_argument("--once", action="store_true", help="ingest what is in Kafka, then stop")
    parser.add_argument("--checkpoint", default="/checkpoints/ingest_raw")
    parser.add_argument("--max-offsets", type=int, default=500_000)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ingest_raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_table(spark)

    writer = (
        kafka_events(spark, args.max_offsets)
        .writeStream.format("iceberg")
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
