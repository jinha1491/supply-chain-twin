import argparse

import pandas as pd
from aggregate import inventory_delta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from twin.stockouts import track_episodes

RAW = "lake.raw.events"
POLICY = "lake.ref.policy"
EPISODES = "lake.gold.stockout_episodes"
OUTPUT_SCHEMA = (
    "site string, part_id string, out_start timestamp, out_end timestamp, duration_days double"
)
STATE_SCHEMA = "on_hand long, out_since_us long"


def make_handler(opening: dict):
    def handle(key, batches, state):
        site, part_id = key
        events = pd.concat(list(batches), ignore_index=True)
        if state.exists:
            on_hand, out_since_us = state.get
        else:
            on_hand, out_since_us = opening.get((site, part_id), 0), None

        episodes, on_hand, out_since_us = track_episodes(
            site, part_id, events, on_hand, out_since_us
        )
        state.update((on_hand, out_since_us))
        if not episodes.empty:
            yield episodes

    return handle


def main() -> None:
    parser = argparse.ArgumentParser(description="Track stockout episodes per site and part.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dc", default="central_dc")
    parser.add_argument("--checkpoint", default="/checkpoints/stockouts")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("stockouts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    opening = {
        (row.site, row.part_id): int(row.order_up_to) for row in spark.table(POLICY).collect()
    }

    changes = (
        spark.readStream.format("iceberg")
        .load(RAW)
        .withColumn("delta", inventory_delta(args.dc))
        .filter(F.col("delta") != 0)
        .select("site", "part_id", "event_time", "delta")
    )
    episodes = changes.groupBy("site", "part_id").applyInPandasWithState(
        make_handler(opening),
        OUTPUT_SCHEMA,
        STATE_SCHEMA,
        "append",
        GroupStateTimeout.NoTimeout,
    )

    writer = (
        episodes.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
    )
    writer = (
        writer.trigger(availableNow=True)
        if args.once
        else writer.trigger(processingTime="10 seconds")
    )
    query = writer.toTable(EPISODES)
    query.awaitTermination()

    rows = sum(p["numInputRows"] for p in query.recentProgress)
    print(f"stockouts: {len(query.recentProgress)} batches, {rows:,} input rows")


if __name__ == "__main__":
    main()
