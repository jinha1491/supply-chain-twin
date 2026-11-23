import argparse
from functools import reduce

from aggregate import count_of
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "lake.raw.events"
DAILY = "lake.gold.daily_service"


def approx_batch_time(spark: SparkSession, batch: int, max_offsets: int, total: int):
    fraction = min(batch * max_offsets / total, 1.0)
    sizes = spark.table(RAW).groupBy("kafka_partition").count().collect()
    points = [
        (F.col("kafka_partition") == row["kafka_partition"])
        & (F.col("kafka_offset") == int(fraction * (row["count"] - 1)))
        for row in sizes
    ]
    return (
        spark.table(RAW)
        .filter(reduce(lambda a, b: a | b, points))
        .agg(F.max("event_time"))
        .first()[0]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches", default="25,47,64", help="first batch after each restart")
    parser.add_argument("--max-offsets", type=int, default=10_000)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("diag_recovery").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    total = spark.table(RAW).count()

    print("restart batches (approximate event time where each started):")
    for batch in (int(b) for b in args.batches.split(",")):
        print(f"  batch {batch}: {approx_batch_time(spark, batch, args.max_offsets, total)}")

    expected = (
        spark.table(RAW)
        .filter(F.col("event_type").isin("demand", "backorder"))
        .groupBy(F.window("event_time", "1 day")["start"].alias("day"), "site")
        .agg(
            count_of("demand").alias("expected_orders"), count_of("backorder").alias("expected_bo")
        )
    )
    diff = (F.col("orders") - F.col("expected_orders")) + (
        F.col("backorders") - F.col("expected_bo")
    )
    print("mismatched windows (streamed minus expected):")
    expected.join(spark.table(DAILY), ["day", "site"]).withColumn("diff", diff).filter(
        F.col("diff") != 0
    ).orderBy("day", "site").show(20, truncate=False)


if __name__ == "__main__":
    main()
