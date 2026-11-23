import argparse

from pyspark.sql import SparkSession


def report(spark: SparkSession, table: str) -> None:
    latency = spark.sql(
        f"""
        WITH files AS (
            SELECT data_file.file_path AS file, snapshot_id
            FROM {table}.entries
            WHERE status = 1
        ),
        per_row AS (
            SELECT
                (unix_micros(s.committed_at) - unix_micros(e.kafka_timestamp)) / 1e6 AS seconds,
                ntile(3) OVER (ORDER BY e.kafka_timestamp) AS third
            FROM {table} e
            JOIN files f ON e._file = f.file
            JOIN {table}.snapshots s ON s.snapshot_id = f.snapshot_id
        )
        SELECT
            count(*) AS rows,
            percentile(seconds, 0.5) AS p50,
            percentile(seconds, 0.95) AS p95,
            percentile(seconds, 0.99) AS p99,
            max(seconds) AS max,
            percentile(CASE WHEN third = 1 THEN seconds END, 0.5) AS p50_first_third,
            percentile(CASE WHEN third = 3 THEN seconds END, 0.5) AS p50_last_third
        FROM per_row
        """
    ).first()
    print(
        f"{table}: {latency['rows']:,} rows, latency p50 {latency['p50']:.2f}s, "
        f"p95 {latency['p95']:.2f}s, p99 {latency['p99']:.2f}s, max {latency['max']:.2f}s, "
        f"median first third {latency['p50_first_third']:.2f}s, "
        f"last third {latency['p50_last_third']:.2f}s",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end latency per table.")
    parser.add_argument("--tables", required=True, help="comma-separated table names")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("latency_report").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for table in args.tables.split(","):
        report(spark, table)


if __name__ == "__main__":
    main()
