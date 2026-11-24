import argparse
import statistics
import time

from pyspark.sql import SparkSession

QUERIES = {
    "full scan aggregate": (
        "SELECT event_type, count(*), sum(quantity) FROM {t} GROUP BY event_type"
    ),
    "one month by site": (
        "SELECT site, count(*) FROM {t} "
        "WHERE event_time >= '2000-03-01' AND event_time < '2000-04-01' GROUP BY site"
    ),
    "one part history": (
        "SELECT event_id, event_time, event_type, site, quantity FROM {t} "
        "WHERE part_id = 'T2226' ORDER BY event_id"
    ),
}


def file_stats(spark: SparkSession, table: str):
    return spark.sql(
        f"""
        SELECT count(*) AS files, sum(record_count) AS rows,
               avg(file_size_in_bytes) / 1e6 AS avg_mb, sum(file_size_in_bytes) / 1e6 AS total_mb
        FROM {table}.files
        """
    ).first()


def fingerprint(spark: SparkSession, table: str):
    return spark.sql(
        "SELECT count(*) AS rows, sum(hash(event_id, event_time, quantity)) AS checksum "
        f"FROM {table}"
    ).first()


def time_queries(spark: SparkSession, table: str, runs: int) -> dict[str, float]:
    medians = {}
    for name, template in QUERIES.items():
        sql = template.format(t=table)
        spark.sql(sql).collect()
        times = []
        for _ in range(runs):
            started = time.perf_counter()
            spark.sql(sql).collect()
            times.append(time.perf_counter() - started)
        medians[name] = statistics.median(times)
    return medians


def show_files(label: str, stats) -> None:
    print(
        f"  {label}: {stats['files']:,} files, {stats['rows']:,} rows, "
        f"avg {stats['avg_mb']:.2f} MB, total {stats['total_mb']:.1f} MB",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Query speed before and after compaction.")
    parser.add_argument("--tables", required=True)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("compact_bench").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in args.tables.split(","):
        short = table.removeprefix("lake.")
        print(f"{table}", flush=True)

        before_files = file_stats(spark, table)
        before_print = fingerprint(spark, table)
        before_times = time_queries(spark, table, args.runs)
        show_files("before", before_files)

        started = time.perf_counter()
        result = spark.sql(
            f"CALL lake.system.rewrite_data_files(table => '{short}', "
            "options => map('target-file-size-bytes', '134217728'))"
        ).first()
        compact_seconds = time.perf_counter() - started
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        spark.sql(
            f"CALL lake.system.expire_snapshots(table => '{short}', "
            f"older_than => TIMESTAMP '{now}', retain_last => 1)"
        )

        after_files = file_stats(spark, table)
        after_print = fingerprint(spark, table)
        after_times = time_queries(spark, table, args.runs)
        show_files("after ", after_files)
        print(
            f"  compaction: {result['rewritten_data_files_count']:,} files rewritten into "
            f"{result['added_data_files_count']:,} in {compact_seconds:.1f}s",
            flush=True,
        )
        print(f"  data unchanged: {before_print == after_print}", flush=True)
        for name in QUERIES:
            b, a = before_times[name], after_times[name]
            print(f"  {name:<20} {b:6.2f}s -> {a:6.2f}s  ({b / a:4.1f}x)", flush=True)


if __name__ == "__main__":
    main()
