from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "lake.raw.events"


def main() -> None:
    spark = SparkSession.builder.appName("check_raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    events = spark.table(TABLE)

    print(f"rows: {events.count():,}")
    events.groupBy("event_type").count().orderBy(F.desc("count")).show(truncate=False)

    duplicates = events.groupBy("kafka_partition", "kafka_offset").count().filter("count > 1")
    print(f"duplicate kafka offsets: {duplicates.count()}")

    print("offsets by partition (rows should equal max - min + 1):")
    events.groupBy("kafka_partition").agg(
        F.count("*").alias("rows"),
        F.min("kafka_offset").alias("min_offset"),
        F.max("kafka_offset").alias("max_offset"),
    ).orderBy("kafka_partition").show()

    spark.sql(
        f"""
        SELECT count(*) AS files,
               sum(record_count) AS records,
               round(avg(file_size_in_bytes) / 1e6, 2) AS avg_file_mb
        FROM {TABLE}.files
        """
    ).show()
    spark.sql(f"SELECT count(*) AS snapshots FROM {TABLE}.snapshots").show()


if __name__ == "__main__":
    main()
