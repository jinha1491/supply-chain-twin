from check_recovery import check_raw
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "lake.raw.events"


def main() -> None:
    spark = SparkSession.builder.appName("check_schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = spark.table(RAW)

    print(f"columns: {raw.columns}")
    check_raw(spark)

    print("rows by schema id:")
    raw.groupBy("schema_id").agg(
        F.count("*").alias("rows"),
        F.min("event_time").alias("first_event"),
        F.max("event_time").alias("last_event"),
    ).orderBy("schema_id").show(truncate=False)

    print("shipments by schema id:")
    raw.filter(F.col("event_type") == "shipment_dispatched").groupBy("schema_id").agg(
        F.count("*").alias("shipments"),
        F.count("carrier_id").alias("with_carrier"),
    ).orderBy("schema_id").show()

    print("carriers on v2 shipments:")
    raw.filter(F.col("carrier_id").isNotNull()).groupBy("site", "carrier_id").count().orderBy(
        "site"
    ).show()

    print("table metadata versions:")
    spark.sql(
        f"SELECT timestamp, latest_schema_id FROM {RAW}.metadata_log_entries ORDER BY timestamp"
    ).show(truncate=False)


if __name__ == "__main__":
    main()
