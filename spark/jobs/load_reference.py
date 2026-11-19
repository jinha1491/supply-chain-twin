from pyspark.sql import SparkSession

TABLES = {
    "lake.ref.policy": "/app/data/processed/policy.parquet",
    "lake.ref.parts": "/app/data/processed/parts.parquet",
}


def main() -> None:
    spark = SparkSession.builder.appName("load_reference").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ref")

    for table, path in TABLES.items():
        spark.read.parquet(path).writeTo(table).createOrReplace()
        print(f"{table}: {spark.table(table).count():,} rows")


if __name__ == "__main__":
    main()
