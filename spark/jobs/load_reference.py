from pyspark.sql import SparkSession

POLICY_FILE = "/app/data/processed/policy.parquet"
TABLE = "lake.ref.policy"


def main() -> None:
    spark = SparkSession.builder.appName("load_reference").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ref")

    policy = spark.read.parquet(POLICY_FILE)
    policy.writeTo(TABLE).createOrReplace()
    print(f"{TABLE}: {spark.table(TABLE).count():,} rows")


if __name__ == "__main__":
    main()
