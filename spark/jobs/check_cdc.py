from cdc_merge import TABLES, column_names
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC = "jdbc:postgresql://postgres:5432/masterdata"


def main() -> None:
    spark = SparkSession.builder.appName("check_cdc").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table, (_, columns) in TABLES.items():
        names = column_names(columns)
        source = (
            spark.read.format("jdbc")
            .option("url", JDBC)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table)
            .option("user", "twin")
            .option("password", "twin")
            .load()
            .select(*names)
        )
        lake = spark.table(f"lake.md.{table}").select(*names)
        missing = source.exceptAll(lake).count()
        extra = lake.exceptAll(source).count()
        history = spark.table(f"lake.md.{table}_history").groupBy("op").count().collect()
        ops = ", ".join(f"{r['op']}={r['count']:,}" for r in sorted(history, key=lambda r: r["op"]))
        print(
            f"{table}: postgres {source.count():,}, lake {lake.count():,}, "
            f"missing {missing}, extra {extra}, history ops [{ops}]"
        )

    print("supplier history:")
    spark.table("lake.md.suppliers_history").select(
        "name", "lead_time_mean", "lead_time_std", "effective_at", "op", "source_ts"
    ).orderBy("kafka_offset").show(truncate=False)

    print("deleted parts:")
    spark.table("lake.md.parts_history").filter(F.col("deleted")).select(
        "part_id", "op", "source_ts"
    ).show(truncate=False)


if __name__ == "__main__":
    main()
