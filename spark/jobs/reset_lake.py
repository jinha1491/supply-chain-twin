import argparse

from pyspark.sql import SparkSession

GOLD = [
    "lake.gold.inventory_flow",
    "lake.gold.daily_service",
    "lake.gold.daily_service_wm7",
    "lake.gold.daily_service_wm30",
    "lake.gold.stockout_episodes",
]
RAW = ["lake.raw.events"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop lakehouse tables.")
    parser.add_argument("--gold-only", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("reset_lake").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for table in GOLD if args.gold_only else GOLD + RAW:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        print(f"dropped {table}")


if __name__ == "__main__":
    main()
