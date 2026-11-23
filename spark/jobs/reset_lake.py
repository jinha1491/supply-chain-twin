import argparse

from pyspark.sql import SparkSession

MD_TABLES = ("suppliers", "sites", "parts", "policy")
GROUPS = {
    "gold": [
        "lake.gold.inventory_flow",
        "lake.gold.daily_service",
        "lake.gold.daily_service_wm7",
        "lake.gold.daily_service_wm30",
        "lake.gold.stockout_episodes",
    ],
    "raw": ["lake.raw.events"],
    "md": [f"lake.md.{t}{suffix}" for t in MD_TABLES for suffix in ("", "_history")],
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop lakehouse tables by group.")
    parser.add_argument("--groups", default="gold,raw", help="any of gold, raw, md")
    parser.add_argument("--gold-only", action="store_true")
    args = parser.parse_args()

    groups = ["gold"] if args.gold_only else args.groups.split(",")
    spark = SparkSession.builder.appName("reset_lake").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for group in groups:
        for table in GROUPS[group]:
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
            print(f"dropped {table}")


if __name__ == "__main__":
    main()
