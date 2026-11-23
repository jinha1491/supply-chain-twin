import argparse
from datetime import UTC, datetime, timedelta

import pandas as pd
import psycopg

from twin.network import load_config

CONFIG = "config/network.yaml"
ADMIN_DSN = "host=localhost port=5433 user=twin password=twin dbname=iceberg"
DSN = "host=localhost port=5433 user=twin password=twin dbname=masterdata"
SIM_START = datetime(1998, 1, 1, tzinfo=UTC)
TABLES = ("suppliers", "sites", "parts", "policy")

DDL = """
CREATE TABLE IF NOT EXISTS suppliers (
    name text PRIMARY KEY,
    lead_time_mean double precision NOT NULL,
    lead_time_std double precision NOT NULL,
    carrier text,
    effective_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    name text PRIMARY KEY,
    kind text NOT NULL,
    capacity_units integer NOT NULL,
    transit_days double precision,
    demand_share double precision,
    carrier text,
    effective_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS parts (
    part_id text PRIMARY KEY,
    supplier text NOT NULL,
    pattern text NOT NULL,
    effective_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS policy (
    site text NOT NULL,
    part_id text NOT NULL,
    reorder_point integer NOT NULL,
    order_up_to integer NOT NULL,
    effective_at timestamptz NOT NULL,
    PRIMARY KEY (site, part_id)
);
"""


def sim_time(day: float) -> datetime:
    return SIM_START + timedelta(days=day)


def ensure_database() -> None:
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = 'masterdata'").fetchone()
        if not exists:
            conn.execute("CREATE DATABASE masterdata")


def load(reset: bool) -> None:
    ensure_database()
    config = load_config(CONFIG)
    parts = pd.read_parquet("data/processed/parts.parquet")
    policy = pd.read_parquet("data/processed/policy.parquet")
    start = sim_time(0)

    with psycopg.connect(DSN) as conn:
        if reset:
            conn.execute(f"DROP TABLE IF EXISTS {', '.join(TABLES)}")
        conn.execute(DDL)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO suppliers VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [
                    (s.name, s.lead_time_mean, s.lead_time_std, s.carrier, start)
                    for s in config.suppliers
                ],
            )
            sites = [(config.dc_name, "dc", config.dc_capacity_units, None, None, None, start)]
            sites += [
                (
                    r.name,
                    "region",
                    r.capacity_units,
                    r.transit_days,
                    r.demand_share,
                    r.carrier,
                    start,
                )
                for r in config.regions
            ]
            cur.executemany(
                "INSERT INTO sites VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                sites,
            )
            cur.executemany(
                "INSERT INTO parts VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [(part, row.supplier, row.pattern, start) for part, row in parts.iterrows()],
            )
            cur.executemany(
                "INSERT INTO policy VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [
                    (row.site, row.part_id, int(row.reorder_point), int(row.order_up_to), start)
                    for row in policy.itertuples()
                ],
            )
        for table in TABLES:
            count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"{table}: {count:,} rows")


def execute(sql: str, params: tuple) -> int:
    with psycopg.connect(DSN) as conn:
        return conn.execute(sql, params).rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="Master data in Postgres.")
    commands = parser.add_subparsers(dest="command", required=True)

    load_cmd = commands.add_parser("load", help="create tables and load the network")
    load_cmd.add_argument("--reset", action="store_true")

    supplier = commands.add_parser("supplier", help="change a supplier's lead time")
    supplier.add_argument("name")
    supplier.add_argument("--lead-time-mean", type=float, required=True)
    supplier.add_argument("--lead-time-std", type=float, required=True)
    supplier.add_argument("--day", type=float, required=True)

    capacity = commands.add_parser("capacity", help="change a site's capacity")
    capacity.add_argument("site")
    capacity.add_argument("units", type=int)
    capacity.add_argument("--day", type=float, required=True)

    discontinue = commands.add_parser("discontinue", help="delete a part and its policy rows")
    discontinue.add_argument("part_id")

    args = parser.parse_args()
    if args.command == "load":
        load(args.reset)
    elif args.command == "supplier":
        n = execute(
            "UPDATE suppliers SET lead_time_mean = %s, lead_time_std = %s, effective_at = %s"
            " WHERE name = %s",
            (args.lead_time_mean, args.lead_time_std, sim_time(args.day), args.name),
        )
        print(f"updated {n} supplier row")
    elif args.command == "capacity":
        n = execute(
            "UPDATE sites SET capacity_units = %s, effective_at = %s WHERE name = %s",
            (args.units, sim_time(args.day), args.site),
        )
        print(f"updated {n} site row")
    else:
        with psycopg.connect(DSN) as conn:
            policies = conn.execute(
                "DELETE FROM policy WHERE part_id = %s", (args.part_id,)
            ).rowcount
            parts = conn.execute("DELETE FROM parts WHERE part_id = %s", (args.part_id,)).rowcount
        print(f"deleted {parts} part row and {policies} policy rows")


if __name__ == "__main__":
    main()


class MasterDataWriter:
    def __init__(self, dsn: str = DSN) -> None:
        self.conn = psycopg.connect(dsn, autocommit=True)

    def apply(self, change: dict) -> None:
        self.conn.execute(
            "UPDATE suppliers SET lead_time_mean = %s, lead_time_std = %s, effective_at = %s"
            " WHERE name = %s",
            (
                change["lead_time_mean"],
                change["lead_time_std"],
                sim_time(change["day"]),
                change["name"],
            ),
        )
        print(f"day {change['day']}: {change['name']} lead time -> {change['lead_time_mean']}")

    def close(self) -> None:
        self.conn.close()
