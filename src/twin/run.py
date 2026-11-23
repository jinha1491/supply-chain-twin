import argparse
import time
from datetime import UTC, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from twin.masterdata import MasterDataWriter
from twin.network import NetworkConfig, load_config
from twin.policy import policy_from_table
from twin.scenario import load_scenario
from twin.sim.demand import generate_orders
from twin.sim.engine import simulate
from twin.sim.events import Emitter
from twin.sim.sinks import ParquetSink
from twin.stream.kafka_sink import KafkaSink
from twin.stream.late import LateSink

CONFIG = "config/network.yaml"


def horizon_days(demand: pd.DataFrame) -> float:
    start = demand["month"].min()
    end = demand["month"].max() + pd.offsets.MonthBegin(1)
    return float((end - start).days)


def make_sink(kind: str, run_id: str, out_dir: str, schema_switch=None):
    if kind == "kafka":
        return KafkaSink.from_settings(schema_switch)
    return ParquetSink(Path(out_dir) / f"{run_id}.parquet")


def run(run_id: str, snapshot_every_days: float, sink, changes=None, on_change=None) -> float:
    config = load_config(CONFIG)
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")
    policy = policy_from_table(pd.read_parquet("data/processed/policy.parquet"), config.dc_name)

    rng = np.random.default_rng(config.seed)
    orders = generate_orders(demand, parts.index, config.regions, rng)

    started = time.perf_counter()
    with sink:
        emitter = Emitter(run_id, demand["month"].min().to_pydatetime(), sink)
        simulate(
            config,
            parts["supplier"].to_dict(),
            policy,
            orders,
            emitter,
            rng,
            until=horizon_days(demand),
            snapshot_every_days=snapshot_every_days,
            changes=changes,
            on_change=on_change,
        )
    return time.perf_counter() - started


def summarize(path: Path, config: NetworkConfig) -> None:
    types = pd.read_parquet(path, columns=["event_type"])["event_type"].value_counts()
    print("\nevents by type:")
    print(types.to_string())

    flow = pd.read_parquet(
        path,
        columns=["event_type", "site", "ref_id", "event_time"],
        filters=[("event_type", "in", ["demand", "backorder", "fulfilled"])],
    )
    demand_by_site = flow[flow["event_type"] == "demand"].groupby("site", observed=True).size()
    backorders = flow[flow["event_type"] == "backorder"]
    backorders_by_site = (
        backorders.groupby("site", observed=True).size().reindex(demand_by_site.index, fill_value=0)
    )
    print("\nfill rate by region:")
    print((1 - backorders_by_site / demand_by_site).round(3).to_string())

    fulfilled_at = flow[flow["event_type"] == "fulfilled"].set_index("ref_id")["event_time"]
    waited_until = fulfilled_at.reindex(backorders["ref_id"]).reset_index(drop=True)
    waits = (waited_until - backorders["event_time"].reset_index(drop=True)).dt.total_seconds()
    waits = (waits / 86400).dropna()
    print(f"\nbackorder wait (days): mean {waits.mean():.1f}, p90 {waits.quantile(0.9):.1f}")

    snapshots = pd.read_parquet(
        path,
        columns=["event_time", "site", "quantity"],
        filters=[("event_type", "=", "inventory_snapshot")],
    )
    if snapshots.empty:
        return
    totals = snapshots.groupby(["event_time", "site"], observed=True)["quantity"].sum()
    totals = totals.unstack("site")
    totals.columns = totals.columns.astype(str)
    capacity = {r.name: r.capacity_units for r in config.regions}
    capacity[config.dc_name] = config.dc_capacity_units
    utilization = totals / pd.Series(capacity)
    print("\nutilization of capacity:")
    print(utilization.agg(["mean", "max"]).T.round(3).to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the supply chain twin simulation.")
    parser.add_argument("--run-id", default="baseline")
    parser.add_argument("--snapshot-every-days", type=float, default=1.0)
    parser.add_argument("--sink", choices=["parquet", "kafka"], default="parquet")
    parser.add_argument("--out-dir", default="data/events")
    parser.add_argument("--late-fraction", type=float, default=0.0)
    parser.add_argument("--late-max-days", type=float, default=5.0)
    parser.add_argument("--schema-switch-day", type=float, default=None)
    parser.add_argument("--scenario", default=None, help="scenario YAML file")
    args = parser.parse_args()

    schema_switch = None
    if args.schema_switch_day is not None:
        start = pd.read_parquet(load_config(CONFIG).demand_file)["month"].min()
        schema_switch = start.to_pydatetime().replace(tzinfo=UTC) + timedelta(
            days=args.schema_switch_day
        )
    base = make_sink(args.sink, args.run_id, args.out_dir, schema_switch)
    sink = (
        LateSink(base, args.late_fraction, max_days=args.late_max_days)
        if args.late_fraction
        else base
    )
    changes, writer = [], None
    if args.scenario:
        changes = load_scenario(args.scenario)["changes"]
        if args.sink == "kafka":
            writer = MasterDataWriter()
    on_change = writer.apply if writer else None
    elapsed = run(args.run_id, args.snapshot_every_days, sink, changes, on_change)
    if writer:
        writer.close()
    print(f"{sink.count:,} events in {elapsed:.1f}s ({sink.count / elapsed:,.0f} events/s)")
    if args.late_fraction:
        print(f"{sink.delayed:,} events sent late")

    if args.sink == "kafka":
        print(f"by schema version: {dict(sorted(base.by_version.items()))}")
    if args.sink == "parquet":
        print(f"{base.path.stat().st_size / 1e6:.1f} MB")
        summarize(base.path, load_config(CONFIG))


if __name__ == "__main__":
    main()
