import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

from twin.network import NetworkConfig, load_config
from twin.policy import policy_from_table
from twin.sim.demand import generate_orders
from twin.sim.engine import simulate
from twin.sim.events import Emitter
from twin.sim.sinks import ParquetSink
from twin.stream.kafka_sink import KafkaSink

CONFIG = "config/network.yaml"


def horizon_days(demand: pd.DataFrame) -> float:
    start = demand["month"].min()
    end = demand["month"].max() + pd.offsets.MonthBegin(1)
    return float((end - start).days)


def make_sink(kind: str, run_id: str, out_dir: str):
    if kind == "kafka":
        return KafkaSink.from_settings()
    return ParquetSink(Path(out_dir) / f"{run_id}.parquet")


def run(run_id: str, snapshot_every_days: float, sink) -> float:
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
    args = parser.parse_args()

    sink = make_sink(args.sink, args.run_id, args.out_dir)
    elapsed = run(args.run_id, args.snapshot_every_days, sink)
    print(f"{sink.count:,} events in {elapsed:.1f}s ({sink.count / elapsed:,.0f} events/s)")

    if args.sink == "parquet":
        print(f"{sink.path.stat().st_size / 1e6:.1f} MB")
        summarize(sink.path, load_config(CONFIG))


if __name__ == "__main__":
    main()
