import argparse
from collections import defaultdict
from datetime import UTC

import numpy as np
import pandas as pd

from twin.network import load_config
from twin.policy import (
    DAYS_PER_MONTH,
    build_policy_table,
    daily_rates,
    load_policy_settings,
    policy_from_table,
    vector_levels,
)
from twin.run import horizon_days
from twin.sim.demand import generate_orders
from twin.sim.engine import simulate
from twin.sim.events import Emitter, Event, EventType

CONFIG = "config/network.yaml"
METHODS = ["naive", "moving_average_12", "ses", "croston", "sba", "tsb", "lightgbm"]


class WindowCounter:
    def __init__(self, window_start) -> None:
        self.window_start = (
            window_start if window_start.tzinfo else window_start.replace(tzinfo=UTC)
        )
        self.demand: dict[str, int] = defaultdict(int)
        self.backorders: dict[str, int] = defaultdict(int)

    def emit(self, event: Event) -> None:
        if event.event_time < self.window_start:
            return
        if event.event_type == EventType.DEMAND:
            self.demand[event.site] += 1
        elif event.event_type == EventType.BACKORDER:
            self.backorders[event.site] += 1

    def fill_rate(self) -> float:
        return 1 - sum(self.backorders.values()) / sum(self.demand.values())


class InventorySampler:
    def __init__(self, start_day: float, end_day: float) -> None:
        self.start_day = start_day
        self.end_day = end_day
        self.totals: dict[str, float] = defaultdict(float)
        self.samples = 0

    def __call__(self, sim):
        yield sim.env.timeout(self.start_day)
        while sim.env.now < self.end_day:
            for (site, _), units in sim.on_hand.items():
                self.totals[site] += units
            self.samples += 1
            yield sim.env.timeout(1)

    def average(self, sites) -> float:
        return sum(self.totals[s] for s in sites) / self.samples


def main() -> None:
    parser = argparse.ArgumentParser(description="Inventory results of forecast-driven policies.")
    parser.add_argument("--tracking-uri", default="sqlite:///mlflow.db")
    args = parser.parse_args()

    config = load_config(CONFIG)
    settings = load_policy_settings(CONFIG)
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")
    forecasts = pd.read_parquet("data/processed/forecasts.parquet")

    start = demand["month"].min()
    horizon = horizon_days(demand)
    supplier = parts["supplier"]
    oracle = policy_from_table(
        build_policy_table(config, settings, daily_rates(demand, parts.index), supplier),
        config.dc_name,
    )
    orders = generate_orders(
        demand, parts.index, config.regions, np.random.default_rng(config.seed)
    )

    months = sorted(forecasts["month"].unique())
    window_day = float((months[0] - start).days)
    regions = [r.name for r in config.regions]

    rows = []
    for method in ["perfect_information", *METHODS]:
        updates = []
        if method != "perfect_information":
            chosen = forecasts[forecasts["method"] == method]
            for month in months:
                monthly = chosen[chosen["month"] == month].set_index("part_id")["forecast"]
                rates = monthly.clip(lower=0) / DAYS_PER_MONTH
                day = float((month - start).days)
                updates.append((day, *vector_levels(config, settings, rates, supplier)))

        counter = WindowCounter(start.to_pydatetime() + pd.Timedelta(days=window_day))
        sampler = InventorySampler(window_day, horizon)
        simulate(
            config,
            supplier.to_dict(),
            oracle,
            orders,
            Emitter(method, start.to_pydatetime(), counter),
            np.random.default_rng(config.seed + 1),
            until=horizon,
            policy_updates=updates,
            observers=[sampler],
        )
        rows.append(
            {
                "method": method,
                "fill_rate": counter.fill_rate(),
                "region_units": sampler.average(regions),
                "dc_units": sampler.average([config.dc_name]),
            }
        )
        print(f"finished {method}", flush=True)

    results = pd.DataFrame(rows).set_index("method")
    results["total_units"] = results["region_units"] + results["dc_units"]
    base = results.loc["perfect_information"]
    results["fill_vs_perfect"] = results["fill_rate"] - base["fill_rate"]
    results["units_vs_perfect"] = results["total_units"] / base["total_units"] - 1
    print(results.round(4).to_string())

    import mlflow

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment("demand-forecasting")
    with mlflow.start_run(run_name="inventory-simulation"):
        mlflow.log_params({"first_month": str(months[0].date()), "months": len(months)})
        for name, row in results.iterrows():
            with mlflow.start_run(run_name=f"inventory-{name}", nested=True):
                mlflow.log_metrics(row.to_dict())


if __name__ == "__main__":
    main()
