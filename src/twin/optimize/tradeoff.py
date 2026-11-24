import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from datetime import UTC, timedelta

import numpy as np
import pandas as pd
from scipy import stats

from twin.forecast.inventory import InventorySampler, WindowCounter
from twin.network import load_config
from twin.policy import DAYS_PER_MONTH, daily_rates, load_policy_settings, vector_levels
from twin.run import horizon_days
from twin.sim.demand import generate_orders
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter

CONFIG = "config/network.yaml"
TARGETS = [0.80, 0.90, 0.95, 0.98, 0.99, 0.995]
METHODS = ["lightgbm", "ses", "moving_average_12"]
OUTPUT = "data/processed/tradeoff.parquet"


def policy_at(config, settings, rates: pd.Series, supplier: pd.Series, target: float) -> Policy:
    levels = vector_levels(config, replace(settings, region_fill_target=target), rates, supplier)
    return Policy(*levels)


def interval(values) -> tuple[float, float]:
    values = np.asarray(values, dtype=float)
    if len(values) < 2 or values.std(ddof=1) == 0:
        return float(values.mean()), 0.0
    half = stats.t.ppf(0.975, len(values) - 1) * values.std(ddof=1) / np.sqrt(len(values))
    return float(values.mean()), float(half)


def run_one(task: dict) -> dict:
    config = task["config"]
    counter = WindowCounter(task["start"] + timedelta(days=task["window_day"]))
    sampler = InventorySampler(task["window_day"], task["horizon"])
    simulate(
        config,
        task["supplier"],
        task["policy"],
        task["orders"],
        Emitter(task["label"], task["start"], counter),
        np.random.default_rng(task["seed"] + 1000),
        until=task["horizon"],
        policy_updates=task["updates"],
        observers=[sampler],
    )
    sites = [r.name for r in config.regions] + [config.dc_name]
    return {
        "label": task["label"],
        "seed": task["seed"],
        "fill_rate": counter.fill_rate(),
        "units": sampler.average(sites),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill rate vs inventory, with seeds.")
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--tracking-uri", default="sqlite:///mlflow.db")
    args = parser.parse_args()

    config = load_config(CONFIG)
    settings = load_policy_settings(CONFIG)
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")
    forecasts = pd.read_parquet("data/processed/forecasts.parquet")

    supplier = parts["supplier"]
    rates = daily_rates(demand, parts.index)
    start = demand["month"].min()
    start_utc = start.to_pydatetime().replace(tzinfo=UTC)
    horizon = horizon_days(demand)
    months = sorted(forecasts["month"].unique())
    window_day = float((months[0] - start).days)

    base_policy = policy_at(config, settings, rates, supplier, settings.region_fill_target)
    method_updates = {}
    for method in METHODS:
        chosen = forecasts[forecasts["method"] == method]
        updates = []
        for month in months:
            monthly = chosen[chosen["month"] == month].set_index("part_id")["forecast"]
            levels = vector_levels(
                config, settings, monthly.clip(lower=0) / DAYS_PER_MONTH, supplier
            )
            updates.append((float((month - start).days), *levels))
        method_updates[method] = updates

    common = {
        "config": config,
        "supplier": supplier.to_dict(),
        "start": start_utc,
        "horizon": horizon,
        "window_day": window_day,
    }
    tasks = []
    for seed in range(args.seeds):
        orders = generate_orders(demand, parts.index, config.regions, np.random.default_rng(seed))
        for target in TARGETS:
            policy = policy_at(config, settings, rates, supplier, target)
            tasks.append(
                {
                    **common,
                    "label": f"perfect@{target}",
                    "seed": seed,
                    "orders": orders,
                    "policy": policy,
                    "updates": [],
                }
            )
        for method in METHODS:
            tasks.append(
                {
                    **common,
                    "label": method,
                    "seed": seed,
                    "orders": orders,
                    "policy": base_policy,
                    "updates": method_updates[method],
                }
            )

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = pd.DataFrame(pool.map(run_one, tasks))
    results.to_parquet(OUTPUT, index=False)

    print(f"{len(tasks)} simulations, {args.seeds} seeds each\n")
    print("perfect-information curve (95% CI across seeds):")
    curve = []
    for target in TARGETS:
        rows = results[results["label"] == f"perfect@{target}"]
        fill, fill_ci = interval(rows["fill_rate"])
        units, units_ci = interval(rows["units"])
        curve.append((units, fill))
        print(
            f"  target {target:<6} fill {fill:.4f} +/- {fill_ci:.4f}   "
            f"units {units:9,.0f} +/- {units_ci:6,.0f}"
        )
    curve_units, curve_fill = map(np.array, zip(*sorted(curve), strict=True))

    print("\nforecast policies vs perfect information at the same seeds:")
    base = results[results["label"] == "perfect@0.95"].set_index("seed")
    for method in METHODS:
        rows = results[results["label"] == method].set_index("seed")
        fill_diff, fill_ci = interval(rows["fill_rate"] - base["fill_rate"])
        unit_diff, unit_ci = interval(rows["units"] / base["units"] - 1)
        units = rows["units"].mean()
        on_curve = np.interp(units, curve_units, curve_fill)
        print(
            f"  {method:<18} fill {fill_diff:+.4f} +/- {fill_ci:.4f}   "
            f"units {unit_diff:+.1%} +/- {unit_ci:.1%}   "
            f"vs curve at same units {rows['fill_rate'].mean() - on_curve:+.4f}"
        )

    import mlflow

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment("inventory-optimization")
    with mlflow.start_run(run_name="tradeoff-curve"):
        mlflow.log_params({"seeds": args.seeds, "targets": ",".join(map(str, TARGETS))})
        mlflow.log_artifact(OUTPUT)


if __name__ == "__main__":
    main()
