import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, timedelta

import numpy as np
import pandas as pd

from twin.forecast.inventory import InventorySampler
from twin.network import load_config
from twin.optimize.allocate import allocate, segments
from twin.optimize.tradeoff import CONFIG, interval, policy_at
from twin.policy import daily_rates, load_policy_settings
from twin.run import horizon_days
from twin.sim.demand import generate_orders
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter, EventType

SERVICE_DAYS = 3.0
GREEDY_BUDGETS = [8000, 6000]
OUTPUT = "data/processed/service.parquet"


def days_between(later, earlier) -> float:
    return (later - earlier).total_seconds() / 86400


class ServiceTracker:
    def __init__(self, window_start, dc_name: str) -> None:
        self.window_start = window_start
        self.dc = dc_name
        self.demand = 0
        self.backorders = 0
        self.open_backorders: dict[str, object] = {}
        self.waits: list[float] = []
        self.open_requests: dict[str, object] = {}
        self.dc_delays: list[float] = []

    def emit(self, event) -> None:
        if event.event_time < self.window_start:
            return
        kind = event.event_type
        if kind == EventType.DEMAND:
            self.demand += 1
        elif kind == EventType.BACKORDER:
            self.backorders += 1
            self.open_backorders[event.ref_id] = event.event_time
        elif kind == EventType.FULFILLED:
            opened = self.open_backorders.pop(event.ref_id, None)
            if opened is not None:
                self.waits.append(days_between(event.event_time, opened))
        elif kind == EventType.REPLENISHMENT_ORDER and event.site != self.dc:
            self.open_requests[event.ref_id] = event.event_time
        elif kind == EventType.SHIPMENT_DISPATCHED and event.site == self.dc:
            opened = self.open_requests.pop(event.ref_id, None)
            if opened is not None:
                self.dc_delays.append(days_between(event.event_time, opened))

    def summary(self) -> dict:
        waits = np.array(self.waits)
        delays = np.array(self.dc_delays)
        served_late = int(np.sum(waits <= SERVICE_DAYS + 1e-9))
        return {
            "fill_rate": 1 - self.backorders / self.demand,
            "served_in_3d": (self.demand - self.backorders + served_late) / self.demand,
            "mean_wait": float(waits.mean()) if len(waits) else 0.0,
            "p95_wait": float(np.percentile(waits, 95)) if len(waits) else 0.0,
            "unfilled_share": len(self.open_backorders) / max(self.backorders, 1),
            "dc_delay_mean": float(delays.mean()) if len(delays) else 0.0,
            "dc_delayed_share": float(np.mean(delays > 1e-9)) if len(delays) else 0.0,
        }


def run_service(task: dict) -> dict:
    config = task["config"]
    tracker = ServiceTracker(task["start"] + timedelta(days=task["window_day"]), config.dc_name)
    sampler = InventorySampler(task["window_day"], task["horizon"])
    simulate(
        config,
        task["supplier"],
        task["policy"],
        task["orders"],
        Emitter(task["label"], task["start"], tracker),
        np.random.default_rng(task["seed"] + 1000),
        until=task["horizon"],
        observers=[sampler],
    )
    sites = [r.name for r in config.regions] + [config.dc_name]
    return {
        "label": task["label"],
        "seed": task["seed"],
        "units": sampler.average(sites),
        **tracker.summary(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Wait times and DC delays by allocation.")
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
    base = policy_at(config, settings, rates, supplier, settings.region_fill_target)
    segs = segments(config, rates)

    base_budget = sum(base.region_base_stock.values())
    policies = {f"uniform@{base_budget}": base}
    for budget in [base_budget, *GREEDY_BUDGETS]:
        levels = allocate(segs, budget)
        policies[f"greedy@{budget}"] = Policy(levels, base.dc_reorder_point, base.dc_order_up_to)

    common = {
        "config": config,
        "supplier": supplier.to_dict(),
        "start": start.to_pydatetime().replace(tzinfo=UTC),
        "horizon": horizon_days(demand),
        "window_day": float((min(forecasts["month"]) - start).days),
    }
    tasks = []
    for seed in range(args.seeds):
        orders = generate_orders(demand, parts.index, config.regions, np.random.default_rng(seed))
        for label, policy in policies.items():
            tasks.append(
                {**common, "label": label, "seed": seed, "orders": orders, "policy": policy}
            )

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = pd.DataFrame(pool.map(run_service, tasks))
    results.to_parquet(OUTPUT, index=False)

    print(f"{args.seeds} seeds, 95% CI on fill and served-in-3-days:\n")
    print(
        f"{'policy':<15} {'fill':>15} {'served <=3d':>15} {'mean wait':>10} "
        f"{'p95 wait':>9} {'unfilled':>9} {'units':>8}"
    )
    for label in policies:
        rows = results[results["label"] == label]
        fill, fill_ci = interval(rows["fill_rate"])
        served, served_ci = interval(rows["served_in_3d"])
        print(
            f"{label:<15} {fill:.4f} +/- {fill_ci:.4f} {served:.4f} +/- {served_ci:.4f} "
            f"{rows['mean_wait'].mean():>9.2f}d {rows['p95_wait'].mean():>8.2f}d "
            f"{rows['unfilled_share'].mean():>9.2%} {rows['units'].mean():>8,.0f}"
        )

    print("\nDC delay per regional request (should match across policies at each seed):")
    delays = results.pivot(index="seed", columns="label", values="dc_delay_mean")
    print(delays.round(4).to_string())
    delayed = results.groupby("label")["dc_delayed_share"].mean()
    print(f"\nshare of regional requests delayed at the DC: {delayed.mean():.2%}")

    import mlflow

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment("inventory-optimization")
    with mlflow.start_run(run_name="service-and-waits"):
        mlflow.log_params({"seeds": args.seeds, "policies": ",".join(policies)})
        mlflow.log_artifact(OUTPUT)


if __name__ == "__main__":
    main()
