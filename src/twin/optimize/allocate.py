import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC

import numpy as np
import pandas as pd
from scipy import stats

from twin.network import load_config
from twin.optimize.tradeoff import CONFIG, TARGETS, interval, policy_at, run_one
from twin.policy import daily_rates, load_policy_settings
from twin.run import horizon_days
from twin.sim.demand import generate_orders
from twin.sim.engine import Policy

EXTRA_BUDGETS = [6000, 8000]
TRADEOFF = "data/processed/tradeoff.parquet"
OUTPUT = "data/processed/allocation.parquet"


def upper_hull(y: np.ndarray) -> list[int]:
    hull = []
    for s in range(len(y)):
        while len(hull) >= 2:
            a, b = hull[-2], hull[-1]
            if (y[b] - y[a]) * (s - b) <= (y[s] - y[b]) * (b - a):
                hull.pop()
            else:
                break
        hull.append(s)
    return hull


def segments(config, rates: pd.Series) -> pd.DataFrame:
    rows = []
    for region in config.regions:
        for part, rate in rates.items():
            demand_rate = rate * region.demand_share
            mean = demand_rate * region.transit_days
            if mean <= 0:
                continue
            top = int(stats.poisson.ppf(0.9999, mean)) + 1
            filled = demand_rate * stats.poisson.cdf(np.arange(top + 1) - 1, mean)
            hull = upper_hull(filled)
            for order, (lo, hi) in enumerate(zip(hull[:-1], hull[1:], strict=True)):
                value = (filled[hi] - filled[lo]) / (hi - lo)
                rows.append((region.name, part, order, hi - lo, value))
    return pd.DataFrame(rows, columns=["site", "part_id", "order", "units", "value"])


def allocate(segs: pd.DataFrame, budget: int) -> dict[tuple[str, str], int]:
    ranked = segs.sort_values(["value", "order"], ascending=[False, True], kind="stable")
    chosen = ranked[ranked["units"].cumsum() <= budget]
    levels = chosen.groupby(["site", "part_id"])["units"].sum()
    return {key: int(units) for key, units in levels.items()}


def model_fill(config, rates: pd.Series, levels: dict) -> float:
    rate = rates.to_numpy(dtype=float)
    filled = total = 0.0
    for region in config.regions:
        demand_rate = rate * region.demand_share
        stock = np.array([levels.get((region.name, p), 0) for p in rates.index])
        with np.errstate(invalid="ignore"):
            cdf = stats.poisson.cdf(stock - 1, demand_rate * region.transit_days)
        filled += float(np.sum(np.where(demand_rate > 0, demand_rate * cdf, 0.0)))
        total += float(demand_rate.sum())
    return filled / total


def main() -> None:
    parser = argparse.ArgumentParser(description="Greedy stock allocation under a unit budget.")
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--tracking-uri", default="sqlite:///mlflow.db")
    args = parser.parse_args()

    config = load_config(CONFIG)
    settings = load_policy_settings(CONFIG)
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")
    forecasts = pd.read_parquet("data/processed/forecasts.parquet")
    uniform_runs = pd.read_parquet(TRADEOFF)

    supplier = parts["supplier"]
    rates = daily_rates(demand, parts.index)
    start = demand["month"].min()
    base = policy_at(config, settings, rates, supplier, settings.region_fill_target)
    segs = segments(config, rates)

    uniform = {}
    for target in TARGETS:
        levels = policy_at(config, settings, rates, supplier, target).region_base_stock
        uniform.setdefault(sum(levels.values()), (target, levels))
    budgets = sorted(set(uniform) | set(EXTRA_BUDGETS))

    print("budget  leftover  model fill (uniform -> greedy)  part-regions at 0")
    allocations = {}
    for budget in budgets:
        levels = allocate(segs, budget)
        allocations[budget] = levels
        stocked = sum(v > 0 for v in levels.values())
        zero_share = 1 - stocked / len(segs.groupby(["site", "part_id"]))
        greedy = model_fill(config, rates, levels)
        before = (
            f"{model_fill(config, rates, uniform[budget][1]):.4f}"
            if budget in uniform
            else "  n/a "
        )
        print(
            f"{budget:>6,}  {budget - sum(levels.values()):>8}  "
            f"{before} -> {greedy:.4f}               {zero_share:6.1%}"
        )

    common = {
        "config": config,
        "supplier": supplier.to_dict(),
        "start": start.to_pydatetime().replace(tzinfo=UTC),
        "horizon": horizon_days(demand),
        "window_day": float((min(forecasts["month"]) - start).days),
        "updates": [],
    }
    tasks = []
    for seed in range(args.seeds):
        orders = generate_orders(demand, parts.index, config.regions, np.random.default_rng(seed))
        for budget, levels in allocations.items():
            policy = Policy(levels, base.dc_reorder_point, base.dc_order_up_to)
            tasks.append(
                {
                    **common,
                    "label": f"greedy@{budget}",
                    "seed": seed,
                    "orders": orders,
                    "policy": policy,
                }
            )

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = pd.DataFrame(pool.map(run_one, tasks))
    results.to_parquet(OUTPUT, index=False)

    curve = uniform_runs[uniform_runs["label"].str.startswith("perfect@")]
    curve = curve.groupby("label")[["units", "fill_rate"]].mean().sort_values("units")

    print(f"\nsimulated, {args.seeds} seeds (95% CI):")
    for budget in budgets:
        rows = results[results["label"] == f"greedy@{budget}"].set_index("seed")
        fill, fill_ci = interval(rows["fill_rate"])
        units = rows["units"].mean()
        line = f"  greedy@{budget:<6,} fill {fill:.4f} +/- {fill_ci:.4f}   units {units:9,.0f}"
        if budget in uniform:
            label = f"perfect@{uniform[budget][0]}"
            paired = uniform_runs[uniform_runs["label"] == label].set_index("seed")
            diff, diff_ci = interval(rows["fill_rate"] - paired["fill_rate"])
            line += f"   vs {label} {diff:+.4f} +/- {diff_ci:.4f}"
        if curve["units"].min() <= units <= curve["units"].max():
            on_curve = np.interp(units, curve["units"], curve["fill_rate"])
            line += f"   vs curve {fill - on_curve:+.4f}"
        print(line)

    import mlflow

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment("inventory-optimization")
    with mlflow.start_run(run_name="greedy-allocation"):
        mlflow.log_params({"seeds": args.seeds, "budgets": ",".join(map(str, budgets))})
        mlflow.log_artifact(OUTPUT)


if __name__ == "__main__":
    main()
