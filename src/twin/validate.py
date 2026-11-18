from collections import Counter
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats

from twin.network import NetworkConfig, Region, load_config
from twin.policy import daily_rates, policy_from_table
from twin.run import horizon_days
from twin.sim.demand import generate_orders
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter, EventType, ListSink

CONFIG = "config/network.yaml"
UNLIMITED = 10**9


def poisson_orders(
    rates: pd.Series, regions: tuple[Region, ...], horizon: float, rng: np.random.Generator
) -> pd.DataFrame:
    shares = np.array([r.demand_share for r in regions])
    counts = rng.poisson(np.outer(rates.to_numpy(), shares) * horizon)

    n_parts, n_regions = counts.shape
    flat = counts.ravel()
    part_idx = np.repeat(np.repeat(np.arange(n_parts), n_regions), flat)
    region_idx = np.repeat(np.tile(np.arange(n_regions), n_parts), flat)

    orders = pd.DataFrame(
        {
            "part_id": rates.index.to_numpy()[part_idx],
            "region": np.array([r.name for r in regions])[region_idx],
            "time_days": rng.uniform(0, horizon, size=len(part_idx)),
        }
    )
    orders = orders.sort_values("time_days", kind="mergesort").reset_index(drop=True)
    orders.insert(0, "order_id", [f"ord-{i:08d}" for i in range(len(orders))])
    return orders


def with_unlimited_dc(policy: Policy, part_ids) -> Policy:
    return Policy(
        region_base_stock=policy.region_base_stock,
        dc_reorder_point={p: -1 for p in part_ids},
        dc_order_up_to={p: UNLIMITED for p in part_ids},
    )


def predicted_fill(policy: Policy, rates: pd.Series, regions: tuple[Region, ...]) -> pd.Series:
    fill = {}
    for region in regions:
        levels = np.array([policy.region_base_stock.get((region.name, p), 0) for p in rates.index])
        mean_in_transit = rates.to_numpy() * region.demand_share * region.transit_days
        per_part = stats.poisson.cdf(levels - 1, mean_in_transit)
        fill[region.name] = np.average(per_part, weights=rates.to_numpy())
    return pd.Series(fill)


def simulate_fill(
    config: NetworkConfig,
    part_supplier: dict[str, str],
    policy: Policy,
    orders: pd.DataFrame,
    start: datetime,
    rng: np.random.Generator,
) -> pd.DataFrame:
    sink = ListSink()
    simulate(config, part_supplier, policy, orders, Emitter("validate", start, sink), rng)

    demand = Counter(e.site for e in sink.events if e.event_type == EventType.DEMAND)
    backorders = Counter(e.site for e in sink.events if e.event_type == EventType.BACKORDER)
    result = pd.DataFrame({"orders": pd.Series(demand)})
    backorder_counts = pd.Series(backorders).reindex(result.index, fill_value=0)
    result["fill"] = 1 - backorder_counts / result["orders"]
    return result


def overall_fill(result: pd.DataFrame) -> float:
    return float(np.average(result["fill"], weights=result["orders"]))


if __name__ == "__main__":
    config = load_config(CONFIG)
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")
    policy = policy_from_table(pd.read_parquet("data/processed/policy.parquet"), config.dc_name)

    rates = daily_rates(demand, parts.index)
    supplier = parts["supplier"].to_dict()
    start = demand["month"].min().to_pydatetime()
    unlimited = with_unlimited_dc(policy, parts.index)

    rng = np.random.default_rng(config.seed)
    orders = poisson_orders(rates, config.regions, horizon_days(demand), rng)
    a = simulate_fill(config, supplier, unlimited, orders, start, rng)

    rng = np.random.default_rng(config.seed)
    orders = generate_orders(demand, parts.index, config.regions, rng)
    b = simulate_fill(config, supplier, unlimited, orders, start, rng)

    rng = np.random.default_rng(config.seed)
    orders = generate_orders(demand, parts.index, config.regions, rng)
    c = simulate_fill(config, supplier, policy, orders, start, rng)

    report = pd.DataFrame({"predicted": predicted_fill(policy, rates, config.regions)})
    report["A_sim"] = a["fill"]
    std_err = np.sqrt(report["predicted"] * (1 - report["predicted"]) / a["orders"])
    report["A_z"] = (report["A_sim"] - report["predicted"]) / std_err
    report["B_sim"] = b["fill"]
    report["C_sim"] = c["fill"]
    print(report.round(3).to_string())

    predicted_overall = float(np.average(report["predicted"], weights=a["orders"]))
    print(f"\npredicted overall:             {predicted_overall:.3f}")
    print(f"A  poisson demand, unlimited DC: {overall_fill(a):.3f}")
    print(f"B  real demand, unlimited DC:    {overall_fill(b):.3f}")
    print(f"C  real demand, real DC:         {overall_fill(c):.3f}")
    print(f"\ncost of demand bursts (A - B):  {overall_fill(a) - overall_fill(b):.3f}")
    print(f"cost of DC delays (B - C):      {overall_fill(b) - overall_fill(c):.3f}")
