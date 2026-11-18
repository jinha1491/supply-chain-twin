import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from scipy import stats

from twin.network import NetworkConfig, load_config
from twin.sim.demand import generate_orders
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter, ListSink

CONFIG = "config/network.yaml"
DAYS_PER_MONTH = 365.25 / 12


@dataclass(frozen=True)
class PolicySettings:
    region_fill_target: float
    dc_service_target: float
    dc_order_cover_days: float
    capacity_headroom: float


def load_policy_settings(path: str | Path) -> PolicySettings:
    settings = PolicySettings(**yaml.safe_load(Path(path).read_text())["policy"])
    for name in ("region_fill_target", "dc_service_target"):
        if not 0 < getattr(settings, name) < 1:
            raise ValueError(f"{name} must be between 0 and 1")
    if settings.dc_order_cover_days <= 0 or settings.capacity_headroom < 1:
        raise ValueError("dc_order_cover_days must be positive and capacity_headroom at least 1")
    return settings


def daily_rates(demand: pd.DataFrame, part_ids) -> pd.Series:
    rows = demand[demand["part_id"].isin(part_ids)]
    return rows.groupby("part_id")["demand"].mean() / DAYS_PER_MONTH


def region_base_stock(rate: float, share: float, transit_days: float, target: float) -> int:
    mean_in_transit = rate * share * transit_days
    if mean_in_transit == 0:
        return 0
    return int(stats.poisson.ppf(target, mean_in_transit)) + 1


def lead_time_demand_dist(rate: float, lt_mean: float, lt_std: float):
    mean = rate * lt_mean
    var = mean + (rate * lt_std) ** 2
    if var - mean < 1e-12:
        return stats.poisson(mean)
    return stats.nbinom(mean**2 / (var - mean), mean / var)


def lead_time_demand_quantile(rate: float, lt_mean: float, lt_std: float, q: float) -> int:
    if rate == 0:
        return 0
    return int(lead_time_demand_dist(rate, lt_mean, lt_std).ppf(q))


def build_policy_table(
    config: NetworkConfig,
    settings: PolicySettings,
    rates: pd.Series,
    part_supplier: pd.Series,
) -> pd.DataFrame:
    suppliers = {s.name: s for s in config.suppliers}
    rows = []
    for part, rate in rates.items():
        for region in config.regions:
            level = region_base_stock(
                rate, region.demand_share, region.transit_days, settings.region_fill_target
            )
            rows.append((region.name, part, level - 1, level))

        supplier = suppliers[part_supplier[part]]
        reorder_point = lead_time_demand_quantile(
            rate, supplier.lead_time_mean, supplier.lead_time_std, settings.dc_service_target
        )
        quantity = max(1, math.ceil(rate * settings.dc_order_cover_days))
        rows.append((config.dc_name, part, reorder_point, reorder_point + quantity))

    return pd.DataFrame(rows, columns=["site", "part_id", "reorder_point", "order_up_to"])


def policy_from_table(table: pd.DataFrame, dc_name: str) -> Policy:
    regions = table[table["site"] != dc_name]
    dc = table[table["site"] == dc_name].set_index("part_id")
    return Policy(
        region_base_stock={
            (site, part): int(level)
            for site, part, level in zip(
                regions["site"], regions["part_id"], regions["order_up_to"], strict=True
            )
        },
        dc_reorder_point=dc["reorder_point"].astype(int).to_dict(),
        dc_order_up_to=dc["order_up_to"].astype(int).to_dict(),
    )


if __name__ == "__main__":
    config = load_config(CONFIG)
    settings = load_policy_settings(CONFIG)
    demand = pd.read_parquet(config.demand_file)
    parts = pd.read_parquet("data/processed/parts.parquet")

    table = build_policy_table(
        config, settings, daily_rates(demand, parts.index), parts["supplier"]
    )
    table.to_parquet("data/processed/policy.parquet", index=False)

    stock = table.groupby("site")["order_up_to"].agg(max_units="sum", mean_level="mean")
    stock["suggested_capacity"] = np.ceil(stock["max_units"] * settings.capacity_headroom)
    print(stock.round(2).to_string())

    rng = np.random.default_rng(config.seed)
    orders = generate_orders(demand, parts.index, config.regions, rng)
    sink = ListSink()
    emitter = Emitter("policy-check", demand["month"].min().to_pydatetime(), sink)
    policy = policy_from_table(table, config.dc_name)
    simulate(config, parts["supplier"].to_dict(), policy, orders, emitter, rng)

    events = pd.DataFrame(
        [(e.event_type.value, e.site) for e in sink.events], columns=["type", "site"]
    )
    demand_by_site = events[events["type"] == "demand"].groupby("site").size()
    backorders = events[events["type"] == "backorder"].groupby("site").size()
    backorders = backorders.reindex(demand_by_site.index, fill_value=0)

    print(f"\nrealized fill rate (target {settings.region_fill_target:.2f}):")
    print((1 - backorders / demand_by_site).round(3).to_string())
    print(f"overall: {1 - backorders.sum() / demand_by_site.sum():.3f}")
