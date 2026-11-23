from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


@dataclass(frozen=True)
class Supplier:
    name: str
    share_of_parts: float
    lead_time_mean: float
    lead_time_std: float
    carrier: str | None = None


@dataclass(frozen=True)
class Region:
    name: str
    demand_share: float
    transit_days: float
    capacity_units: int
    carrier: str | None = None


@dataclass(frozen=True)
class NetworkConfig:
    seed: int
    demand_file: Path
    profile_file: Path
    suppliers: tuple[Supplier, ...]
    dc_name: str
    dc_capacity_units: int
    regions: tuple[Region, ...]


def load_config(path: str | Path) -> NetworkConfig:
    raw = yaml.safe_load(Path(path).read_text())
    suppliers = tuple(
        Supplier(
            name=s["name"],
            share_of_parts=s["share_of_parts"],
            lead_time_mean=s["lead_time_days"]["mean"],
            lead_time_std=s["lead_time_days"]["std"],
            carrier=s.get("carrier"),
        )
        for s in raw["suppliers"]
    )
    config = NetworkConfig(
        seed=raw["seed"],
        demand_file=Path(raw["data"]["demand_file"]),
        profile_file=Path(raw["data"]["profile_file"]),
        suppliers=suppliers,
        dc_name=raw["dc"]["name"],
        dc_capacity_units=raw["dc"]["capacity_units"],
        regions=tuple(Region(**r) for r in raw["regions"]),
    )
    validate(config)
    return config


def _check_shares(label: str, shares: list[float]) -> None:
    if any(s <= 0 for s in shares) or not np.isclose(sum(shares), 1.0):
        raise ValueError(f"{label} must be positive and sum to 1, got {shares}")


def validate(config: NetworkConfig) -> None:
    _check_shares("supplier share_of_parts", [s.share_of_parts for s in config.suppliers])
    _check_shares("region demand_share", [r.demand_share for r in config.regions])

    names = [s.name for s in config.suppliers] + [config.dc_name]
    names += [r.name for r in config.regions]
    if len(names) != len(set(names)):
        raise ValueError(f"node names must be unique, got {names}")

    for s in config.suppliers:
        if s.lead_time_mean <= 0 or s.lead_time_std < 0:
            raise ValueError(f"supplier {s.name} has an invalid lead time")
    for r in config.regions:
        if r.transit_days <= 0 or r.capacity_units <= 0:
            raise ValueError(f"region {r.name} needs positive transit_days and capacity_units")
    if config.dc_capacity_units <= 0:
        raise ValueError("dc capacity_units must be positive")


def complete_parts(demand: pd.DataFrame) -> pd.Index:
    final_month = demand["month"].max()
    last_seen = demand.dropna(subset=["demand"]).groupby("part_id")["month"].max()
    return last_seen.index[last_seen == final_month]


def assign_suppliers(part_ids, suppliers: tuple[Supplier, ...], seed: int) -> pd.Series:
    ids = sorted(part_ids)
    rng = np.random.default_rng(seed)
    choice = rng.choice(
        [s.name for s in suppliers],
        size=len(ids),
        p=[s.share_of_parts for s in suppliers],
    )
    return pd.Series(choice, index=pd.Index(ids, name="part_id"), name="supplier")


if __name__ == "__main__":
    config = load_config("config/network.yaml")
    demand = pd.read_parquet(config.demand_file)
    profile = pd.read_parquet(config.profile_file)

    keep = complete_parts(demand)
    parts = profile.loc[keep].join(assign_suppliers(keep, config.suppliers, config.seed))

    out = Path("data/processed/parts.parquet")
    parts.to_parquet(out)
    print(f"kept {len(keep)} of {demand['part_id'].nunique()} parts")
    print(parts.groupby(["supplier", "pattern"]).size().unstack(fill_value=0))
