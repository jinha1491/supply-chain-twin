from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from twin.network import NetworkConfig, Region, Supplier
from twin.policy import (
    PolicySettings,
    build_policy_table,
    lead_time_demand_dist,
    lead_time_demand_quantile,
    load_policy_settings,
    policy_from_table,
    region_base_stock,
)

SETTINGS = PolicySettings(0.95, 0.95, 30, 1.15)
CONFIG = NetworkConfig(
    seed=0,
    demand_file=Path("d"),
    profile_file=Path("p"),
    suppliers=(Supplier("s", 1.0, 20.0, 5.0),),
    dc_name="dc",
    dc_capacity_units=100,
    regions=(Region("r1", 0.6, 2, 10), Region("r2", 0.4, 3, 10)),
)


def test_region_base_stock_matches_poisson():
    # mean in transit = 1: P(<=2) = 0.920, P(<=3) = 0.981, so k = 3 and S = 4
    assert region_base_stock(1.0, 1.0, 1.0, 0.95) == 4


def test_slow_part_gets_one_unit_and_zero_demand_gets_none():
    assert region_base_stock(0.001, 1.0, 1.0, 0.95) == 1
    assert region_base_stock(0.0, 1.0, 1.0, 0.95) == 0


def test_lead_time_demand_has_expected_moments():
    dist = lead_time_demand_dist(rate=0.5, lt_mean=20, lt_std=5)
    assert np.isclose(dist.mean(), 10.0)
    assert np.isclose(dist.var(), 10.0 + 2.5**2)


def test_lead_time_variability_raises_reorder_point():
    steady = lead_time_demand_quantile(0.5, 20, 0, 0.95)
    variable = lead_time_demand_quantile(0.5, 20, 10, 0.95)
    assert steady < variable


def test_build_policy_table_shapes():
    table = build_policy_table(CONFIG, SETTINGS, pd.Series({"p": 0.1}), pd.Series({"p": "s"}))
    regions = table[table["site"] != "dc"]
    dc = table[table["site"] == "dc"].iloc[0]

    assert set(table["site"]) == {"r1", "r2", "dc"}
    assert (regions["reorder_point"] == regions["order_up_to"] - 1).all()
    assert dc["order_up_to"] - dc["reorder_point"] == 3


def test_policy_from_table_round_trip():
    table = build_policy_table(CONFIG, SETTINGS, pd.Series({"p": 0.1}), pd.Series({"p": "s"}))
    policy = policy_from_table(table, "dc")

    assert set(policy.region_base_stock) == {("r1", "p"), ("r2", "p")}
    assert policy.dc_order_up_to["p"] - policy.dc_reorder_point["p"] == 3


def test_settings_reject_impossible_target(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "policy:\n  region_fill_target: 1.0\n  dc_service_target: 0.95\n"
        "  dc_order_cover_days: 30\n  capacity_headroom: 1.15\n"
    )
    with pytest.raises(ValueError, match="between 0 and 1"):
        load_policy_settings(path)


def test_repo_policy_settings_are_valid():
    load_policy_settings("config/network.yaml")
