import numpy as np
import pandas as pd
import pytest

from twin.network import Region
from twin.sim.demand import generate_orders

REGIONS = (Region("r1", 0.7, 2, 10), Region("r2", 0.3, 3, 10))


def make_demand():
    months = pd.date_range("2000-01-01", periods=3, freq="MS")
    return pd.DataFrame(
        {
            "part_id": ["a"] * 3 + ["b"] * 3 + ["x"] * 3,
            "month": list(months) * 3,
            "demand": [2, 0, 5, 1, np.nan, 3, 4, 4, 4],
        }
    )


def orders_for(demand, part_ids, seed=0):
    return generate_orders(demand, part_ids, REGIONS, np.random.default_rng(seed))


def test_order_totals_match_demand():
    orders = orders_for(make_demand(), ["a", "b"])
    assert orders["part_id"].value_counts().to_dict() == {"a": 7, "b": 4}


def test_excluded_parts_get_no_orders():
    orders = orders_for(make_demand(), ["a", "b"])
    assert "x" not in set(orders["part_id"])


def test_orders_fall_inside_their_month():
    orders = orders_for(make_demand(), ["a", "b"])
    jan = orders["time_days"] < 31
    mar = (orders["time_days"] >= 60) & (orders["time_days"] < 91)

    assert (jan | mar).all()
    assert ((orders["part_id"] == "a") & jan).sum() == 2
    assert ((orders["part_id"] == "a") & mar).sum() == 5
    assert ((orders["part_id"] == "b") & mar).sum() == 3


def test_region_split_follows_shares():
    demand = pd.DataFrame(
        {"part_id": ["big"], "month": [pd.Timestamp("2000-01-01")], "demand": [10000]}
    )
    orders = orders_for(demand, ["big"])
    assert abs((orders["region"] == "r1").mean() - 0.7) < 0.02


def test_same_seed_gives_same_orders():
    demand = make_demand()
    assert orders_for(demand, ["a", "b"], seed=3).equals(orders_for(demand, ["a", "b"], seed=3))


def test_orders_are_sorted_with_unique_ids():
    orders = orders_for(make_demand(), ["a", "b"])
    assert orders["time_days"].is_monotonic_increasing
    assert orders["order_id"].is_unique


def test_fractional_demand_raises():
    demand = make_demand()
    demand.loc[0, "demand"] = 1.5
    with pytest.raises(ValueError, match="whole units"):
        orders_for(demand, ["a", "b"])
