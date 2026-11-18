from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from twin.network import NetworkConfig, Region, Supplier
from twin.sim.engine import Policy
from twin.validate import poisson_orders, predicted_fill, simulate_fill, with_unlimited_dc

START = datetime(2000, 1, 1, tzinfo=UTC)
REGIONS = (Region("r", 1.0, 2.0, 100),)
CONFIG = NetworkConfig(
    seed=0,
    demand_file=Path("d"),
    profile_file=Path("p"),
    suppliers=(Supplier("s", 1.0, 10.0, 0.0),),
    dc_name="dc",
    dc_capacity_units=100,
    regions=REGIONS,
)


def test_poisson_orders_have_expected_volume():
    orders = poisson_orders(pd.Series({"p": 0.5}), REGIONS, 10_000, np.random.default_rng(0))
    assert abs(len(orders) - 5_000) < 300
    assert orders["time_days"].is_monotonic_increasing
    assert orders["time_days"].between(0, 10_000).all()


def test_predicted_fill_matches_hand_calculation():
    policy = Policy({("r", "p"): 2}, {}, {})
    # mean in transit = 0.5 * 2 = 1, fill = P(N <= 1) = 2 / e
    assert np.isclose(predicted_fill(policy, pd.Series({"p": 0.5}), REGIONS)["r"], 2 / np.e)


def test_unlimited_dc_never_reorders():
    policy = with_unlimited_dc(Policy({("r", "p"): 1}, {"p": 0}, {"p": 1}), ["p"])
    assert policy.dc_reorder_point == {"p": -1}
    assert policy.region_base_stock == {("r", "p"): 1}


def test_simulation_matches_palm_theorem():
    rng = np.random.default_rng(0)
    orders = poisson_orders(pd.Series({"p": 0.5}), REGIONS, 40_000, rng)
    policy = with_unlimited_dc(Policy({("r", "p"): 2}, {"p": 0}, {"p": 1}), ["p"])
    result = simulate_fill(CONFIG, {"p": "s"}, policy, orders, START, rng)

    expected = stats.poisson.cdf(1, 1.0)
    assert abs(result.loc["r", "fill"] - expected) < 0.015
