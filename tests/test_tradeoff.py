import numpy as np
import pandas as pd

from tests.test_policy_updates import CONFIG, SETTINGS
from twin.optimize.tradeoff import interval, policy_at


def test_interval_of_identical_values_is_exact():
    assert interval([0.9, 0.9, 0.9]) == (0.9, 0.0)


def test_interval_widens_with_spread():
    _, narrow = interval([1.0, 1.1, 0.9, 1.0, 1.0])
    _, wide = interval([1.0, 2.0, 0.0, 1.5, 0.5])
    assert wide > narrow > 0


def test_higher_target_never_lowers_a_level():
    rates = pd.Series(
        np.random.default_rng(1).gamma(0.5, 0.2, 100), index=[f"p{i}" for i in range(100)]
    )
    supplier = pd.Series("s", index=rates.index)
    low = policy_at(CONFIG, SETTINGS, rates, supplier, 0.80)
    high = policy_at(CONFIG, SETTINGS, rates, supplier, 0.99)
    assert all(high.region_base_stock[k] >= v for k, v in low.region_base_stock.items())
