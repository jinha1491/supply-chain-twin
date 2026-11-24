import numpy as np
import pandas as pd

from tests.test_policy_updates import CONFIG, SETTINGS
from twin.optimize.allocate import allocate, model_fill, segments, upper_hull
from twin.optimize.tradeoff import policy_at


def small_rates():
    rng = np.random.default_rng(1)
    return pd.Series(rng.gamma(0.5, 0.2, 100), index=[f"p{i}" for i in range(100)])


def test_hull_keeps_a_concave_curve():
    assert upper_hull(np.array([0.0, 3.0, 5.0, 6.0])) == [0, 1, 2, 3]


def test_hull_skips_a_low_value_start():
    assert upper_hull(np.array([0.0, 1.0, 4.0, 5.0])) == [0, 2, 3]


def test_segment_values_fall_within_each_item():
    segs = segments(CONFIG, small_rates())
    for _, item in segs.groupby(["site", "part_id"]):
        assert item.sort_values("order")["value"].is_monotonic_decreasing


def test_allocation_stays_within_budget():
    segs = segments(CONFIG, small_rates())
    for budget in [0, 10, 75]:
        assert sum(allocate(segs, budget).values()) <= budget


def test_greedy_beats_uniform_at_the_same_budget():
    rates = small_rates()
    supplier = pd.Series("s", index=rates.index)
    uniform = policy_at(CONFIG, SETTINGS, rates, supplier, 0.95).region_base_stock
    greedy = allocate(segments(CONFIG, rates), sum(uniform.values()))
    assert model_fill(CONFIG, rates, greedy) >= model_fill(CONFIG, rates, uniform) - 1e-12
