import numpy as np
import pytest

from twin.forecast import methods


def rows(*series):
    return np.array(series, dtype=float)


def test_croston_on_steady_demand_returns_the_demand():
    assert methods.croston(rows([2, 2, 2, 2]))[0] == pytest.approx(2.0)


def test_croston_splits_size_and_interval():
    history = rows([0, 3, 0, 3, 0, 3])
    assert methods.croston(history)[0] == pytest.approx(1.5)
    assert methods.croston(history, variant="sba")[0] == pytest.approx(1.5 * 0.95)


def test_croston_ignores_quiet_months_but_tsb_decays():
    history = rows([0, 4, 0, 0, 0, 0, 0, 0, 0, 0])
    assert methods.croston(history)[0] == pytest.approx(2.0)
    assert methods.tsb(history)[0] < 2.0


def test_no_demand_forecasts_zero():
    history = rows([0, 0, 0, 0])
    assert methods.croston(history)[0] == 0
    assert methods.tsb(history)[0] == 0


def test_methods_handle_many_parts_like_one():
    history = rows([0, 3, 0, 3], [1, 1, 1, 1], [0, 0, 5, 0])
    together = methods.croston(history)
    separately = [methods.croston(history[i : i + 1])[0] for i in range(3)]
    assert together == pytest.approx(separately)


def test_simple_baselines():
    history = rows([1, 2, 3, 4])
    assert methods.naive(history)[0] == 4
    assert methods.moving_average(history, window=2)[0] == pytest.approx(3.5)
    assert methods.ses(rows([5, 5, 5]))[0] == pytest.approx(5.0)
