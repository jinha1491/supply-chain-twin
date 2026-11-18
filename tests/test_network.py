import numpy as np
import pandas as pd
import pytest

from twin.network import assign_suppliers, complete_parts, load_config

VALID = """
seed: 1
data:
  demand_file: d.parquet
  profile_file: p.parquet
suppliers:
  - {name: a, share_of_parts: 0.5, lead_time_days: {mean: 5, std: 1}}
  - {name: b, share_of_parts: 0.5, lead_time_days: {mean: 30, std: 10}}
dc: {name: dc, capacity_units: 100}
regions:
  - {name: r1, demand_share: 0.6, transit_days: 2, capacity_units: 10}
  - {name: r2, demand_share: 0.4, transit_days: 3, capacity_units: 10}
"""


def write(tmp_path, text):
    path = tmp_path / "network.yaml"
    path.write_text(text)
    return path


def test_valid_config_loads(tmp_path):
    config = load_config(write(tmp_path, VALID))
    assert [s.name for s in config.suppliers] == ["a", "b"]
    assert config.regions[1].transit_days == 3


def test_repo_config_is_valid():
    load_config("config/network.yaml")


def test_shares_must_sum_to_one(tmp_path):
    bad = VALID.replace("demand_share: 0.4", "demand_share: 0.3")
    with pytest.raises(ValueError, match="sum to 1"):
        load_config(write(tmp_path, bad))


def test_names_must_be_unique(tmp_path):
    bad = VALID.replace("name: r2", "name: r1")
    with pytest.raises(ValueError, match="unique"):
        load_config(write(tmp_path, bad))


def test_complete_parts_drops_parts_that_stop_early():
    months = pd.date_range("2000-01-01", periods=3, freq="MS")
    demand = pd.DataFrame(
        {
            "part_id": ["full"] * 3 + ["stops"] * 3,
            "month": list(months) * 2,
            "demand": [1, 0, 2, 1, np.nan, np.nan],
        }
    )
    assert list(complete_parts(demand)) == ["full"]


def test_assign_suppliers_is_reproducible_and_valid(tmp_path):
    config = load_config(write(tmp_path, VALID))
    ids = [f"p{i}" for i in range(1000)]
    first = assign_suppliers(ids, config.suppliers, seed=7)
    second = assign_suppliers(reversed(ids), config.suppliers, seed=7)
    assert first.equals(second)
    assert set(first) <= {"a", "b"}
    assert 0.4 < (first == "a").mean() < 0.6
