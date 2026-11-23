from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from twin.network import NetworkConfig, Region, Supplier
from twin.scenario import load_scenario
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter, EventType, ListSink

START = datetime(2000, 1, 1, tzinfo=UTC)


def test_repo_scenario_is_valid():
    scenario = load_scenario("config/scenarios/port_congestion.yaml")
    assert [c["day"] for c in scenario["changes"]] == [600, 1300]


def test_unsupported_change_kind_is_rejected(tmp_path):
    path = tmp_path / "s.yaml"
    path.write_text(
        "changes:\n  - {day: 1, kind: site, name: x, lead_time_mean: 1, lead_time_std: 0}\n"
    )
    with pytest.raises(ValueError, match="unsupported"):
        load_scenario(path)


def test_lead_time_change_applies_to_later_orders_only():
    config = NetworkConfig(
        seed=0,
        demand_file=Path("d"),
        profile_file=Path("p"),
        suppliers=(Supplier("s", 1.0, 10.0, 0.0),),
        dc_name="dc",
        dc_capacity_units=100,
        regions=(Region("r", 1.0, 2.0, 100),),
    )
    orders = pd.DataFrame(
        [("o1", "p", "r", 0.0), ("o2", "p", "r", 20.0)],
        columns=["order_id", "part_id", "region", "time_days"],
    )
    change = {
        "day": 5,
        "kind": "supplier",
        "name": "s",
        "lead_time_mean": 30.0,
        "lead_time_std": 0.0,
    }
    applied = []
    sink = ListSink()
    simulate(
        config,
        {"p": "s"},
        Policy({("r", "p"): 0}, {"p": 0}, {"p": 1}),
        orders,
        Emitter("t", START, sink),
        np.random.default_rng(0),
        changes=[change],
        on_change=applied.append,
    )

    dc_receipts = [
        e.event_time for e in sink.events if e.event_type == EventType.RECEIPT and e.site == "dc"
    ]
    assert dc_receipts == [START + timedelta(days=10), START + timedelta(days=50)]
    assert applied == [change]
