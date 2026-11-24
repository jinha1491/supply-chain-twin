from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from twin.network import NetworkConfig, Region, Supplier
from twin.policy import PolicySettings, build_policy_table, vector_levels
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter, EventType, ListSink

START = datetime(2000, 1, 1, tzinfo=UTC)
CONFIG = NetworkConfig(
    seed=0,
    demand_file=Path("d"),
    profile_file=Path("p"),
    suppliers=(Supplier("s", 1.0, 20.0, 5.0),),
    dc_name="dc",
    dc_capacity_units=100,
    regions=(Region("r1", 0.6, 2, 10), Region("r2", 0.4, 3, 10)),
)
SETTINGS = PolicySettings(0.95, 0.95, 30, 1.15)


def test_vector_levels_match_part_by_part_policy():
    rng = np.random.default_rng(0)
    rates = pd.Series(rng.gamma(0.5, 0.05, size=200), index=[f"p{i}" for i in range(200)])
    rates.iloc[:5] = 0.0
    supplier = pd.Series("s", index=rates.index)

    table = build_policy_table(CONFIG, SETTINGS, rates, supplier).set_index(["site", "part_id"])
    regions, dc_reorder, dc_up_to = vector_levels(CONFIG, SETTINGS, rates, supplier)

    for (site, part), level in regions.items():
        assert level == table.loc[(site, part), "order_up_to"]
    for part in rates.index:
        assert dc_reorder[part] == table.loc[("dc", part), "reorder_point"]
        assert dc_up_to[part] == table.loc[("dc", part), "order_up_to"]


def test_raising_then_lowering_a_region_level():
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
        [("o1", "p", "r", 11.0), ("o2", "p", "r", 12.0)],
        columns=["order_id", "part_id", "region", "time_days"],
    )
    updates = [(1.0, {("r", "p"): 3}, {}, {}), (10.0, {("r", "p"): 1}, {}, {})]
    sink = ListSink()
    sim = simulate(
        config,
        {"p": "s"},
        Policy({("r", "p"): 1}, {"p": -1}, {"p": 100}),
        orders,
        Emitter("t", START, sink),
        np.random.default_rng(0),
        policy_updates=updates,
    )

    region_requests = [
        e for e in sink.events if e.event_type == EventType.REPLENISHMENT_ORDER and e.site == "r"
    ]
    fills = [e for e in sink.events if e.event_type == EventType.FULFILLED]
    assert len(region_requests) == 2
    assert len(fills) == 2
    assert sim.on_hand[("r", "p")] == 1
