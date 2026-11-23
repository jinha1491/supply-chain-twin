from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from twin.network import NetworkConfig, Region, Supplier
from twin.sim.engine import Policy, simulate
from twin.sim.events import Emitter, EventType, ListSink

START = datetime(2000, 1, 1, tzinfo=UTC)


def make_config(lead_mean=10.0, lead_std=0.0, transit=2.0):
    return NetworkConfig(
        seed=0,
        demand_file=Path("d"),
        profile_file=Path("p"),
        suppliers=(Supplier("s", 1.0, lead_mean, lead_std),),
        dc_name="dc",
        dc_capacity_units=100,
        regions=(Region("r", 1.0, transit, 100),),
    )


def run(rows, policy, parts=("p",), lead_std=0.0, until=None, snapshot=None):
    orders = pd.DataFrame(rows, columns=["order_id", "part_id", "region", "time_days"])
    sink = ListSink()
    sim = simulate(
        make_config(lead_std=lead_std),
        {p: "s" for p in parts},
        policy,
        orders,
        Emitter("t", START, sink),
        np.random.default_rng(0),
        until=until,
        snapshot_every_days=snapshot,
    )
    return sim, sink.events


def fulfilled_times(events):
    return {e.ref_id: e.event_time for e in events if e.event_type == EventType.FULFILLED}


def test_order_from_stock_and_stock_returns_to_base():
    policy = Policy({("r", "p"): 1}, {"p": 0}, {"p": 1})
    sim, events = run([("o1", "p", "r", 0.0)], policy)

    assert [e.event_type for e in events] == [
        EventType.DEMAND,
        EventType.FULFILLED,
        EventType.REPLENISHMENT_ORDER,
        EventType.SHIPMENT_DISPATCHED,
        EventType.REPLENISHMENT_ORDER,
        EventType.SHIPMENT_DISPATCHED,
        EventType.RECEIPT,
        EventType.RECEIPT,
    ]
    assert sim.on_hand[("r", "p")] == 1
    assert sim.on_hand[("dc", "p")] == 1


def test_backorder_is_filled_when_stock_arrives():
    policy = Policy({("r", "p"): 0}, {"p": 0}, {"p": 1})
    _, events = run([("o1", "p", "r", 0.0)], policy)

    assert fulfilled_times(events) == {"o1": START + timedelta(days=2)}


def test_dc_stockout_queues_requests_until_supplier_receipt():
    policy = Policy({("r", "p"): 0}, {"p": 0}, {"p": 1})
    sim, events = run([("o1", "p", "r", 0.0), ("o2", "p", "r", 0.5)], policy)

    assert fulfilled_times(events) == {
        "o1": START + timedelta(days=2),
        "o2": START + timedelta(days=12),
    }
    dc_orders = [
        e for e in events if e.event_type == EventType.REPLENISHMENT_ORDER and e.site == "dc"
    ]
    assert len(dc_orders) == 2
    assert sim.on_hand[("dc", "p")] == 1


def test_random_run_conserves_units():
    rng = np.random.default_rng(1)
    times = np.sort(rng.uniform(0, 365, size=200))
    rows = [(f"o{i}", rng.choice(["p", "q"]), "r", t) for i, t in enumerate(times)]
    policy = Policy({("r", "p"): 2, ("r", "q"): 2}, {"p": 1, "q": 1}, {"p": 4, "q": 4})
    sim, events = run(rows, policy, parts=("p", "q"), lead_std=3.0)

    types = [e.event_type for e in events]
    assert types.count(EventType.DEMAND) == types.count(EventType.FULFILLED) == 200
    for part in ("p", "q"):
        assert sim.on_hand[("r", part)] == 2
        assert 1 < sim.on_hand[("dc", part)] <= 4
        assert not sim.region_backorders[("r", part)]
        assert not sim.dc_queue[part]


def test_lead_time_matches_supplier_mean():
    policy = Policy({}, {}, {})
    sim, _ = run([], policy, lead_std=3.0)
    supplier = Supplier("s", 1.0, 10.0, 3.0)
    samples = [sim.lead_time(supplier) for _ in range(20000)]

    assert abs(np.mean(samples) - 10.0) < 0.2
    assert min(samples) > 0


def test_snapshots_cover_every_site_and_part():
    policy = Policy({("r", "p"): 1}, {"p": 0}, {"p": 1})
    _, events = run([("o1", "p", "r", 0.0)], policy, until=3.5, snapshot=1.0)

    snapshots = [e for e in events if e.event_type == EventType.INVENTORY_SNAPSHOT]
    assert len(snapshots) == 6
    assert {(e.site, e.event_time.day) for e in snapshots} == {
        ("r", 2),
        ("dc", 2),
        ("r", 3),
        ("dc", 3),
        ("r", 4),
        ("dc", 4),
    }


def test_shipments_carry_lane_carrier():
    config = NetworkConfig(
        seed=0,
        demand_file=Path("d"),
        profile_file=Path("p"),
        suppliers=(Supplier("s", 1.0, 10.0, 0.0, carrier="ocean"),),
        dc_name="dc",
        dc_capacity_units=100,
        regions=(Region("r", 1.0, 2.0, 100, carrier="truck"),),
    )
    orders = pd.DataFrame(
        [("o1", "p", "r", 0.0)], columns=["order_id", "part_id", "region", "time_days"]
    )
    sink = ListSink()
    policy = Policy({("r", "p"): 1}, {"p": 0}, {"p": 1})
    simulate(
        config, {"p": "s"}, policy, orders, Emitter("t", START, sink), np.random.default_rng(0)
    )

    shipments = [e for e in sink.events if e.event_type == EventType.SHIPMENT_DISPATCHED]
    assert {e.site: e.carrier_id for e in shipments} == {"dc": "truck", "s": "ocean"}
    assert all(e.carrier_id is None for e in sink.events if e not in shipments)
