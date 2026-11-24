from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from twin.optimize.service import ServiceTracker
from twin.sim.events import EventType

START = datetime(2020, 1, 1, tzinfo=UTC)


def event(day, kind, ref, site="west"):
    return SimpleNamespace(
        event_time=START + timedelta(days=day), event_type=kind, ref_id=ref, site=site
    )


def test_wait_runs_from_backorder_to_fulfilment():
    tracker = ServiceTracker(START, "dc")
    for e in [
        event(10, EventType.DEMAND, "o1"),
        event(10, EventType.BACKORDER, "o1"),
        event(12.5, EventType.FULFILLED, "o1"),
        event(11, EventType.DEMAND, "o2"),
        event(11, EventType.FULFILLED, "o2"),
    ]:
        tracker.emit(e)
    summary = tracker.summary()
    assert tracker.waits == [2.5]
    assert summary["fill_rate"] == 0.5
    assert summary["served_in_3d"] == 1.0


def test_dc_delay_ignores_supplier_shipments():
    tracker = ServiceTracker(START, "dc")
    for e in [
        event(5, EventType.REPLENISHMENT_ORDER, "rr1"),
        event(9, EventType.SHIPMENT_DISPATCHED, "rr1", site="dc"),
        event(6, EventType.SHIPMENT_DISPATCHED, "po1", site="asia"),
    ]:
        tracker.emit(e)
    assert tracker.dc_delays == [4.0]


def test_events_before_the_window_are_skipped():
    tracker = ServiceTracker(START + timedelta(days=100), "dc")
    tracker.emit(event(50, EventType.DEMAND, "o1"))
    tracker.emit(event(50, EventType.BACKORDER, "o1"))
    assert tracker.demand == 0 and tracker.backorders == 0
