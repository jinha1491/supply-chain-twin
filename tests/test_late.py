from datetime import datetime

from twin.sim.events import Emitter, EventType, ListSink
from twin.stream.late import LateSink


def test_delayed_event_arrives_after_later_events():
    inner = ListSink()
    with LateSink(inner, fraction=1.0, min_days=1.0, max_days=1.0) as sink:
        emitter = Emitter("t", datetime(2000, 1, 1), sink)
        emitter.emit(0.0, EventType.DEMAND, "p", "west", 1, "o1")
        emitter.emit(0.5, EventType.RECEIPT, "p", "west", 1, "r1")
        emitter.emit(1.5, EventType.RECEIPT, "p", "west", 1, "r2")

    assert [e.ref_id for e in inner.events] == ["r1", "o1", "r2"]
    assert sink.delayed == 1
    assert sink.count == 3


def test_other_event_types_are_never_delayed():
    inner = ListSink()
    with LateSink(inner, fraction=1.0) as sink:
        emitter = Emitter("t", datetime(2000, 1, 1), sink)
        emitter.emit(0.0, EventType.RECEIPT, "p", "west", 1, "r1")

    assert [e.ref_id for e in inner.events] == ["r1"]
    assert sink.delayed == 0


def test_close_flushes_events_still_held():
    inner = ListSink()
    with LateSink(inner, fraction=1.0, min_days=5.0, max_days=5.0) as sink:
        Emitter("t", datetime(2000, 1, 1), sink).emit(0.0, EventType.DEMAND, "p", "west", 1)

    assert len(inner.events) == 1
