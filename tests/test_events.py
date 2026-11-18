import json
from datetime import UTC, datetime

from twin.sim.events import Emitter, EventType, ListSink


def test_emitter_assigns_sequential_ids_and_utc_times():
    sink = ListSink()
    emitter = Emitter("run1", datetime(2000, 1, 1), sink)
    first = emitter.emit(0.5, EventType.DEMAND, "p1", "west", 1, ref_id="ord-1")
    emitter.emit(2.0, EventType.FULFILLED, "p1", "west", 1, ref_id="ord-1")

    assert [e.event_id for e in sink.events] == ["run1-000000001", "run1-000000002"]
    assert first.event_time == datetime(2000, 1, 1, 12, tzinfo=UTC)


def test_to_dict_is_json_serializable():
    sink = ListSink()
    event = Emitter("run1", datetime(2000, 1, 1), sink).emit(0.5, EventType.DEMAND, "p1", "west", 1)
    record = event.to_dict()

    assert record["event_type"] == "demand"
    assert record["event_time"] == "2000-01-01T12:00:00+00:00"
    json.dumps(record)
