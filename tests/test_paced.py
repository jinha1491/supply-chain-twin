import time
from datetime import datetime

from twin.bench import PacedSink
from twin.sim.events import Emitter, EventType, ListSink


def emit_many(sink, n):
    emitter = Emitter("t", datetime(2000, 1, 1), sink)
    for i in range(n):
        emitter.emit(i * 0.001, EventType.DEMAND, "p", "west", 1)


def test_paced_sink_stops_at_max_events():
    inner = ListSink()
    sink = PacedSink(inner, rate=1_000_000, max_events=1_200)
    emit_many(sink, 5_000)
    assert len(inner.events) == 1_200 and sink.count == 1_200


def test_paced_sink_holds_the_rate():
    sink = PacedSink(ListSink(), rate=5_000, max_events=2_000)
    started = time.perf_counter()
    emit_many(sink, 2_000)
    assert time.perf_counter() - started >= 0.35
    assert sink.achieved_rate() < 5_500
