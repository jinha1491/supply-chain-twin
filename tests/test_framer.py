import io
import json
from datetime import UTC, datetime

import fastavro

from twin.sim.events import Event, EventType
from twin.stream.kafka_sink import ConfluentFramer, avro_record, load_schema

EVENT = Event(
    "r-1",
    datetime(2000, 1, 1, 12, tzinfo=UTC),
    EventType.SHIPMENT_DISPATCHED,
    "p1",
    "dc",
    1,
    "rr-1",
    "westfreight",
)


def test_framer_writes_confluent_wire_format():
    data = ConfluentFramer(load_schema(), schema_id=42)(avro_record(EVENT))

    assert data[0] == 0
    assert int.from_bytes(data[1:5], "big") == 42

    schema = fastavro.parse_schema(json.loads(load_schema()))
    record = fastavro.schemaless_reader(io.BytesIO(data[5:]), schema)
    assert record["event_time"] == EVENT.event_time
    assert record["carrier_id"] == "westfreight"
