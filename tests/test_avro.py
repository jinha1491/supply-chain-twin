import dataclasses
import io
import json
from datetime import UTC, datetime

import fastavro

from twin.sim.events import Event, EventType
from twin.stream.kafka_sink import avro_record, load_schema

SCHEMA = fastavro.parse_schema(json.loads(load_schema()))


def round_trip(event: Event) -> dict:
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, SCHEMA, avro_record(event))
    buffer.seek(0)
    return fastavro.schemaless_reader(buffer, SCHEMA)


def test_event_survives_avro_round_trip():
    event = Event(
        "r-1", datetime(2000, 1, 1, 12, tzinfo=UTC), EventType.DEMAND, "p1", "west", 1, "o1"
    )
    record = round_trip(event)
    assert record["event_time"] == event.event_time
    assert record["event_type"] == "demand"
    assert record["ref_id"] == "o1"


def test_missing_ref_id_stays_null():
    event = Event(
        "r-2", datetime(2000, 1, 2, tzinfo=UTC), EventType.INVENTORY_SNAPSHOT, "p1", "dc", 3
    )
    assert round_trip(event)["ref_id"] is None


def test_schema_fields_match_event_dataclass():
    schema_fields = [f["name"] for f in json.loads(load_schema())["fields"]]
    assert schema_fields == [f.name for f in dataclasses.fields(Event)]
