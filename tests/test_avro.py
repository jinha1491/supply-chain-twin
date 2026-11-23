import dataclasses
import io
import json
from datetime import UTC, datetime

import fastavro

from twin.sim.events import Event, EventType
from twin.stream.kafka_sink import avro_record, load_schema

SCHEMAS = {v: fastavro.parse_schema(json.loads(load_schema(v))) for v in (1, 2)}
SHIPMENT = Event(
    "r-1",
    datetime(2000, 1, 1, 12, tzinfo=UTC),
    EventType.SHIPMENT_DISPATCHED,
    "p1",
    "dc",
    1,
    "rr-1",
    "westfreight",
)


def write(event: Event, version: int) -> io.BytesIO:
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, SCHEMAS[version], avro_record(event, version))
    buffer.seek(0)
    return buffer


def test_v2_round_trip_keeps_carrier():
    record = fastavro.schemaless_reader(write(SHIPMENT, 2), SCHEMAS[2])
    assert record["carrier_id"] == "westfreight"
    assert record["event_time"] == SHIPMENT.event_time


def test_v1_message_read_with_v2_schema_gets_null_carrier():
    record = fastavro.schemaless_reader(write(SHIPMENT, 1), SCHEMAS[1], SCHEMAS[2])
    assert record["carrier_id"] is None
    assert record["ref_id"] == "rr-1"


def test_v1_record_has_no_carrier_field():
    assert "carrier_id" not in avro_record(SHIPMENT, 1)


def test_missing_ref_id_stays_null():
    event = Event(
        "r-2", datetime(2000, 1, 2, tzinfo=UTC), EventType.INVENTORY_SNAPSHOT, "p1", "dc", 3
    )
    assert fastavro.schemaless_reader(write(event, 2), SCHEMAS[2])["ref_id"] is None


def test_latest_schema_matches_event_dataclass():
    fields = [f["name"] for f in json.loads(load_schema())["fields"]]
    assert fields == [f.name for f in dataclasses.fields(Event)]


def test_v2_only_adds_optional_fields():
    v1 = json.loads(load_schema(1))["fields"]
    v2 = json.loads(load_schema(2))["fields"]
    assert v2[: len(v1)] == v1
    for field in v2[len(v1) :]:
        assert "null" in field["type"] and field["default"] is None
