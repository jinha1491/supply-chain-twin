import io
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import fastavro
from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from twin.sim.events import Event
from twin.stream import settings

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas"
SCHEMA_FILES = {1: "inventory_event.avsc", 2: "inventory_event_v2.avsc"}
LATEST = max(SCHEMA_FILES)


def load_schema(version: int = LATEST) -> str:
    return (SCHEMA_DIR / SCHEMA_FILES[version]).read_text()


def avro_record(event: Event, version: int = LATEST) -> dict:
    record = {
        "event_id": event.event_id,
        "event_time": event.event_time,
        "event_type": event.event_type.value,
        "part_id": event.part_id,
        "site": event.site,
        "quantity": event.quantity,
        "ref_id": event.ref_id,
    }
    if version >= 2:
        record["carrier_id"] = event.carrier_id
    return record


class ConfluentFramer:
    def __init__(self, schema_str: str, schema_id: int) -> None:
        self.parsed = fastavro.parse_schema(json.loads(schema_str))
        self.header = b"\x00" + schema_id.to_bytes(4, "big")

    @classmethod
    def registered(cls, registry: SchemaRegistryClient, subject: str, schema_str: str):
        return cls(schema_str, registry.register_schema(subject, Schema(schema_str, "AVRO")))

    def __call__(self, record: dict, ctx=None) -> bytes:
        buffer = io.BytesIO()
        buffer.write(self.header)
        fastavro.schemaless_writer(buffer, self.parsed, record)
        return buffer.getvalue()


class KafkaSink:
    def __init__(
        self,
        bootstrap: str,
        registry_url: str,
        topic: str,
        schema_switch: datetime | None = None,
        serializer: str = "confluent",
    ) -> None:
        registry = SchemaRegistryClient({"url": registry_url})
        if serializer == "fast":
            subject = f"{topic}-value"
            self._serializers = {
                v: ConfluentFramer.registered(registry, subject, load_schema(v))
                for v in SCHEMA_FILES
            }
        else:
            self._serializers = {v: AvroSerializer(registry, load_schema(v)) for v in SCHEMA_FILES}
        self._context = SerializationContext(topic, MessageField.VALUE)
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 500_000,
            }
        )
        self.topic = topic
        self.schema_switch = schema_switch
        self.by_version: Counter = Counter()
        self.count = 0
        self.failed = 0
        self.last_error = None

    @classmethod
    def from_settings(
        cls, schema_switch: datetime | None = None, serializer: str = "confluent"
    ) -> "KafkaSink":
        return cls(
            settings.BOOTSTRAP, settings.REGISTRY_URL, settings.TOPIC, schema_switch, serializer
        )

    def version_for(self, event: Event) -> int:
        if self.schema_switch is None or event.event_time >= self.schema_switch:
            return LATEST
        return 1

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.last_error = err

    def emit(self, event: Event) -> None:
        version = self.version_for(event)
        value = self._serializers[version](avro_record(event, version), self._context)
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=event.part_id.encode(),
                    value=value,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self._producer.poll(0.5)
        self.by_version[version] += 1
        self.count += 1
        if self.count % 10_000 == 0:
            self._producer.poll(0)

    def close(self) -> None:
        remaining = self._producer.flush(120)
        if remaining or self.failed:
            raise RuntimeError(
                f"{remaining} messages undelivered, {self.failed} failed: {self.last_error}"
            )

    def __enter__(self) -> "KafkaSink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
