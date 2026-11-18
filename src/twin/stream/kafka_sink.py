from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from twin.sim.events import Event
from twin.stream import settings

SCHEMA_PATH = Path(__file__).resolve().parents[3] / "schemas" / "inventory_event.avsc"


def load_schema() -> str:
    return SCHEMA_PATH.read_text()


def avro_record(event: Event) -> dict:
    return {
        "event_id": event.event_id,
        "event_time": event.event_time,
        "event_type": event.event_type.value,
        "part_id": event.part_id,
        "site": event.site,
        "quantity": event.quantity,
        "ref_id": event.ref_id,
    }


class KafkaSink:
    def __init__(self, bootstrap: str, registry_url: str, topic: str) -> None:
        registry = SchemaRegistryClient({"url": registry_url})
        self._serializer = AvroSerializer(registry, load_schema())
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
        self.count = 0
        self.failed = 0
        self.last_error = None

    @classmethod
    def from_settings(cls) -> "KafkaSink":
        return cls(settings.BOOTSTRAP, settings.REGISTRY_URL, settings.TOPIC)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.last_error = err

    def emit(self, event: Event) -> None:
        value = self._serializer(avro_record(event), self._context)
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
