import argparse

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from twin.run import run
from twin.sim.events import Event
from twin.sim.sinks import NullSink
from twin.stream import settings
from twin.stream.kafka_sink import ConfluentFramer, KafkaSink, avro_record, load_schema
from twin.stream.topics import create_topic, delete_topic, message_count, topic_exists


class SerializeOnlySink:
    def __init__(self, serializer, topic: str) -> None:
        self.serializer = serializer
        self.context = SerializationContext(topic, MessageField.VALUE)
        self.count = 0

    def emit(self, event: Event) -> None:
        self.serializer(avro_record(event), self.context)
        self.count += 1

    def __enter__(self) -> "SerializeOnlySink":
        return self

    def __exit__(self, *exc) -> None:
        pass


def fresh_topic(admin: AdminClient, topic: str) -> None:
    if topic_exists(admin, topic):
        delete_topic(admin, topic)
    create_topic(admin, topic)


def producer_bench(snapshot_days: float, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": settings.BOOTSTRAP})
    registry = SchemaRegistryClient({"url": settings.REGISTRY_URL})
    subject = f"{topic}-value"
    bootstrap, registry_url = settings.BOOTSTRAP, settings.REGISTRY_URL

    cases = [
        ("simulation only", lambda: NullSink()),
        (
            "+ confluent serializer",
            lambda: SerializeOnlySink(AvroSerializer(registry, load_schema()), topic),
        ),
        (
            "+ fast serializer",
            lambda: SerializeOnlySink(
                ConfluentFramer.registered(registry, subject, load_schema()), topic
            ),
        ),
        (
            "+ kafka, confluent",
            lambda: KafkaSink(bootstrap, registry_url, topic, serializer="confluent"),
        ),
        ("+ kafka, fast", lambda: KafkaSink(bootstrap, registry_url, topic, serializer="fast")),
    ]
    for label, make_sink in cases:
        if "kafka" in label:
            fresh_topic(admin, topic)
        sink = make_sink()
        elapsed = run("bench", snapshot_days, sink)
        rate = sink.count / elapsed
        print(f"{label:<24} {sink.count:>10,} events  {elapsed:6.1f}s  {rate:>9,.0f}/s")

    print(f"messages in {topic}: {message_count(topic):,} (last run sent {sink.count:,})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmarks.")
    commands = parser.add_subparsers(dest="command", required=True)
    producer = commands.add_parser("producer", help="producer throughput by layer")
    producer.add_argument("--snapshot-every-days", type=float, default=7.0)
    producer.add_argument("--topic", default="twin.bench")
    args = parser.parse_args()

    if args.command == "producer":
        producer_bench(args.snapshot_every_days, args.topic)


if __name__ == "__main__":
    main()
