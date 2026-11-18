from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from twin.stream import settings


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.BOOTSTRAP,
            "group.id": "twin-check",
            "enable.auto.commit": False,
        }
    )
    partitions = consumer.list_topics(settings.TOPIC, timeout=10).topics[settings.TOPIC].partitions

    total = 0
    for p in sorted(partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(settings.TOPIC, p), timeout=10)
        print(f"partition {p}: {high - low:,} messages")
        total += high - low
    print(f"total: {total:,}")

    deserializer = AvroDeserializer(SchemaRegistryClient({"url": settings.REGISTRY_URL}))
    context = SerializationContext(settings.TOPIC, MessageField.VALUE)
    consumer.assign([TopicPartition(settings.TOPIC, 0, 0)])
    print("\nfirst records in partition 0:")
    shown = 0
    while shown < 3:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            raise RuntimeError(msg.error())
        print(f"  key={msg.key().decode()} {deserializer(msg.value(), context)}")
        shown += 1
    consumer.close()


if __name__ == "__main__":
    main()
