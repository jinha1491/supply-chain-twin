import argparse
import time

from confluent_kafka.admin import AdminClient, NewTopic

from twin.stream import settings

PARTITIONS = 6


def topic_exists(admin: AdminClient, topic: str) -> bool:
    return topic in admin.list_topics(timeout=10).topics


def create_topic(admin: AdminClient, topic: str) -> None:
    new_topic = NewTopic(
        topic,
        num_partitions=PARTITIONS,
        replication_factor=1,
        config={"retention.ms": "-1"},
    )
    admin.create_topics([new_topic])[topic].result()


def delete_topic(admin: AdminClient, topic: str) -> None:
    admin.delete_topics([topic])[topic].result()
    for _ in range(30):
        if not topic_exists(admin, topic):
            return
        time.sleep(1)
    raise RuntimeError(f"topic {topic} was not deleted")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the event topic.")
    parser.add_argument("--reset", action="store_true", help="delete and recreate the topic")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": settings.BOOTSTRAP})
    if args.reset and topic_exists(admin, settings.TOPIC):
        delete_topic(admin, settings.TOPIC)
        print(f"deleted {settings.TOPIC}")
    if topic_exists(admin, settings.TOPIC):
        print(f"{settings.TOPIC} already exists")
    else:
        create_topic(admin, settings.TOPIC)
        print(f"created {settings.TOPIC} with {PARTITIONS} partitions")


if __name__ == "__main__":
    main()


def message_count(topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": settings.BOOTSTRAP,
            "group.id": "twin-count",
            "enable.auto.commit": False,
        }
    )
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    total = 0
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += high - low
    consumer.close()
    return total
