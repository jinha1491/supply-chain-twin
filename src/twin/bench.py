import argparse
import shutil
import subprocess
import time
from pathlib import Path

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
    ingest = commands.add_parser("ingest", help="bulk Kafka to Iceberg throughput")
    ingest.add_argument("--topic", default="twin.bench")
    latency = commands.add_parser("latency", help="end-to-end latency at fixed rates")
    latency.add_argument("--topic", default="twin.bench")
    latency.add_argument("--rates", default="10000,30000,60000")
    latency.add_argument("--seconds", type=int, default=60)
    args = parser.parse_args()

    if args.command == "producer":
        producer_bench(args.snapshot_every_days, args.topic)
    elif args.command == "ingest":
        ingest_bench(args.topic)
    elif args.command == "latency":
        latency_bench(args.topic, [int(r) for r in args.rates.split(",")], args.seconds)


LOGS = Path("logs")


class PacedSink:
    def __init__(self, inner, rate: float, max_events: int) -> None:
        self.inner = inner
        self.rate = rate
        self.max_events = max_events
        self.count = 0
        self.started = None
        self.last_sent = None

    def emit(self, event: Event) -> None:
        if self.count >= self.max_events:
            return
        if self.started is None:
            self.started = time.perf_counter()
        self.inner.emit(event)
        self.count += 1
        if self.count % 500 == 0:
            ahead = self.count / self.rate - (time.perf_counter() - self.started)
            if ahead > 0:
                time.sleep(ahead)
        self.last_sent = time.perf_counter()

    def achieved_rate(self) -> float:
        return self.count / (self.last_sent - self.started)

    def __enter__(self) -> "PacedSink":
        self.inner.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self.inner.__exit__(*exc)


def spark_job(topic: str, args: list[str], log, background: bool = False):
    command = ["docker", "compose", "run", "--rm", "-e", f"TWIN_TOPIC={topic}", "spark", *args]
    if background:
        return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
    return subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)


def wait_for_line(path: Path, text: str, timeout: float = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and text in path.read_text():
            return
        time.sleep(1)
    raise TimeoutError(f"{text!r} did not appear in {path}")


def show_lines(path: Path, prefix: str) -> None:
    for line in path.read_text().splitlines():
        if line.startswith(prefix):
            print(line)


def ingest_bench(topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": settings.BOOTSTRAP})
    fresh_topic(admin, topic)
    sink = KafkaSink(settings.BOOTSTRAP, settings.REGISTRY_URL, topic, serializer="fast")
    elapsed = run("bulk", 1.0, sink)
    print(f"produced {sink.count:,} events in {elapsed:.1f}s ({sink.count / elapsed:,.0f}/s)")
    print(f"messages in {topic}: {message_count(topic):,}")

    LOGS.mkdir(exist_ok=True)
    shutil.rmtree("checkpoints/bench_ingest", ignore_errors=True)
    log_path = LOGS / "bench_ingest.log"
    job = [
        "/app/spark/jobs/ingest_raw.py",
        "--once",
        "--fresh",
        "--table",
        "lake.bench.events",
        "--checkpoint",
        "/checkpoints/bench_ingest",
    ]
    with open(log_path, "w") as log:
        spark_job(topic, job, log)
    show_lines(log_path, "ingest:")


def latency_bench(topic: str, rates: list[int], seconds: int) -> None:
    admin = AdminClient({"bootstrap.servers": settings.BOOTSTRAP})
    LOGS.mkdir(exist_ok=True)
    tables = []
    for rate in rates:
        fresh_topic(admin, topic)
        table = f"lake.bench.latency_{rate}"
        tables.append(table)
        shutil.rmtree(f"checkpoints/bench_latency_{rate}", ignore_errors=True)
        log_path = LOGS / f"bench_latency_{rate}.log"
        job = [
            "/app/spark/jobs/ingest_raw.py",
            "--fresh",
            "--table",
            table,
            "--checkpoint",
            f"/checkpoints/bench_latency_{rate}",
            "--trigger-seconds",
            "2",
            "--run-seconds",
            str(seconds + 60),
        ]
        with open(log_path, "w") as log:
            proc = spark_job(topic, job, log, background=True)
            wait_for_line(log_path, "ingest: query started")
            inner = KafkaSink(settings.BOOTSTRAP, settings.REGISTRY_URL, topic, serializer="fast")
            sink = PacedSink(inner, rate, rate * seconds)
            run(f"r{rate}", 7.0, sink)
            print(f"rate {rate:,}/s: sent {sink.count:,} at {sink.achieved_rate():,.0f}/s")
            proc.wait()
        show_lines(log_path, "ingest:")

    log_path = LOGS / "bench_latency_report.log"
    with open(log_path, "w") as log:
        spark_job(topic, ["/app/spark/jobs/latency_report.py", "--tables", ",".join(tables)], log)
    show_lines(log_path, "lake.bench.")


if __name__ == "__main__":
    main()
