import time

import psycopg
import requests
from confluent_kafka.admin import AdminClient

from twin.masterdata import DSN, load
from twin.stream import settings
from twin.stream.connector import CONFIG, CONNECT, NAME


def remove_connector() -> None:
    base = f"{CONNECT}/connectors/{NAME}"
    if not requests.get(base, timeout=10).ok:
        return
    requests.put(f"{base}/stop", timeout=10).raise_for_status()
    for _ in range(30):
        state = requests.get(f"{base}/status", timeout=10).json()["connector"]["state"]
        if state == "STOPPED":
            break
        time.sleep(1)
    requests.delete(f"{base}/offsets", timeout=30).raise_for_status()
    requests.delete(base, timeout=10).raise_for_status()
    print(f"removed connector {NAME} and its saved offsets")


def drop_slot_and_publication() -> None:
    slot = CONFIG["slot.name"]
    with psycopg.connect(DSN, autocommit=True) as conn:
        for _ in range(30):
            row = conn.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,)
            ).fetchone()
            if row is None or not row[0]:
                break
            time.sleep(1)
        if row is not None:
            conn.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
        conn.execute("DROP PUBLICATION IF EXISTS dbz_publication")
    print(f"dropped replication slot {slot} and publication")


def delete_md_topics() -> None:
    admin = AdminClient({"bootstrap.servers": settings.BOOTSTRAP})
    topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith("md.")]
    if topics:
        for future in admin.delete_topics(topics).values():
            future.result()
    for _ in range(30):
        if not any(t.startswith("md.") for t in admin.list_topics(timeout=10).topics):
            break
        time.sleep(1)
    print(f"deleted topics {sorted(topics)}")


def main() -> None:
    remove_connector()
    drop_slot_and_publication()
    delete_md_topics()
    load(reset=True)


if __name__ == "__main__":
    main()
