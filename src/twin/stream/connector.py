import argparse
import time

import requests

CONNECT = "http://localhost:8083"
NAME = "masterdata"
TABLES = ("suppliers", "sites", "parts", "policy")

CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "twin",
    "database.password": "twin",
    "database.dbname": "masterdata",
    "topic.prefix": "md",
    "plugin.name": "pgoutput",
    "slot.name": "masterdata_slot",
    "publication.autocreate.mode": "filtered",
    "table.include.list": ",".join(f"public.{t}" for t in TABLES),
    "snapshot.mode": "initial",
    "decimal.handling.mode": "double",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,source.ts_ms,source.lsn",
    "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
}


def wait_until_running(timeout: float = 90) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = requests.get(f"{CONNECT}/connectors/{NAME}/status", timeout=10)
        if response.ok:
            status = response.json()
            states = [status["connector"]["state"]] + [t["state"] for t in status["tasks"]]
            if "FAILED" in states:
                raise RuntimeError(status)
            if status["tasks"] and all(s == "RUNNING" for s in states):
                return status
        time.sleep(2)
    raise TimeoutError(f"{NAME} did not reach RUNNING")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the Debezium connector.")
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    if args.delete:
        requests.delete(f"{CONNECT}/connectors/{NAME}", timeout=10).raise_for_status()
        print(f"deleted {NAME}")
        return

    requests.put(f"{CONNECT}/connectors/{NAME}/config", json=CONFIG, timeout=10).raise_for_status()
    status = wait_until_running()
    print(f"{NAME}: connector {status['connector']['state']}, {len(status['tasks'])} task running")
    topics = requests.get(f"{CONNECT}/connectors/{NAME}/topics", timeout=10).json()
    print(f"topics: {sorted(topics[NAME]['topics'])}")


if __name__ == "__main__":
    main()
