import json

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from twin.stream import settings
from twin.stream.kafka_sink import load_schema


def required_carrier_schema() -> str:
    schema = json.loads(load_schema(2))
    for field in schema["fields"]:
        if field["name"] == "carrier_id":
            field["type"] = "string"
            field.pop("default")
    return json.dumps(schema)


def main() -> None:
    client = SchemaRegistryClient({"url": settings.REGISTRY_URL})
    subject = f"{settings.TOPIC}-value"
    print(f"compatibility rule: {client.get_compatibility()}")
    print(f"registered versions: {client.get_versions(subject)}")

    candidates = {
        "v2, carrier_id optional with default": load_schema(2),
        "carrier_id required, no default": required_carrier_schema(),
    }
    for label, schema in candidates.items():
        ok = client.test_compatibility(subject, Schema(schema, "AVRO"))
        print(f"{label}: {'compatible' if ok else 'incompatible'}")

    try:
        client.register_schema(subject, Schema(required_carrier_schema(), "AVRO"))
        print("required carrier schema was registered, which should not happen")
    except SchemaRegistryError as error:
        print(f"registering the required carrier schema failed: HTTP {error.http_status_code}")


if __name__ == "__main__":
    main()
