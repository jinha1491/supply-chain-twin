import os

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("TWIN_TOPIC", "twin.events")
