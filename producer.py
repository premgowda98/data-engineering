import os
import io
import json
import uuid
import time
import base64
import struct
import requests
from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Environment variables
# -------------------------------------------------------------------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "medallion")

SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
SCHEMA_REGISTRY_USERNAME = os.environ["SCHEMA_REGISTRY_USERNAME"]
SCHEMA_REGISTRY_PASSWORD = os.environ["SCHEMA_REGISTRY_PASSWORD"]
OUTER_SCHEMA_ID = int(os.environ["OUTER_SCHEMA_ID"])
INNER_SCHEMA_ID = int(os.environ["INNER_SCHEMA_ID"])

USE_BASE64 = os.getenv("USE_BASE64", "true").lower() == "true"

# -------------------------------------------------------------------
# Kafka Producer
# -------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers="kafka-2e83401f-premgowda.e.aivencloud.com:21292",
    security_protocol="SSL",
    ssl_cafile="certs/ca.pem",
    ssl_certfile="certs/service.cert",
    ssl_keyfile="certs/service.key",
)

# -------------------------------------------------------------------
# Schema Registry helpers
# -------------------------------------------------------------------
def fetch_schema(schema_id: int) -> dict:
    """
    Fetch schema JSON from Schema Registry using schema ID
    """
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    response = requests.get(url, auth=(
        SCHEMA_REGISTRY_USERNAME,
        SCHEMA_REGISTRY_PASSWORD
    ))
    response.raise_for_status()
    return json.loads(response.json()["schema"])


# -------------------------------------------------------------------
# Load and parse schemas
# -------------------------------------------------------------------
outer_schema_dict = fetch_schema(OUTER_SCHEMA_ID)
inner_schema_dict = fetch_schema(INNER_SCHEMA_ID)

outer_schema = parse_schema(outer_schema_dict)
inner_schema = parse_schema(inner_schema_dict)

# -------------------------------------------------------------------
# Avro encoding helpers
# -------------------------------------------------------------------
def avro_encode(schema, record) -> bytes:
    """
    Encode record using Avro (no Confluent wire format)
    """
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record)
    return buffer.getvalue()


def confluent_wire_format(schema_id: int, avro_bytes: bytes) -> bytes:
    """
    Confluent wire format:
    [magic_byte=0][schema_id (4 bytes, big endian)][avro payload]
    """
    return b"\x00" + struct.pack(">I", schema_id) + avro_bytes


# -------------------------------------------------------------------
# Example inner payload (ENTITY SCHEMA DATA)
# -------------------------------------------------------------------
inner_record = {
    "rbpGroupId": "12345",
    "rbpGroupName": "rbp"
}

inner_avro_bytes = avro_encode(inner_schema, inner_record)

if USE_BASE64:
    inner_value_str = base64.b64encode(inner_avro_bytes).decode("utf-8")
else:
    # Only safe if bytes are valid UTF-8
    inner_value_str = inner_avro_bytes.decode("utf-8", errors="replace")

# -------------------------------------------------------------------
# Build outer record
# -------------------------------------------------------------------
outer_record = {
    "eventHeader": {
        "id": str(uuid.uuid4()),
        "appEnvironment": "dev"
    },
    "body": {
        "entityType": "PRODUCT",
        "entityId": str(uuid.uuid4()),
        "identifierType": "UUID",
        "data": {
            "schema": json.dumps(inner_schema_dict),
            "schemaType": "AVRO",
            "value": inner_value_str,
            "valueBytes": inner_avro_bytes,
            "schemaId": str(INNER_SCHEMA_ID)
        }
    }
}

# -------------------------------------------------------------------
# Encode outer record (Avro → Confluent wire format)
# -------------------------------------------------------------------
outer_avro_bytes = avro_encode(outer_schema, outer_record)
final_payload = confluent_wire_format(OUTER_SCHEMA_ID, outer_avro_bytes)

# -------------------------------------------------------------------
# Publish to Kafka
# -------------------------------------------------------------------
message_key = str(uuid.uuid4()).encode("utf-8")

producer.send(
    KAFKA_TOPIC,
    key=message_key,
    value=final_payload
)

producer.flush()
producer.close()

print("Message published successfully 🚀")
