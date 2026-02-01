import avro.schema
import avro.io
import io
import json
import base64
import binascii
import re
from fastavro import schemaless_writer, schemaless_reader
import requests
import os
from dotenv import load_dotenv

load_dotenv()

SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
SCHEMA_REGISTRY_USERNAME = os.environ["SCHEMA_REGISTRY_USERNAME"]
SCHEMA_REGISTRY_PASSWORD = os.environ["SCHEMA_REGISTRY_PASSWORD"]


class CustomBinaryDecoder(avro.io.BinaryDecoder):
    def read_utf8(self):
        byte_data = self.read_bytes()
        try:
            return byte_data.decode("utf-8")
        except UnicodeDecodeError:
            return byte_data.decode("latin-1", errors="replace")
        except Exception as e:
            return None

def deserialize_data(schema, value):
    try:
        if isinstance(value, bytes):
            binary = value
        elif isinstance(value, bytearray):
            binary = bytes(value)
        elif isinstance(value, str):
            try:
                binary =  base64.b64decode(value, validate=True)
            except (binascii.Error, ValueError):
                if '\ufffd' in value:
                    cleaned = value.replace('\ufffd', '')
                    binary = cleaned.encode("utf-8", errors="ignore")
                else:
                    try:
                        binary = value.encode("latin-1")
                    except UnicodeEncodeError:
                        binary = value.encode("utf-8", errors="surrogateescape")
        else:
            return None

        inner_schema_dict = json.loads(schema)
        inner_schema = avro.schema.parse(json.dumps(inner_schema_dict))

        decoder = CustomBinaryDecoder(io.BytesIO(binary))
        reader = avro.io.DatumReader(inner_schema)
        inner_decoded = reader.read(decoder)
        return inner_decoded
    except Exception as e:
        return None

def deserialize_data_id(schema_id, value):
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"

    response = requests.get(url, auth=(
        SCHEMA_REGISTRY_USERNAME,
        SCHEMA_REGISTRY_PASSWORD
    ))

    schema = response.json()['schema']
    try:
        if isinstance(value, bytes):
            binary = value
        elif isinstance(value, bytearray):
            binary = bytes(value)
        elif isinstance(value, str):
            try:
                binary =  base64.b64decode(value, validate=True)
            except (binascii.Error, ValueError):
                if '\ufffd' in value:
                    cleaned = value.replace('\ufffd', '')
                    binary = cleaned.encode("utf-8", errors="ignore")
                else:
                    try:
                        binary = value.encode("latin-1")
                    except UnicodeEncodeError:
                        binary = value.encode("utf-8", errors="surrogateescape")
        else:
            return None

        inner_schema_dict = json.loads(schema)
        inner_schema = avro.schema.parse(json.dumps(inner_schema_dict))

        decoder = CustomBinaryDecoder(io.BytesIO(binary))
        reader = avro.io.DatumReader(inner_schema)
        inner_decoded = reader.read(decoder)
        return inner_decoded
    except Exception as e:
        return None