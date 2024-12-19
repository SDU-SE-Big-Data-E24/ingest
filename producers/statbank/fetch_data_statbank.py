import os

import requests
import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import redis
from dotenv import load_dotenv
import hashlib

load_dotenv()
# Configuration

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://kafka-schema-registry:8081"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
PARAM = os.getenv("PARAM")
API_URL = f"https://api.statbank.dk/v1/data/{KAFKA_TOPIC}/JSONSTAT?{PARAM}"

# Redis connection
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def generate_key_from_record(record):
    record_str = json.dumps(record, sort_keys=True)
    return hashlib.sha256(record_str.encode('utf-8')).hexdigest()


def fetch_statbank_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return None


def clean_statbank_data(raw_data):
    dataset = raw_data["dataset"]
    dimensions = dataset["dimension"]

    # Identificer fælles felter
    label = dataset["label"]
    source = dataset["source"]
    updated = dataset["updated"]
    documentation = dataset.get("extension", {}).get("px", {}).get("infofile", None)
    table_id = dataset.get("extension", {}).get("px", {}).get("tableid", "Unknown")  # Sæt standardværdi
    decimals = dataset.get("extension", {}).get("px", {}).get("decimals", 0)
    values = dataset["value"]
    time_labels = dimensions["Tid"]["category"]["label"]

    # Identificer felter, der varierer mellem JSON-typer
    region = dimensions["OMRÅDE"]["category"]["label"]["000"] if "OMRÅDE" in dimensions else None
    vehicle_type = dimensions["BILTYPE"]["category"]["label"]["4000100001"] if "BILTYPE" in dimensions else None
    terms_of_use = dimensions["BRUG"]["category"]["label"]["1000"] if "BRUG" in dimensions else None
    ownership = dimensions["EJER"]["category"]["label"]["1000"] if "EJER" in dimensions else None
    propellant = dimensions["DRIV"]["category"]["label"]["20225"] if "20225" in dimensions.get("DRIV", {}).get("category", {}).get("label", {}) else dimensions["DRIV"]["category"]["label"].get("20200")
    contents_code = next(iter(dimensions["ContentsCode"]["category"]["label"].values()))
    unit = next(iter(dimensions["ContentsCode"]["category"]["unit"].values()))["base"]

    # Opret renset datastruktur
    cleaned_data = [
        {
            "time": time,
            "value": value,
            "region": region,
            "type_of_vehicle": vehicle_type,
            "terms_of_use": terms_of_use,
            "ownership": ownership,
            "propellant": propellant,
            "contents_code": contents_code,
            "unit": unit,
            "label": label,
            "source": source,
            "updated": updated,
            "documentation": documentation,
            "table_id": table_id,  # Inkluder table_id
            "decimals": decimals
        }
        for time, value in zip(time_labels.values(), values)
    ]

    return cleaned_data




def send_to_kafka(record, producer, schema_registry_client):
    try:
        subject = KAFKA_TOPIC + "-value"
        schema = schema_registry_client.get_latest_version(subject).schema.schema_str
        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema,
            to_dict=lambda record, ctx: record
        )

        key = record["time"]
        string_serializer = StringSerializer("utf_8")
        producer.produce(
            topic=KAFKA_TOPIC,
            key=string_serializer(key, SerializationContext(KAFKA_TOPIC, MessageField.KEY)),
            value=avro_serializer(record, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
        )
        producer.flush()
    except Exception as e:
        print(f"Error sending record to Kafka: {e}")


def process_data_by_month(cleaned_data, producer, schema_registry_client):
    for record in cleaned_data:
        record_key = generate_key_from_record(record)
        if not redis_client.hexists(KAFKA_TOPIC, record_key):  # Deduplication
            send_to_kafka(record, producer, schema_registry_client)
            redis_client.hset(KAFKA_TOPIC, record_key, json.dumps(record))


import time

SLEEP_DELAY = int(os.getenv("SLEEP_DELAY", 60))

def main():
    while True:
        try:
            print ("Fetching data from Statbank API")
            raw_data = fetch_statbank_data()
            if not raw_data:
                return

            cleaned_data = clean_statbank_data(raw_data)

            schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

            process_data_by_month(cleaned_data, producer, schema_registry_client)

            print("Data processing completed.")
        except Exception as e:
            print(f"Error in main: {e}")

        # Wait 60 seconds before restarting
        time.sleep(SLEEP_DELAY)



if __name__ == "__main__":
    main()
