import requests
import os
import pickle
import avro.io
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# File to store the last total value
LAST_TOTAL_FILE = "last_total.pkl"

# File to store processed record keys
PROCESSED_RECORDS_FILE = "processed_records.pkl"


# Load the last total value
def load_last_total():
    if os.path.exists(LAST_TOTAL_FILE):
        with open(LAST_TOTAL_FILE, "rb") as f:
            return pickle.load(f)
    return 0  # Default total if no file exists


# Save the last total value
def save_last_total(total):
    with open(LAST_TOTAL_FILE, "wb") as f:
        pickle.dump(total, f)


# Load processed record keys
def load_processed_records():
    if os.path.exists(PROCESSED_RECORDS_FILE):
        with open(PROCESSED_RECORDS_FILE, "rb") as f:
            return pickle.load(f)
    return set()


# Save processed record keys
def save_processed_records(processed_records):
    with open(PROCESSED_RECORDS_FILE, "wb") as f:
        pickle.dump(processed_records, f)


# Kafka and Schema Registry Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# API Configuration
API_URL = os.getenv("API_URL")

# Load Avro Schema
schema_path = os.getenv("SCHEMA_PATH")
schema = avro.schema.parse(open(schema_path, "rb").read())


# Initialize Kafka Producer
def get_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


# Fetch Data from API
def fetch_api_data():
    print("Fetching data from API...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        result = response.json()

        current_total = result.get('total', 0)
        last_total = load_last_total()

        if current_total != last_total:
            print(f"Total has changed from {last_total} to {current_total}. Fetching new data...")
            save_last_total(current_total)
            return result.get('records', [])
        else:
            print("No new data. Total has not changed.")
            return []
    except Exception as e:
        print(f"Error fetching API data: {e}")
        return []


# Serialize Record Using Avro
def serialize_record(record):
    try:
        writer = DataFileWriter(open("consumption_industry.avro", "wb"), DatumWriter(), schema)
        writer.append(record)
        writer.close()
        return True
    except Exception as e:
        print(f"Error serializing record: {e}")
        return False


# Send Record to Kafka
def send_to_kafka(record, producer):
    print(f"Preparing to send record to Kafka: {record}")
    try:
        key = str(record.get("MunicipalityNo"))
        if serialize_record(record):
            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode("utf-8"),
                value=open("consumption_industry.avro", "rb").read()
            )
            producer.flush()
        else:
            print(f"Serialization failed for record: {record}")
    except Exception as e:
        print(f"Error sending record to Kafka: {e}")


# Produce Messages Repeatedly
def produce_messages(producer):
    print("Producing messages...")
    records = fetch_api_data()

    if not records:
        print("No new records to process.")
        return

    # Load previously processed records
    processed_records = load_processed_records()

    for record in records:
        # Generate a composite key
        record_key = f"{record.get('HourUTC')}-{record.get('HourDK')}-{record.get('MunicipalityNo')}-{record.get('Branche')}-{record.get('ConsumptionkWh')}"

        if record_key in processed_records:
            print(f"Skipping duplicate record: {record_key}")
            continue  # Skip already processed records

        send_to_kafka(record, producer)

        # Mark record as processed
        processed_records.add(record_key)

    # Save processed records
    save_processed_records(processed_records)


# Main Function
def main():
    producer = get_producer()
    produce_messages(producer)


if __name__ == "__main__":
    main()
