import requests
import os
import pickle
import avro.io
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from confluent_kafka import Producer
from dotenv import load_dotenv
from dateutil.parser import parse
from datetime import timedelta, datetime

# Load environment variables from .env
load_dotenv()

# File to store the last date value
LAST_DATE_FILE = "last_date.pkl"


# Load the last date value
def load_last_date():
    if os.path.exists(LAST_DATE_FILE):
        with open(LAST_DATE_FILE, "rb") as f:
            return parse(pickle.load(f))
    return 0  # Default total if no file exists


# Save the last date value
def save_last_date(date):
    with open(LAST_DATE_FILE, "wb") as f:
        pickle.dump(date, f)


# Kafka and Schema Registry Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# API Configuration
API_URL = os.getenv("API_URL")
API_URL_FIRST_DATE = os.getenv("API_URL_FIRST_DATE")
API_URL_LAST_DATE = os.getenv("API_URL_LAST_DATE")


# Initialize Kafka Producer
def get_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def historical_fetch(producer):
    try:
        latest_date = parse(requests.get(API_URL_LAST_DATE).json().get('records', [])[0].get('HourUTC'))

        if load_last_date() == 0:
            current_date = parse(requests.get(API_URL_FIRST_DATE).json().get('records', [])[0].get('HourUTC'))
        else:
            current_date = load_last_date()

        while current_date <= latest_date:
            next_day = current_date + timedelta(days=1)
            result = fetch_api_data(current_date.strftime("%Y-%m-%dT%H:%M"), next_day.strftime("%Y-%m-%dT%H:%M"))
            records = result.get("records", [])
            schema_name = result.get("dataset", "")

            for record in records:
                send_to_kafka(record, producer, schema_name)

            save_last_date(current_date.strftime("%Y-%m-%dT%H:%M"))
            current_date = next_day
    except Exception as e:
        print(f"Error fetching API data: {e}")


# Fetch Data from API
def fetch_api_data(start_date, end_day):
    try:
        print(f"Fetching new data... from {start_date} to {end_day}. ")
        response = requests.get(API_URL + f"?offset=0&start={start_date}&end={end_day}&sort=HourUTC%20DESC")
        response.raise_for_status()
        result = response.json()
        return result
    except Exception as e:
        print(f"Error fetching API data: {e}")
        return []


# Serialize Record Using Avro
def serialize_record(record, schema):
    try:
        writer = DataFileWriter(open("temp_schema.avsc", "wb"), DatumWriter(), schema)
        writer.append(record)
        writer.close()
        return True
    except Exception as e:
        print(f"Error serializing record: {e}")
        return False


# Send Record to Kafka
def send_to_kafka(record, producer, schema_name):
    schema = avro.schema.parse(open(schema_name + ".avsc", "rb").read())
    try:
        key = str(record.get("MunicipalityNo"))
        if serialize_record(record, schema):
            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode("utf-8"),
                value=open("temp_schema.avsc", "rb").read()
            )
            producer.flush()
        else:
            print(f"Serialization failed for record: {record}")
    except Exception as e:
        print(f"Error sending record to Kafka: {e}")


# Produce Messages Repeatedly
def produce_messages(producer):
    # if database has data, fetch data from last date to current date
    today = datetime.now()
    if load_last_date() == 0 or load_last_date() <= today:
        historical_fetch(producer)


# Main Function
def main():
    producer = get_producer()
    produce_messages(producer)


if __name__ == "__main__":
    main()
