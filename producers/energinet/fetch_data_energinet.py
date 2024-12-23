import redis
import os
import redis.client
import requests
from dotenv import load_dotenv
from dateutil.parser import parse
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import hashlib

# Load environment variables from .env
load_dotenv()

# Database connection --------------------------------------------------------

# Set Redis database
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = int(os.getenv("REDIS_DB", 0))
PROCESSED_RECORD_KEY = os.getenv("KAFKA_TOPIC") + "_record_key"
PROCESSED_DATE_KEY = os.getenv("KAFKA_TOPIC") + "_date_key"

# Connect to Redis
try:
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
except Exception as e:
    print(f"Error connecting to Redis: {e}")
    exit(1)


# Clear Redis keys
def clear_redis_key(type):
    try:
        redis_client.delete(type)
        print(f"Successfully cleared Redis key: {type}")
    except redis.RedisError as e:
        print(f"Error clearing Redis key {type}: {e}")


# Save record to Redis hash
def save_record(type, record_key, record_data):
    try:
        record_data['timestamp'] = datetime.utcnow().isoformat()  # Add current timestamp
        redis_client.hset(type, record_key, json.dumps(record_data))
    except redis.RedisError as e:
        print(f"Error saving record: {e}")


# check if redis is empty
def is_redis_empty(type):
    try:
        return redis_client.hlen(type) == 0
    except redis.RedisError as e:
        print(f"Error checking if Redis is empty: {e}")
        return False


# Check if a record exists in Redis
def is_record_in_redis(type, record_key):
    try:
        return redis_client.hexists(type, record_key)
    except redis.RedisError as e:
        print(f"Error checking record {record_key} in Redis: {e}")
        return False


def generate_key_from_record(record):
    # Serialize record to JSON string with sorted keys
    record_str = json.dumps(record, sort_keys=True)
    # Generate a consistent SHA256 hash
    return hashlib.sha256(record_str.encode('utf-8')).hexdigest()


# Data handling --------------------------------------------------------------
# API Configuration
API_URL = os.getenv("API_URL")
FROM_DATE = os.getenv("FROM_DATE")
TO_DATE = os.getenv("TO_DATE", datetime.now().strftime("%Y-%m-%d"))
ORDER_BY = os.getenv("ORDER_BY")


def validate_date(date):
    if date and date.strip():
        return True
    else:
        return False


# Fetch Data from API
def fetch_api_data(start_date, end_day):
    try:
        print(f"Fetching new data... from {start_date} to {end_day}. ")
        response = requests.get(
            API_URL + "?offset=0&start=" + start_date + "&end=" + end_day + "&sort=" + ORDER_BY + "%20DESC")
        response.raise_for_status()
        result = response.json()
        return result
    except Exception as e:
        print(f"Error fetching API data in call: {e}")
        return []


def fetch_api_dates():
    try:
        # Fetch `from_date`
        if not validate_date(FROM_DATE):
            from_date_response = requests.get(API_URL + f"?offset=0&limit=1&sort={ORDER_BY}%20ASC")
            from_date_response.raise_for_status()
            from_date = parse(from_date_response.json().get('records', [])[0].get(ORDER_BY))
        else:
            from_date_response = requests.get(API_URL + f"?offset=0&start={FROM_DATE}&limit=1&sort={ORDER_BY}%20ASC")
            from_date_response.raise_for_status()
            from_date = parse(from_date_response.json().get('records', [])[0].get(ORDER_BY))

        # Fetch `to_date`
        if not TO_DATE:
            to_date_response = requests.get(API_URL + f"?offset=0&limit=1&sort={ORDER_BY}%20DESC")
            to_date_response.raise_for_status()
            to_date = parse(to_date_response.json().get('records', [])[0].get(ORDER_BY))
        else:
            to_date_response = requests.get(API_URL + f"?offset=0&end={TO_DATE}&limit=1&sort={ORDER_BY}%20DESC")
            to_date_response.raise_for_status()
            to_date = parse(to_date_response.json().get('records', [])[0].get(ORDER_BY))

        # Ensure from_date is earlier than to_date
        if from_date > to_date:
            raise ValueError(f"Inverted date range: from_date ({from_date}) is later than to_date ({to_date})")

        # Return both dates
        return from_date, to_date

    except Exception as e:
        print(f"Error fetching API dates: {e}")
        return None, None


# Fetch Data and Save to Redis
def fetch(producer):
    try:
        # set the from and to date
        from_date, to_date = fetch_api_dates()
        time_taken = 0

        while from_date <= to_date:

            print("Fetching data for date:", from_date.isoformat())

            # Check if date has already been processed
            date_key = generate_key_from_record({"date": from_date.isoformat()})
            if is_record_in_redis(PROCESSED_DATE_KEY, date_key):
                print(f"Date {from_date.isoformat()} already processed.")
                from_date += relativedelta(days=1)
                continue

            if time_taken < 10:
                print("Reducing fetch frequency to avoid rate limiting...")
                time.sleep(5)

            start_timer = datetime.now()

            # Fetch data for the current date
            next_date = from_date + relativedelta(days=1)
            result = fetch_api_data(from_date.strftime("%Y-%m-%dT%H:%M"), next_date.strftime("%Y-%m-%dT%H:%M"))
            save_record(PROCESSED_DATE_KEY, date_key, {"date": from_date.isoformat()})
            records = result.get("records", [])
            # schema_name = result.get("dataset", SCHEMA_PATH)

            # Process each record
            for record in records:
                record_data_key = generate_key_from_record(record)
                if not is_record_in_redis(PROCESSED_RECORD_KEY, record_data_key):  # Avoid duplicates
                    send_to_kafka(record, producer)
                    save_record(PROCESSED_RECORD_KEY, record_data_key, record)
                else:
                    print(f"Record {record_data_key} already processed.")

            from_date = next_date
            clear_redis_key(PROCESSED_RECORD_KEY)
            end_timer = datetime.now()

            time_taken = (end_timer - start_timer).total_seconds()
            print(f"Time taken to fetch data for date {from_date.isoformat()}: {time_taken}")
            print("Done fetching data")
    except Exception as e:
        print(f"Error fetching API data main loop: {e}")


# Kafka communication ---------------------------------------------------------

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroSerializer

from confluent_kafka.schema_registry import SchemaRegistryClient

# Kafka and Schema Registry Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST") + ":" + os.getenv("KAFKA_BOOTSTRAP_SERVERS_PORT")
SCHEMA_REGISTRY_URL = "http://" + os.getenv("SCHEMA_REGISTRY_HOST") + ":" + os.getenv("SCHEMA_REGISTRY_PORT")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

CACHE_SCHEMA = {}


def user_to_dict(record, ctx):
    return record  # Adjust this if your `record` structure differs


# Initialize Kafka Producer
def get_producer():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return producer


# Send Record to Kafka
def send_to_kafka(record, producer):
    try:

        subject = KAFKA_TOPIC + "-value"
        schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        subjects = schema_registry_client.get_subjects()
        if subject not in subjects:
            print("Schema registered for subject:", KAFKA_TOPIC + "-value not found.")
            raise Exception("Schema not found in the registry.")
        # Load schema string from a schema registry

        # Check and cache schema as a string
        if subject not in CACHE_SCHEMA:
            try:
                # Fetch schema from the registry as a string
                schema_str = fetch_schema_from_registry(subject)
                CACHE_SCHEMA[subject] = schema_str  # Store raw string
                print(f"Schema cached for subject: {subject}")
            except Exception as e:
                print(f"Error fetching schema for subject {subject}: {e}")
                raise

        schema_str = CACHE_SCHEMA[subject]

        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str,
            to_dict=user_to_dict
        )

        key = str(record.get(ORDER_BY))
        string_serializer = StringSerializer("utf_8")
        # SerializationContext specifies the message field as the value
        producer.produce(
            topic=KAFKA_TOPIC,
            key=string_serializer(key, SerializationContext(KAFKA_TOPIC, MessageField.KEY)),
            value=avro_serializer(record, SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
        )
        producer.flush()
    except Exception as e:
        print(f"Error sending record to Kafka: {e}")
        raise e


# # Initialize the Schema Registry client
def get_schema_registry_client():
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


# # Fetch Schema by Subject
def fetch_schema_from_registry(subject):
    try:
        client = get_schema_registry_client()
        # Get the latest version of the schema
        schema = client.get_latest_version(subject)
        return schema.schema.schema_str  # Return the schema string
    except Exception as e:
        print(f"Error fetching schema from registry: {e}")
        raise


# Main Function --------------------------------------------------------------
import time

SLEEP_DELAY = int(os.getenv("SLEEP_DELAY", 60))


def main():
    while True:
        try:
            print("Starting fetch...")
            producer = get_producer()
            fetch(producer)
            print("Fetch completed. Restarting in 60 seconds...")

        except Exception as e:
            print(f"Fatal error during fetch: {e}. Retrying in 60 seconds...")

        # Wait 60 seconds before restarting
        time.sleep(SLEEP_DELAY)


if __name__ == "__main__":
    main()