import redis
import os
import redis.client
import requests
from dotenv import load_dotenv
from dateutil.parser import parse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema
import json
import hashlib
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

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
        response = requests.get(API_URL + "?offset=0&start=" + start_date + "&end=" + end_day + "&sort=HourUTC%20DESC")
        response.raise_for_status()
        result = response.json()
        return result
    except Exception as e:
        print(f"Error fetching API data: {e}")
        return []


def fetch_api_dates():
    try:
        from_date_status = None
        # Fetch `from_date`
        if not validate_date(FROM_DATE):
            from_date_response = requests.get(API_URL + f"?offset=0&limit=1&sort={ORDER_BY}%20ASC")
            print(from_date_response.raise_for_status())
            from_date = parse(from_date_response.json().get('records', [])[0].get(ORDER_BY))
        else:
            from_date_response = requests.get(API_URL + f"?offset=0&start={FROM_DATE}&limit=1&sort={ORDER_BY}%20ASC")
            print(from_date_response.raise_for_status())
            from_date = parse(from_date_response.json().get('records', [])[0].get(ORDER_BY))

        # Fetch `to_date`
        if not TO_DATE:
            to_date_response = requests.get(API_URL + f"?offset=0&limit=1&sort={ORDER_BY}%20DESC")
            print(to_date_response.raise_for_status())
            to_date = parse(to_date_response.json().get('records', [])[0].get(ORDER_BY))
        else:
            to_date_response = requests.get(API_URL + f"?offset=0&end={TO_DATE}&limit=1&sort={ORDER_BY}%20DESC")
            print(to_date_response.raise_for_status())
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
        print(f"Fetching data from {from_date} to {to_date}")

        while from_date <= to_date:
            # Check if date has already been processed
            date_key = generate_key_from_record({"date": from_date.isoformat()})
            if is_record_in_redis(PROCESSED_DATE_KEY, date_key):
                print(f"Date {from_date} already processed.")
                from_date += relativedelta(days=1)
                continue

            # Fetch data for the current date
            next_date = from_date + relativedelta(days=1)
            result = fetch_api_data(from_date.strftime("%Y-%m-%dT%H:%M"), next_date.strftime("%Y-%m-%dT%H:%M"))
            save_record(PROCESSED_DATE_KEY, date_key, {"date": from_date.isoformat()})
            records = result.get("records", [])

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
    except Exception as e:
        print(f"Error fetching API data: {e}")


# Kafka communication ---------------------------------------------------------
import os
import avro.schema
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient

# Kafka and Schema Registry Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST") + ":" + os.getenv("KAFKA_BOOTSTRAP_SERVERS_PORT")
SCHEMA_REGISTRY_URL = "http://" + os.getenv("SCHEMA_REGISTRY_HOST") + ":" + os.getenv("SCHEMA_REGISTRY_PORT")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
SCHEMA_CACHE_DIR = os.getenv("SCHEMA_CACHE_DIR", "./cached_schemas")
CACHE_SCHEMA = {}
# Ensure schema cache directory exists
os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)

# Initialize AvroProducer
def get_producer(topic_name):
    """
    Initializes an AvroProducer dynamically based on topic-specific schemas.
    """
    value_subject = f"{topic_name}-value"
    value_schema = get_schema_from_registry(value_subject)

    # Return an AvroProducer using the dynamic schema
    avro_producer = AvroProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
        },
        default_value_schema=value_schema
    )
    return avro_producer

# Send Record to Kafka
def send_to_kafka(record, producer):
    try:
        # Use any field as the key (e.g., first field of the schema)
        key = str(next(iter(record.values())))  # Use the first value as the key dynamically

        # Ensure the record matches the schema structure
        value = {field: record[field] for field in record}

        # Send the record
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=value
        )
        producer.flush()
    except Exception as e:
        print(f"Error sending record to Kafka: {e}")


# def get_cached_schema(subject):
#     """
#     Fetches and caches the schema for a given subject.
#     """
#     if subject not in CACHE_SCHEMA:
#         try:
#             # Fetch schema from the registry
#             schema = get_schema_from_registry(subject)
#             # Parse and store in the cache
#             CACHE_SCHEMA[subject] = avro.schema.parse(schema)
#             print(f"Schema cached for subject: {subject}")
#         except Exception as e:
#             print(f"Error fetching or parsing schema for subject {subject}: {e}")
#             raise
#     return CACHE_SCHEMA[subject]


# # Serialize Record to Avro
# def serialize_record(record, subject):
#     try:
#
#         schema = get_cached_schema(subject)
#         # Save schema to a local file
#         writer = DataFileWriter(open("temp.avro", "wb"), DatumWriter(), schema)
#         writer.append(record)
#         writer.close()
#         return True
#     except Exception as e:
#         print(f"Error serializing record: {e}")
#         return False


# # Initialize the Schema Registry client
# def get_schema_registry_client():
#     return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# Fetch Schema by Subject
def get_schema_from_registry(subject_name):
    """
    Fetches schema from Schema Registry and caches it locally.
    """
    schema_cache_file = os.path.join(SCHEMA_CACHE_DIR, f"{subject_name}.avsc")
    if os.path.exists(schema_cache_file):
        with open(schema_cache_file, "r") as file:
            print(f"Loading schema from cache: {schema_cache_file}")
            return avro.schema.parse(file.read())

    try:
        print(f"Fetching schema for subject: {subject_name}")
        client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        schema_response = client.get_latest_version(subject_name)
        schema_str = schema_response.schema.schema_str

        # Cache the schema locally
        with open(schema_cache_file, "w") as file:
            file.write(schema_str)
            print(f"Schema cached at: {schema_cache_file}")
        return avro.schema.parse(schema_str)
    except Exception as e:
        print(f"Error fetching schema: {e}")
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
