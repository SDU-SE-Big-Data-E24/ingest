import redis
import os
import requests
from dotenv import load_dotenv
from dateutil.parser import parse
from datetime import timedelta, datetime
import pickle

# Load environment variables from .env
load_dotenv()

# Redis connection settings
REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # Kubernetes Redis service name
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
PROCESSED_RECORDS_KEY = "processed_records"

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


# Save an individual record to Redis
def save_record_to_redis(record_key, record_data):
    try:
        redis_client.hset(PROCESSED_RECORDS_KEY, record_key, record_data)
    except redis.RedisError as e:
        print(f"Error saving record {record_key} to Redis: {e}")


# Check if a record exists in Redis
def is_record_in_redis(record_key):
    try:
        return redis_client.hexists(PROCESSED_RECORDS_KEY, record_key)
    except redis.RedisError as e:
        print(f"Error checking record {record_key} in Redis: {e}")
        return False


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
from confluent_kafka import Producer


def get_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


# Fetch Data and Save to Redis
def historical_fetch(producer):
    try:
        latest_date = parse(requests.get(API_URL_LAST_DATE).json().get('records', [])[0].get('HourUTC'))
        current_date = parse(requests.get(API_URL_FIRST_DATE).json().get('records', [])[0].get('HourUTC'))

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
                record_data = str(record)  # Convert record to string for storage
                hash_key = hash(record_data)

                if is_record_in_redis(hash_key):
                    print(f"Record {hash_key} already exists in Redis")

                if not is_record_in_redis(hash_key):  # Avoid duplicates
                    send_to_kafka(record, producer, schema_name)
                    check = save_record_to_redis(hash_key, record_data)
                    print(check)

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


# Send Record to Kafka
def send_to_kafka(record, producer, schema_name):
    try:
        key = str(record.get("MunicipalityNo"))
        value = str(record)  # Serialize record to string
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8")
        )
        producer.flush()
    except Exception as e:
        print(f"Error sending record to Kafka: {e}")


# Main Function
def main():
    producer = get_producer()
    historical_fetch(producer)


if __name__ == "__main__":
    main()
