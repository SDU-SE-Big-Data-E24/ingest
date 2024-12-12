import threading
import requests

import avro.schema
import avro.io
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from confluent_kafka import Producer


# Kafka and Schema Registry Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # Replace with the actual node IP and port
SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
KAFKA_TOPIC = "industry-consumption"

# API Configuration
API_URL = 'https://api.energidataservice.dk/dataset/ConsumptionIndustry?limit=5'

# Load Avro Schema
schema_path = "consumption_industry_schema.avsc"
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
        records = result.get('records', [])
        for record in records:
            return records
    except Exception as e:
        print(f"Error fetching API data: {e}")
        return []


# Serialize Record Using Avro
def serialize_record(record):
    try:
        writer = DataFileWriter(open("consumption_industry.avro","wb"), DatumWriter(), schema)
        writer.append(record)
        writer.close()
        return True
    except Exception as e:
        print(f"Error serializing record: {e}")
        return False



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
    for record in records:
        send_to_kafka(record, producer)


# Repeating Timer for Periodic Execution
class RepeatTimer(threading.Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


# Main Function
def main():
    producer = get_producer()
    timer = RepeatTimer(10.0, produce_messages, [producer])  # Run every 10 seconds

    try:
        timer.start()
        while True:
            pass
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        timer.cancel()
        producer.flush()


if __name__ == "__main__":
    main()
