import avro.schema
import avro.io
from confluent_kafka import Consumer, KafkaError
import io
import json
from fastavro import reader

# Kafka and Schema Registry Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # Replace with the actual node IP and port
KAFKA_TOPIC = "industry-consumption"
KAFKA_GROUP_ID = "industry-consumption-group"
SCHEMA_PATH = "consumption_industry_schema.avsc"

# Load Avro Schema
schema = avro.schema.parse(open(SCHEMA_PATH, "rb").read())


# Initialize Kafka Consumer
def get_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest"
    })


def deserialize_avro_message(message):
    with open(SCHEMA_PATH, "r") as schema_file:
        schema = json.load(schema_file)

    avro_reader = reader(io.BytesIO(message), schema)
    for record in avro_reader:
        return record


# Consume Messages from Kafka
def consume_messages():
    consumer = get_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    try:
        print("Consuming messages...")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Deserialize Avro message
            record = deserialize_avro_message(msg.value())
            if record:
                print("Deserialized Record:", record)

    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()


# Main Function
def main():
    consume_messages()


if __name__ == "__main__":
    main()