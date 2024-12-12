import argparse
import requests
import json
from confluent_kafka import Producer
import avro.schema
import avro.io
import io
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_data_from_api(url: str, params: dict = None) -> dict:
    """
    Fetch data from the given API URL with optional parameters.

    :param url: The API endpoint URL.
    :param params: A dictionary of query parameters to include in the request.
    :return: The JSON response from the API as a dictionary.
    """
    try:
        logging.info(f"Fetching data from API: {url} with params: {params}")
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'EnergiDataFetcher/1.0'  # As recommended by API guides
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        logging.info(f"Fetched data: {json.dumps(data, indent=2)}")  # Log fetched data
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return {}

def convert_to_avro(data: dict, schema_str: str) -> bytes:
    """
    Convert a dictionary to Avro format using the provided schema.

    :param data: The data to convert.
    :param schema_str: The Avro schema as a JSON string.
    :return: The data in Avro format as bytes.
    """
    try:
        logging.info("Converting data to Avro format.")
        schema = avro.schema.parse(schema_str)
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        avro_data = bytes_writer.getvalue()
        logging.info(f"Converted Avro data (bytes): {avro_data}")
        return avro_data
    except Exception as e:
        logging.error(f"Error converting data to Avro: {e}")
        return b''

def send_to_kafka(topic: str, data: bytes, kafka_config: dict):
    """
    Send data to a Kafka topic.

    :param topic: The Kafka topic.
    :param data: The data to send.
    :param kafka_config: Configuration for the Kafka producer.
    """
    try:
        logging.info(f"Sending data to Kafka topic: {topic}")
        producer = Producer(kafka_config)
        producer.produce(topic, value=data, callback=delivery_report)
        producer.flush()
        logging.info("Data successfully sent to Kafka.")
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {e}")

def delivery_report(err, msg):
    """ Delivery report callback for Kafka producer. """
    if err is not None:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logging.info(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch data from API and send to Kafka.")
    parser.add_argument("--url", required=True, help="The API endpoint URL.")
    parser.add_argument("--params", required=False, help="Query parameters as a JSON string.")
    parser.add_argument("--schema", required=True, help="The name of the Avro schema to use (e.g., ConsumptionIndustry).")
    parser.add_argument("--kafka-topic", required=True, help="The Kafka topic to send data to.")
    parser.add_argument("--schema-dir", required=False, default="schemas", help="Directory containing Avro schemas.")

    args = parser.parse_args()

    api_url = args.url
    query_params = json.loads(args.params) if args.params else {}
    schema_name = args.schema
    kafka_topic = args.kafka_topic
    schema_dir = args.schema_dir

    # Fetch data from API
    data = fetch_data_from_api(api_url, query_params)

    if not data:
        logging.error("No data fetched from API. Exiting.")
    else:
        # Extract records
        records = data.get('records', [])
        if not records:
            logging.error("No records found in the fetched data. Exiting.")
        else:
            # Load the Avro schema
            schema_file = os.path.join(schema_dir, f"{schema_name}.avsc")
            if not os.path.exists(schema_file):
                logging.error(f"Schema file not found: {schema_file}. Exiting.")
                exit(1)

            with open(schema_file, 'r') as f:
                avro_schema_str = f.read()

            # Convert records to Avro format
            avro_data_list = []
            for record in records:
                avro_data = convert_to_avro(record, avro_schema_str)
                if avro_data:
                    avro_data_list.append(avro_data)
                else:
                    logging.error(f"Failed to convert record to Avro: {record}")

            # Send to Kafka if data exists
            if avro_data_list:
                kafka_config = {'bootstrap.servers': 'localhost:9093'}
                for avro_data in avro_data_list:
                    send_to_kafka(kafka_topic, avro_data, kafka_config)
            else:
                logging.error("No data to send to Kafka. Exiting.")
