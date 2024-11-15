import os
from google.cloud import storage
from dotenv import load_dotenv
import pickle
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

TOPIC_NAME = os.getenv('TOPIC_NAME')
producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_REMOTE_SERVERS'),
    acks='all',
    batch_size=16 * 1024,  # Smaller batch size
    linger_ms=5,  # Lower linger time
    buffer_memory=5 * 1024 * 1024,
    max_request_size=2 * 1024 * 1024,  # 2 MB limit
)


# Function to send each deserialized pickle object to Kafka
def send_to_kafka(serialized_object, index):
    try:
        logging.info(f"Sending object {index} of size {len(serialized_object)} bytes to Kafka.")
        producer.send(TOPIC_NAME, serialized_object)
        producer.flush()
        logging.info(f"Object {index} sent successfully.")
    except Exception as e:
        logging.error(f"Error sending object {index}: {e}")


# Process each pickle object from a large file in GCS without loading entire file
def process_large_pickle_file(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    index = 0

    # Open file from GCS and read each pickle object sequentially
    with blob.open('rb') as f:
        while True:
            try:
                # Load the next pickled object
                obj = pickle.load(f)
                # Serialize the object before sending
                serialized_object = pickle.dumps(obj)

                # Send the serialized object to Kafka
                send_to_kafka(serialized_object, index)
                index += 1

            except EOFError:
                # End of file reached
                logging.info("Reached end of pickle file.")
                break
            except Exception as e:
                logging.error(f"Error reading or sending object {index}: {e}")
                break


# Main function to control the Kafka streaming process
def main():
    bucket_name = os.getenv('GCS_BUCKET2')
    file_name = os.getenv('BIG_FILE2')

    logging.info("Starting the producer for large pickle file processing.")
    process_large_pickle_file(bucket_name, file_name)

    producer.close()
    logging.info("All objects sent successfully.")


if __name__ == '__main__':
    main()
