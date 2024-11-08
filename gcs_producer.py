import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage
from dotenv import load_dotenv
import pickle

from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

TOPIC_NAME = os.getenv("TOPIC_NAME")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_REMOTE_SERVERS"),
    acks='all',
    compression_type=None,  # Không nén tại Kafka để tránh lỗi nén
    batch_size=32 * 1024,
    linger_ms=10,
    buffer_memory=128 * 1024 * 1024,
    max_request_size=5 * 1024 * 1024,
)


# Function to send each extracted pickle file to Kafka
def send_pickle_to_kafka(data, pickle_index):
    try:
        # Serialize data to prepare it for sending
        serialized_data = pickle.dumps(data)
        producer.send(TOPIC_NAME, serialized_data)
        logging.info(f"Sent pickle {pickle_index} to topic: {TOPIC_NAME}")
    except Exception as e:
        logging.error(f"Error sending pickle {pickle_index}: {e}")


def extract_pickles_from_large_file(bucket_name_, file_name_):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name_)
    blob = bucket.blob(file_name_)
    pickles = []
    # Open the large file from GCS
    with blob.open('rb') as f:
        pickle_index = 0
        while True:
            try:
                # Load each pickle object from the file
                data = pickle.load(f)
                pickles.append((pickle_index, data))
                pickle_index += 1
                print(pickle_index)
            except EOFError:
                # End of file reached, break the loop
                break
            except Exception as e:
                logging.error(f"Error reading pickle {pickle_index}: {e}")
                break
    return pickles

# def stream_pickles_from_combined_file(bucket_name_: str, file_name_: str):
#     """Stream a combined file from GCS and load each pickle sequentially."""
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name_)
#     blob = bucket.blob(file_name_)
#     pickles = []
#     # Open the large file from GCS
#     with blob.open('rb') as f:
#         pickle_index = 0
#         while True:
#             try:
#                 # Load each pickle object from the file
#                 data = pickle.load(f)
#                 pickles.append((pickle_index, data))
#                 pickle_index += 1
#                 print(pickle_index)
#             except EOFError:
#                 # End of file reached, break the loop
#                 break
#             except Exception as e:
#                 logging.error(f"Error reading pickle {pickle_index}: {e}")
#                 break

def main():
    my_bucket_name = os.getenv('GCS_BUCKET1')
    filename = os.getenv('BIG_FILE1')
    max_worker = os.getenv('MAX_WORKERS')
    print('start stream')
    # Extract individual pickles from the large file
    pickles = extract_pickles_from_large_file(my_bucket_name, filename)

    # Use ThreadPoolExecutor to send each extracted pickle in a separate thread
    try:
        with ThreadPoolExecutor(max_workers=max_worker) as executor:
            futures = []
            for pickle_index, data in pickles:
                futures.append(executor.submit(send_pickle_to_kafka, data, pickle_index))

            for future in as_completed(futures):
                try:
                    future.result()  # Capture exceptions in threads
                except Exception as e:
                    logging.error(f"Exception in thread: {e}")

        producer.flush()
        logging.info("All pickles sent successfully.")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
