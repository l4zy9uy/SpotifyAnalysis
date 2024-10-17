import os
from kafka import KafkaProducer
import s3fs
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize S3 filesystem object
fs = s3fs.S3FileSystem(anon=False)

# S3 bucket and directory path
s3_bucket = os.getenv("S3_BUCKET")
s3_directory = os.getenv("S3_RAW_DATA_DIR")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    compression_type="gzip",
    acks=1,
    linger_ms=5,
    batch_size=163840,
)

# List all files in the S3 directory
files = fs.ls(f'{s3_bucket}/{s3_directory}')

# Loop through all the files in the directory
for s3_file_path in files:
    print(f"Reading file: {s3_file_path}")

    with fs.open(s3_file_path, 'rb') as s3_file:
        raw_data = s3_file.read()

    producer.send('my_topic', value=raw_data)

# Flush and close the producer
producer.flush()
producer.close()

print(f"All files processed and sent to Kafka successfully.")
