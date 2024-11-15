import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import pickle
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField

load_dotenv()

kafka_package = os.getenv('KAFKA_PACKAGE')

spark = SparkSession.builder \
    .appName('export_track_ids') \
    .config('spark.jars.packages', kafka_package) \
    .config('spark.executor.memory', '4g') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.json.keyfile", "bigdata-class-438306-d7222b6dac49.json")

kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_REMOTE_SERVERS")
kafka_topic = os.getenv("TOPIC_NAME")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

def deserialize_pickle(pickled_bytes):
    try:
        # Deserialize the byte array
        return str(pickle.loads(pickled_bytes))
    except Exception as e:
        return str(e)

# Register the UDF to deserialize pickled data from Kafka's value field
pickle_deserializer_udf = udf(deserialize_pickle, StringType())

# Apply the deserialization to the 'value' column (binary) which contains the pickled data
deserialized_df = kafka_df.withColumn("deserialized_value", pickle_deserializer_udf(col("value")))

# Define a schema for the deserialized value (assuming it’s in JSON or dictionary format)
json_schema = StructType([
    StructField("track_uri", StringType(), True),
    StructField("other_field", StringType(), True)  # Add other fields as needed
])

# Parse the deserialized JSON string into a structured column using from_json
parsed_df = deserialized_df.withColumn("parsed_json", from_json(col("deserialized_value"), json_schema))

result_df = parsed_df.select(
    col("parsed_json.track_uri")  # Access track_uri from deserialized_value
)

# Write the results to the console
def write_batch_to_files(batch_df, batch_id):
    # Skip empty batches
    if batch_df.isEmpty():
        return

    # Coalesce to a single partition for a single file output
    batch_df = batch_df.coalesce(1)

    # Write to a specific file
    file_path = f"{os.getenv('GCS_BUCKET2_PATH')}/track_uris.txt"
    batch_df.write.mode("append").text(file_path)

# Use foreachBatch to process each micro-batch and apply custom logic
query = result_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_to_files) \
    .trigger(processingTime="1800 seconds") \
    .start()

# Wait for the stream to finish
query.awaitTermination()
