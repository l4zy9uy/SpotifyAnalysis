import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import pickle
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType
from pyspark.sql.types import StringType, StructType, StructField

load_dotenv()

kafka_package = os.getenv('KAFKA_PACKAGE')

spark = SparkSession.builder \
    .appName('g1') \
    .config('spark.jars.packages', kafka_package) \
    .getOrCreate()

kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = 'my_topic'

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

# Extract the track_uri field from the parsed JSON
result_df = parsed_df.select(
    col("key").cast("string"),  # Cast the key as string
    col("parsed_json.track_uri")  # Access track_uri from deserialized_value
)

# Write the results to the console
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the stream to finish
query.awaitTermination()