from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
import os
from dotenv import load_dotenv

load_dotenv()

kafka_package = os.getenv('KAFKA_PACKAGE')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName('SpotifyTrackProcessor') \
    .config('spark.jars.packages', kafka_package) \
    .config('spark.executor.memory', '4g') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.json.keyfile", os.getenv("JSON_KEYFILE"))


# Define Schema for Required Fields
track_schema = StructType([
    StructField("album", StructType([
        StructField("uri", StringType(), True),
        StructField("artists", ArrayType(StructType([
            StructField("uri", StringType(), True)
        ])), True)
    ]), True),
    StructField("artists", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("uri", StringType(), True)
    ])), True),
    StructField("available_markets", ArrayType(StringType()), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("uri", StringType(), True)
])

# Read the JSON file
gcs_input_path = "gs://don-result-csv/*.json"

# Read the JSON file from GCS into a Spark DataFrame
df = spark.read \
    .schema(track_schema) \
    .json(gcs_input_path, multiLine=False)

print("count: ", df.count())


# Combine all track artists and album artists into single columns
processed_df = df.select(
    col("name").alias("track_name"),
    col("uri").alias("track_uri"),
    col("popularity"),
    col("duration_ms"),
    col("album.uri").alias("album_uri"),
    concat_ws(", ", col("album.artists.uri")).alias("album_artist_uris"),  # Combine all album artists URIs
    concat_ws(", ", col("artists.name")).alias("artist_names"),            # Combine all track artist names
    concat_ws(", ", col("artists.uri")).alias("artist_uris"),              # Combine all track artist URIs
    concat_ws(", ", col("available_markets")).alias("available_markets")   # Convert array to string
)

# Write the processed data to a CSV file
output_csv_path = "gs://don-result-csv/processed_spotify_tracks/"
processed_df.coalesce(1).write \
    .mode('append') \
    .option('header', 'true') \
    .csv(output_csv_path)

print(f"Processed data written to {output_csv_path}")
