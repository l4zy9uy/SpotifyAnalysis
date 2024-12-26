from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, explode, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables
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

# Define schema for the dataset
schema = StructType([
    StructField("track_name", StringType(), True),
    StructField("artist_names", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("album_uri", StringType(), True),
    StructField("track_uri", StringType(), True)
])

# Load the processed Spotify dataset with schema
input_csv_path = "gs://don-result-csv/processed_spotify_tracks/part-00000-91c9c5f5-ee85-43b7-9955-9a7b04eb9ab5-c000.csv"
df = spark.read.option("header", "true").schema(schema).csv(input_csv_path)

# Validate the schema and data
df.printSchema()
df.show(10)

# Check for non-numeric values in the popularity column
non_numeric_popularity = df.filter(~col("popularity").cast("int").isNotNull())
if non_numeric_popularity.count() > 0:
    print("Non-numeric values found in popularity column:")
    non_numeric_popularity.show()
else:
    print("No non-numeric values found in popularity column.")

# 1. KPI Metrics
# Split artist_names into individual artists and count distinct artists
df_with_artists = df.withColumn("artist_name", explode(split(col("artist_names"), ", ")))
kpi_metrics_df = df_with_artists.select(
    count("track_name").alias("total_tracks"),
    countDistinct("album_uri").alias("total_albums"),
    countDistinct("artist_name").alias("total_artists")
)
kpi_metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").option("quote", '"').option("escape", '"').csv("gs://don-result-csv/visualizations/kpi_metrics.csv")

# 2. Sort tracks by popularity
popularity_by_track_df = df.select("track_name", "popularity").orderBy(col("popularity").desc())
popularity_by_track_df.coalesce(1).write.mode("overwrite").option("header", "true").option("quote", '"').option("escape", '"').csv("gs://don-result-csv/visualizations/popularity_by_track.csv")

# Display sorted tracks
popularity_by_track_df.show(1000)

print("Selected visualizations' data saved to GCS.")
