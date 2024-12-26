from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, to_date
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

# Input CSV file path on GCS
input_csv_path = "gs://don-result-csv/processed_spotify_tracks/part-00000-b8035722-6ef0-44a3-8570-9eaa159a21ce-c000.csv"

# Read the CSV file into a Spark DataFrame
df = spark.read \
    .option("header", "true") \
    .csv(input_csv_path)

# Query 1: Get all unique artist URIs
artist_uris_df = df.select("artist_uris").distinct()

# Query 2: Get all unique album URIs
album_uris_df = df.select("album_uri").distinct()

# Assuming there is a column 'release_date' in 'YYYY-MM-DD' format for the next query
if "release_date" in df.columns:
    # Query 3: Count the number of tracks released each year
    df_with_year = df.withColumn("release_year", year(to_date(col("release_date"), "yyyy-MM-dd")))
    tracks_per_year_df = df_with_year.groupBy("release_year").agg(count("*").alias("track_count")).orderBy("release_year")
else:
    print("Column 'release_date' not found in the dataset. Skipping yearly track count query.")

# Output CSV paths
output_artist_uris = "gs://don-result-csv/processed_results/unique_artist_uris.csv"
output_album_uris = "gs://don-result-csv/processed_results/unique_album_uris.csv"
output_tracks_per_year = "gs://don-result-csv/processed_results/tracks_per_year.csv"

# Write query results back to GCS as CSV
artist_uris_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_artist_uris)
album_uris_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_album_uris)

if "release_date" in df.columns:
    tracks_per_year_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_tracks_per_year)

print("Results written to GCS:")
print(f"Artist URIs: {output_artist_uris}")
print(f"Album URIs: {output_album_uris}")
if "release_date" in df.columns:
    print(f"Tracks Per Year: {output_tracks_per_year}")
