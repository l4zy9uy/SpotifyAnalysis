import os
import json
import time

from google.cloud import storage
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from spotipy.exceptions import SpotifyException

# Load environment variables
load_dotenv()
cid = os.getenv('SPOTIFY_CLIENT_ID')
secret = os.getenv('SPOTIFY_CLIENT_SECRET')

# Spotify API authentication
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
sp.search(q="test", type="track")  # This should not raise an error

# Initialize GCS Client
storage_client = storage.Client()


def is_valid_uri(uri):
    """
    Validates whether a given URI is a valid Spotify track URI.

    Args:
        uri (str): The URI to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    return uri.startswith("spotify:track:") and len(uri.split(":")) == 3


def read_files_from_multiple_buckets(buckets, folder_name):
    """
    Reads .txt files from specified folders in multiple GCS buckets, validates URIs,
    and yields only valid Spotify track URIs.

    Args:
        buckets (list): List of GCS bucket names.
        folder_name (str): Folder name inside each bucket.

    Yields:
        str: Valid Spotify track URIs from all .txt files in the specified buckets and folders.
    """
    for bucket_name in buckets:
        try:
            bucket = storage_client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=folder_name)  # List objects in the specified folder
            for blob in blobs:
                if blob.name.startswith(f"{folder_name}/part") and blob.name.endswith(".txt"):
                    print(f"Reading file {blob.name} from bucket {bucket_name}")
                    data = blob.download_as_text()  # Read file content directly
                    for line in data.splitlines():
                        uri = line.strip()
                        if is_valid_uri(uri):
                            yield uri
                        else:
                            print(f"Invalid URI skipped: {uri}")
        except Exception as e:
            print(f"Error reading files from bucket {bucket_name}, folder {folder_name}: {e}")
        time.sleep(3)


def chunk_list(data, chunk_size):
    """
    Splits a list into smaller chunks of specified size.
    """
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def process_and_save_to_gcs_stream(buckets, folder_name, output_bucket, output_file_name):
    """
    Reads track URIs directly from multiple GCS buckets and folders, retrieves their details via Spotify API,
    and writes the track data to a JSONL file on GCS in a streaming fashion.

    Args:
        buckets (list): List of GCS bucket names to read input files from.
        folder_name (str): Folder name inside each bucket to filter files.
        output_bucket (str): GCS bucket name to save the output file.
        output_file_name (str): Name of the output JSON file to save on GCS.
    """
    all_track_uris = set()

    # Read URIs from multiple GCS buckets and folders
    for uri in read_files_from_multiple_buckets(buckets, folder_name):
        if uri:  # Skip empty lines
            all_track_uris.add(uri)  # Add URI to the set (avoiding duplicates)

    # Convert set to list for processing
    all_track_uris = list(all_track_uris)
    print("Read successfully")
    # Get the output GCS bucket and blob
    bucket = storage_client.bucket(output_bucket)
    blob = bucket.blob(output_file_name)

    # Initialize a streaming upload to GCS
    with blob.open("w", content_type="application/json") as output_file:
        # Process URIs in chunks of 50
        count = 0
        for chunk in chunk_list(all_track_uris, 50):
            try:
                # Fetch track details for up to 50 URIs
                print("Send api....")
                try:
                    tracks = sp.tracks(chunk)
                except SpotifyException as e:
                    if e.http_status == 429:  # Rate limit error
                        retry_after = int(e.headers.get("Retry-After", 1))
                        print(f"Rate limit hit. Retrying after {retry_after} seconds...")
                        time.sleep(retry_after)
                for track in tracks['tracks']:
                    if track:  # Ensure track is not None
                        json_line = json.dumps(track, ensure_ascii=False)
                        output_file.write(json_line + "\n")  # Write each track as a line
                        print(f"{count} Fetched and written: {track['name']} - {track['uri']}")
                        count += 1
            except Exception as e:
                print(f"Error processing chunk: {chunk}, Error: {e}")
        time.sleep(0.5)
        if count % 40000 == 0:
            print("done 1 part")
            time.sleep(120)
    print(f"JSONL file successfully written to GCS: gs://{output_bucket}/{output_file_name}")


if __name__ == '__main__':
    # List of GCS buckets to fetch files from
    gcs_buckets = [os.getenv('GCS_BUCKET1'), os.getenv('GCS_BUCKET2'), os.getenv('GCS_BUCKET3'),
                   os.getenv('GCS_BUCKET4'), os.getenv('GCS_BUCKET5'), os.getenv('GCS_BUCKET6')]

    # Folder inside each bucket containing the input files
    input_folder = 'track_uris'

    # Output GCS bucket and file name
    output_bucket = os.getenv('OUTPUT_GCS_BUCKET')
    output_file_name = 'spotify_tracks.json'

    print("Fetching track URIs directly from multiple GCS buckets and folders...")
    process_and_save_to_gcs_stream(gcs_buckets, input_folder, output_bucket, output_file_name)
    print("Processing finished successfully.")
