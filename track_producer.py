import os
import json
import time
from itertools import cycle

from google.cloud import storage
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from spotipy.exceptions import SpotifyException

# Load environment variables
load_dotenv()

# Spotify credentials: List of client ID and client secret pairs
CLIENT_CREDENTIALS = [
    {"client_id": "22b4e97088cc4a0ababfa24df4362822", "client_secret": "89e90fcc9b044661bed5cc2124ce3288"},
    {"client_id": "9e05f5ddf55f4e2e94dcf01417dcf056", "client_secret": "9bf9f468b06446b785f9e10b55649824"},
    {"client_id": "be722a8be96e47bab585a2a630cc7dc4", "client_secret": "3fac8ddf4d2749029c1c498b915398d2"},
    {"client_id": "c9d456c804eb454da26c4d6ce1d79c80", "client_secret": "725483649b18485eb2d7db83acf1de7e"},
    {"client_id": "c3900abc57b04c33b3ff75bd2e493a29", "client_secret": "709daaa62c2b408e9af403726d3463bb"}
]

# Initialize credential rotation
credential_cycle = cycle(CLIENT_CREDENTIALS)


def get_spotify_client():
    """
    Returns a Spotify client using the next available credentials in the cycle.
    """
    credentials = next(credential_cycle)
    client_credentials_manager = SpotifyClientCredentials(
        client_id=credentials["client_id"],
        client_secret=credentials["client_secret"]
    )
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)


# Initialize GCS Client
storage_client = storage.Client()


def is_valid_uri(uri):
    """
    Validates whether a given URI is a valid Spotify track URI.
    """
    return uri.startswith("spotify:track:") and len(uri.split(":")) == 3


def read_files_from_multiple_buckets(buckets, folder_name):
    """
    Reads .txt files from specified folders in multiple GCS buckets, validates URIs,
    and yields only valid Spotify track URIs.
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


def process_and_save_to_gcs_stream(bucket_name, folder_name, output_bucket, output_file_name):
    """
    Reads track URIs directly from multiple GCS buckets and folders, retrieves their details via Spotify API,
    and writes the track data to a JSONL file on GCS in a streaming fashion.
    """
    all_track_uris = set()

    # Read URIs from the GCS bucket
    for uri in read_files_from_multiple_buckets([bucket_name], folder_name):
        if uri:  # Skip empty lines
            all_track_uris.add(uri)  # Add URI to the set (avoiding duplicates)

    # Convert set to list for processing
    all_track_uris = list(all_track_uris)
    print(f"Read successfully from bucket {bucket_name}")

    # Get the output GCS bucket and blob
    bucket = storage_client.bucket(output_bucket)
    blob = bucket.blob(output_file_name)

    # Initialize a streaming upload to GCS
    with blob.open("w", content_type="application/json") as output_file:
        spotify_client = get_spotify_client()  # Get the initial Spotify client
        count = 0

        for chunk in chunk_list(all_track_uris, 50):
            print("Sending API request...")
            try:
                tracks = spotify_client.tracks(chunk)
            except SpotifyException as e:
                if e.http_status == 429:  # Rate limit error
                    retry_after = int(e.headers.get("Retry-After", 1))
                    print(f"Rate limit hit. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    spotify_client = get_spotify_client()  # Rotate credentials
                    continue  # Retry the same chunk
                elif e.http_status == 403:  # Forbidden error
                    print(f"403 Forbidden: Retrying the same chunk after 30 seconds...")
                    time.sleep(30)
                    spotify_client = get_spotify_client()  # Rotate credentials
                    continue  # Retry the same chunk
                else:
                    print(f"Unexpected error: {e}")
                    raise e
            for track in tracks['tracks']:
                if track:  # Ensure track is not None
                    json_line = json.dumps(track, ensure_ascii=False)
                    output_file.write(json_line + "\n")  # Write each track as a line
                    print(f"{count} Fetched and written: {track['name']} - {track['uri']}")
                    count += 1
                    if count % 10000 == 0:
                        print(f"Processed {count} records. Pausing for 30 seconds...")
                        time.sleep(30)  # Pause after every 10,000 records


if __name__ == '__main__':
    # List of GCS buckets to fetch files from
    gcs_buckets = [
        os.getenv('GCS_BUCKET2'),
        os.getenv('GCS_BUCKET3'),
        os.getenv('GCS_BUCKET4'),
        os.getenv('GCS_BUCKET5'),
        os.getenv('GCS_BUCKET6'),
    ]

    # Folder inside each bucket containing the input files
    input_folder = 'track_uris'

    # Output GCS bucket and file name
    output_bucket = os.getenv('OUTPUT_GCS_BUCKET')

    print("Starting processing of GCS buckets...")

    for idx, bucket_name in enumerate(gcs_buckets, 1):
        output_file_name = f'spotify_tracks_bucket_{idx}.json'
        print(f"Processing bucket {idx}: {bucket_name}")
        process_and_save_to_gcs_stream(bucket_name, input_folder, output_bucket, output_file_name)
        print(f"Finished processing bucket {idx}: {bucket_name}")
