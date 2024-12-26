from google.cloud import storage
from dotenv import load_dotenv
from google.api_core.retry import Retry
import pickle

load_dotenv()

def stream_pickles_from_combined_file(bucket_name_: str, file_name_: str):
    """Stream a combined file from GCS and load each pickle sequentially."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name_)
    blob = bucket.blob(file_name_)

    # Open the large file from GCS
    with blob.open('rb') as f:
        print(f"Reading and extracting pickles from {file_name_}...")
        count = 0
        try:
            while True:
                # Load one pickle object at a time from the stream
                obj = pickle.load(f)
                print(f"Loaded object: {type(obj)}")  # Example: Print the object type

                # Process the object (store it, analyze it, etc.)
                # Example: Just print the size of the object
                print(f"Object size: {len(str(obj))} bytes")
                count+=1
                print(count)

        except EOFError:
            # End of file reached
            print("All pickles have been read from the combined file.")

def upload_blob(bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: Ensure the file is uploaded only if it doesn't already exist
    blob.upload_from_filename(source_file_name, retry=Retry(), timeout=300)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

if __name__ == '__main__':

    my_bucket_name = 'spotify-tracks-data'
    folder_name = 'my-folder'
    file = 'test.txt'
    des_file = f'{folder_name}/uploaded.txt'
    print('start stream')
    stream_pickles_from_combined_file(my_bucket_name, 'archive')
