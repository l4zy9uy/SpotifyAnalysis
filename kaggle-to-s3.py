import boto3
from kaggle.api.kaggle_api_extended import KaggleApi
import os
from dotenv import load_dotenv

load_dotenv()

api = KaggleApi()
api.authenticate()

s3 = boto3.client('s3', region_name='ap-southeast-1')

owner_slug = os.getenv('OWNER_SLUG')
dataset_slug = os.getenv('DATASET_SLUG')
s3_bucket_name = os.getenv('S3_BUCKET')

response = api.datasets_list_files(owner_slug, dataset_slug, page_size=1000)
files = response['datasetFiles']

download_path = os.getcwd()

for file in files:
    full_file_path = file['name']  # Get the full file path from the dataset
    file_name = os.path.basename(full_file_path)  # Extract only the file name
    print(f"Downloading {file_name}...")

    # Download the file from Kaggle
    api.dataset_download_file(f'{owner_slug}/{dataset_slug}', full_file_path, path=download_path, quiet=True,
                              force=True)

    # Get the full path of the downloaded file
    file_path = os.path.join(download_path, file_name)

    # Ensure file exists before attempting to upload
    if os.path.exists(file_path):
        # Upload the file to S3
        s3_key = f"kaggle/{file_name}"  # Define the S3 object key (path in S3 bucket)
        print(f"Uploading {file_name} to S3 bucket: {s3_bucket_name}")

        with open(file_path, 'rb') as file_data:
            s3.upload_fileobj(file_data, s3_bucket_name, s3_key)

        print(f"Uploaded {file_name} successfully!\n")
        try:
            os.remove(file_path)
            print(f"Deleted {file_name} from local storage.")
        except OSError as e:
            print(f"Error deleting {file_name}: {e}")
    else:
        print(f"File {file_name} does not exist, skipping.")

print("All files uploaded successfully.")
