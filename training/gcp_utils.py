from google.cloud import storage
from google.oauth2 import service_account
import os

def get_storage_client(key_file_path):
    """
    Creates and returns a Google Cloud Storage client using a service account JSON key file.
    
    Args:
        key_file_path (str): Path to the service account JSON key file.
    
    Returns:
        storage.Client: Authenticated GCS client.
    """
    if key_file_path and os.path.exists(key_file_path):
        credentials = service_account.Credentials.from_service_account_file(key_file_path)
        return storage.Client(credentials=credentials)
    else:
        # Fall back to default credentials if no key file is provided or file doesn't exist
        return storage.Client()

def upload_checkpoint_to_gcs(bucket_name, checkpoint_path, local_dir, key_file_path):
    """
    Uploads a local checkpoint directory to a Google Cloud Storage bucket using a service account JSON key file.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        checkpoint_path (str): Path in the bucket where the checkpoint should be uploaded.
        local_dir (str): Local directory containing the checkpoint files.
        key_file_path (str): Path to the service account JSON key file.
    """
    client = get_storage_client(key_file_path)
    bucket = client.bucket(bucket_name)

    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            blob_path = os.path.join(checkpoint_path, relative_path)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
    
    print(f"Checkpoint uploaded to gs://{bucket_name}/{checkpoint_path}")

def fetch_checkpoint_from_gcs(bucket_name, checkpoint_path, output_dir, key_file_path):
    """
    Fetches a specified checkpoint from a Google Cloud Storage bucket and saves it in a subfolder
    named after the last part of the checkpoint path within the output_dir.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        checkpoint_path (str): Path to the checkpoint in the GCS bucket.
        output_dir (str): Local directory to save the checkpoint.
        key_file_path (str): Path to the service account JSON key file.
    """
    print(f"Using key file: {key_file_path}")
    client = get_storage_client(key_file_path)
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=checkpoint_path)

    # Extract the last part of the checkpoint path to use as the subfolder name
    subfolder_name = checkpoint_path.split('/')[-1]
    checkpoint_output_dir = os.path.join(output_dir, subfolder_name)

    # Ensure the checkpoint output directory exists
    os.makedirs(checkpoint_output_dir, exist_ok=True)

    for blob in blobs:
        # Skip the directory itself
        if blob.name.endswith('/'):
            continue
        
        # Get the filename without the full path
        filename = os.path.basename(blob.name)
        
        # Set the local file path
        local_file_path = os.path.join(checkpoint_output_dir, filename)
        
        # Download the file
        blob.download_to_filename(local_file_path)
        print(f"Downloaded: {local_file_path}")

    print(f"Checkpoint downloaded to {checkpoint_output_dir}")