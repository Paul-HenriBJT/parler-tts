from google.oauth2 import service_account
import os
from google.cloud import storage, transfer_manager

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

def fetch_checkpoint_from_gcs(bucket_name, checkpoint_path, output_dir, key_file_path, workers=8):
    """
    Fetches a specified checkpoint from a Google Cloud Storage bucket and adds it to the output_dir folder,
    preserving the original folder structure. Uses transfer_manager for concurrent downloads.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        checkpoint_path (str): Path to the checkpoint in the GCS bucket.
        output_dir (str): Local directory to save the checkpoint.
        key_file_path (str): Path to the service account JSON key file.
        workers (int): Number of concurrent workers for downloads. Defaults to 8.
    """
    print(f"Fetching checkpoint from {bucket_name}/{checkpoint_path} with token {key_file_path}")
    print(f"Using key file: {key_file_path}")
    
    storage_client = storage.Client.from_service_account_json(key_file_path)
    bucket = storage_client.bucket(bucket_name)
    
    # List all blobs with the given prefix
    blobs = list(bucket.list_blobs(prefix=checkpoint_path))
    
    # Prepare the list of blob names to download
    blob_names = [blob.name for blob in blobs if not blob.name.endswith("/")]
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Download blobs concurrently
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=output_dir, max_workers=workers
    )

    # Process results
    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            print(f"Failed to download {name} due to exception: {result}")
        else:
            local_path = os.path.join(output_dir, name)
            print(f"Downloaded: {local_path}")

    print(f"Checkpoint downloaded to {output_dir}")