import os
from huggingface_hub import HfApi, hf_hub_download


def fetch_checkpoint_from_huggingface(repo_id, checkpoint_path, output_dir, token=None):
    """
    Fetches a specified checkpoint from a Hugging Face repository and adds it to the output_dir folder,
    preserving the original folder structure.
    
    Args:
        repo_id (str): The Hugging Face repository ID (e.g., "username/model-name").
        checkpoint_path (str): Path to the checkpoint in the Hugging Face repository.
        output_dir (str): Local directory to save the checkpoint.
        token (str, optional): Hugging Face API token for private repositories.
    """
    print(f"Fetching checkpoint from {repo_id}/{checkpoint_path}")
    
    api = HfApi()
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # List all files in the repository
        files = api.list_repo_files(repo_id, repo_type="model", token=token)
        
        # Filter files that are under the checkpoint_path
        checkpoint_files = [f for f in files if f.startswith(checkpoint_path)]
        
        for file in checkpoint_files:
            # Download each file
            local_path = os.path.join(output_dir, file)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            hf_hub_download(
                repo_id=repo_id,
                filename=file,
                local_dir=output_dir,
                token=token
            )
            print(f"Downloaded: {local_path}")
        
        print(f"Checkpoint downloaded to {output_dir}")
    
    except Exception as e:
        print(f"An error occurred while fetching the checkpoint: {str(e)}")
