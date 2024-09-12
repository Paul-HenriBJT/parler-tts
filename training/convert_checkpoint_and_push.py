import os
import argparse
import torch
from transformers import AutoFeatureExtractor, AutoTokenizer
from parler_tts import ParlerTTSConfig, ParlerTTSForConditionalGeneration
from accelerate import Accelerator
from huggingface_hub import HfApi, login

def convert_checkpoint_to_model_and_push(original_model_path, checkpoint_path, output_path, repo_id, token):
    # Initialize accelerator
    accelerator = Accelerator()

    # Load the original model configuration
    config = ParlerTTSConfig.from_pretrained(original_model_path)

    # Create model with original configuration
    model = ParlerTTSForConditionalGeneration.from_pretrained(
        original_model_path,
        config=config,
    )

    # Prepare the model
    model = accelerator.prepare(model)

    # Load the checkpoint
    print(f"Loading checkpoint from {checkpoint_path}")
    accelerator.load_state(checkpoint_path)

    # Unwrap the model if it's wrapped by accelerator
    unwrapped_model = accelerator.unwrap_model(model)

    # Save the model
    unwrapped_model.save_pretrained(output_path)

    # Load and save tokenizers and feature extractor
    prompt_tokenizer = AutoTokenizer.from_pretrained(original_model_path)
    description_tokenizer = AutoTokenizer.from_pretrained(original_model_path)
    feature_extractor = AutoFeatureExtractor.from_pretrained(original_model_path)

    prompt_tokenizer.save_pretrained(output_path)
    description_tokenizer.save_pretrained(output_path)
    feature_extractor.save_pretrained(output_path)

    # Save the configuration
    config.save_pretrained(output_path)

    print(f"Model, tokenizers, feature extractor, and config saved to {output_path}")

    # Push to Hub
    login(token=token)
    api = HfApi()

    api.create_repo(repo_id=repo_id, exist_ok=True)
    api.upload_folder(
        folder_path=output_path,
        repo_id=repo_id,
        repo_type="model",
    )

    print(f"Model pushed to the Hub: https://huggingface.co/{repo_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a checkpoint to a usable model and push to Hub")
    parser.add_argument("--original_model_path", type=str, required=True, help="Path to the original model")
    parser.add_argument("--checkpoint_path", type=str, required=True, help="Path to the checkpoint")
    parser.add_argument("--output_path", type=str, required=True, help="Path to save the converted model locally")
    parser.add_argument("--repo_id", type=str, required=True, help="Hugging Face Hub repository ID to push the model to")
    parser.add_argument("--token", type=str, required=True, help="Hugging Face access token")

    args = parser.parse_args()

    convert_checkpoint_to_model_and_push(
        args.original_model_path, 
        args.checkpoint_path, 
        args.output_path, 
        args.repo_id, 
        args.token
    )