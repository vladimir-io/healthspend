#!/usr/bin/env python3
"""
Publish the Healthspend audit database directly to the Hugging Face dataset.
"""

import argparse
import sys
from pathlib import Path

try:
    from huggingface_hub import HfApi
except ImportError:
    print("Error: huggingface_hub is required. Install with `pip install huggingface_hub`")
    sys.exit(1)

def main() -> int:
    parser = argparse.ArgumentParser(description="Publish artifacts to Hugging Face Datasets")
    parser.add_argument("--repo-id", required=True, help="Hugging Face repo ID (e.g., vladimir-io/healthspend-data)")
    parser.add_argument("--files", nargs="+", required=True, help="Artifact files to publish")
    parser.add_argument("--token", help="Hugging Face API token. If omitted, expects HUGGINGFACE_CO_TOKEN or HF_TOKEN in environment.")
    args = parser.parse_args()

    files = [Path(p) for p in args.files]
    for f in files:
        if not f.exists():
            raise SystemExit(f"Missing file: {f}")

    print(f"Connecting to Hugging Face dataset: {args.repo_id}")
    api = HfApi(token=args.token)
    
    for f in files:
        print(f"Uploading {f.name} ({f.stat().st_size} bytes)...")
        api.upload_file(
            path_or_fileobj=str(f),
            path_in_repo=f.name,
            repo_id=args.repo_id,
            repo_type="dataset",
        )
        print(f"Successfully uploaded {f.name} to {args.repo_id}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
