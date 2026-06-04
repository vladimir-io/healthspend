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
    parser.add_argument("--token", help="Hugging Face API token. If omitted, reads HF_TOKEN, HF, or HUGGINGFACE_CO_TOKEN from environment.")
    parser.add_argument(
        "--prefer-compressed",
        action="store_true",
        help="Upload .db.zst sibling when smaller than raw .db",
    )
    args = parser.parse_args()

    files: list[Path] = []
    for p in args.files:
        path = Path(p)
        if args.prefer_compressed:
            zst = path.with_suffix(path.suffix + ".zst")
            if zst.is_file() and zst.stat().st_size < path.stat().st_size:
                files.append(zst)
                continue
        files.append(path)
    for f in files:
        if not f.exists():
            raise SystemExit(f"Missing file: {f}")

    import os

    token = (
        args.token
        or os.environ.get("HF_TOKEN")
        or os.environ.get("HF")
        or os.environ.get("HUGGINGFACE_CO_TOKEN")
    )
    if not token:
        raise SystemExit("Missing Hugging Face token. Set HF, HF_TOKEN, or pass --token.")

    print(f"Connecting to Hugging Face dataset: {args.repo_id}")
    api = HfApi(token=token)
    
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
