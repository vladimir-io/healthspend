#!/usr/bin/env python3
"""
Publish hot SQLite artifacts to Cloudflare R2.

Example:
python3 scripts/deploy_artifacts.py \
  --bucket healthspend-hot \
  --prefix v1 \
  --files web/public/audit_data.db web/public/metrics.db
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import List


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def run(cmd: List[str], dry_run: bool) -> None:
    print("$ " + " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish hot SQLite artifacts to Cloudflare R2")
    parser.add_argument("--bucket", required=True, help="R2 bucket name")
    parser.add_argument("--prefix", default="hot", help="Object key prefix")
    parser.add_argument("--files", nargs="+", required=True, help="Artifact files to publish")
    parser.add_argument("--wrangler", default="wrangler", help="Wrangler CLI command")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--manifest", default="web/public/hot-artifacts-manifest.json", help="Manifest output path")
    args = parser.parse_args()

    files = [Path(p) for p in args.files]
    for f in files:
        if not f.exists():
            raise SystemExit(f"Missing file: {f}")

    published = []
    for f in files:
        digest = sha256(f)
        key = f"{args.prefix}/{f.name}"
        versioned_key = f"{args.prefix}/{f.stem}-{digest[:12]}{f.suffix}"

        run(
            [args.wrangler, "r2", "object", "put", f"{args.bucket}/{versioned_key}", "--file", str(f)],
            args.dry_run,
        )
        run(
            [args.wrangler, "r2", "object", "put", f"{args.bucket}/{key}", "--file", str(f)],
            args.dry_run,
        )

        published.append(
            {
                "file": str(f),
                "key": key,
                "versioned_key": versioned_key,
                "sha256": digest,
                "size_bytes": f.stat().st_size,
            }
        )

    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "bucket": args.bucket,
        "prefix": args.prefix,
        "objects": published,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
