#!/usr/bin/env python3
"""Prepare web/public DB artifacts for upload (optional zstd compression)."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def maybe_compress(path: Path) -> Path | None:
    zst = shutil.which("zstd")
    if not zst:
        return None
    out = path.with_suffix(path.suffix + ".zst")
    subprocess.run([zst, "-T0", "-f", "-q", str(path), "-o", str(out)], check=True)
    if out.exists() and out.stat().st_size < path.stat().st_size * 0.85:
        return out
    out.unlink(missing_ok=True)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare deploy artifacts")
    parser.add_argument("--public-dir", default="web/public")
    parser.add_argument("--compress", action="store_true")
    args = parser.parse_args()

    pub = Path(args.public_dir)
    manifest_path = pub / "dataset_manifest.json"
    artifacts: dict[str, object] = {}

    for name in ("audit_data.db", "audit_hot.db"):
        path = pub / name
        if not path.is_file():
            continue
        upload = path
        if args.compress:
            compressed = maybe_compress(path)
            if compressed:
                upload = compressed
                print(f"  compressed {name}: {path.stat().st_size:,} → {upload.stat().st_size:,}")
        conn = sqlite3.connect(path)
        prices = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        cpts = conn.execute(
            "SELECT COUNT(DISTINCT cpt_code) FROM prices WHERE cpt_code IS NOT NULL AND cpt_code != ''"
        ).fetchone()[0]
        conn.close()
        artifacts[name] = {
            "upload_file": upload.name,
            "size_bytes": upload.stat().st_size,
            "price_rows": prices,
            "distinct_cpt_codes": cpts,
            "compressed": upload.suffix == ".zst",
        }

    meta = {
        "prepared_at": datetime.now(timezone.utc).isoformat(),
        "artifacts": artifacts,
    }
    out_meta = pub / "deploy_artifacts.json"
    out_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"✓ Wrote {out_meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
