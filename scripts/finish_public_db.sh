#!/usr/bin/env bash
# Publish scraper/prices.db → web/public without re-running discovery or MRF ingest.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export HS_SKIP_FTS="${HS_SKIP_FTS:-1}"
export HS_SKIP_VACUUM="${HS_SKIP_VACUUM:-1}"

echo "==> Apply MRF index (if present)"
if [[ -f data/mrf_url_index.sqlite ]]; then
  python3 scripts/apply_mrf_index.py
fi

echo "==> Merge pipeline → audit_data.db"
python3 scripts/merge_pipeline_data.py

echo "==> Hot shard + manifest + verify"
python3 scripts/build_hot_db.py
python3 scripts/write_dataset_manifest.py
python3 scripts/verify_dataset_artifacts.py

echo "==> Public DBs ready in web/public/"
