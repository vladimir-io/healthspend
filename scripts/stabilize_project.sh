#!/usr/bin/env bash
# Return repo to a known-good state: reclaim disk, verify public DBs, refresh hot shard.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Disk cleanup"
AGGRESSIVE="${AGGRESSIVE:-1}" ./scripts/cleanup_disk.sh

echo "==> Apply MRF index (if present)"
if [[ -f data/mrf_url_index.sqlite ]]; then
  python3 scripts/apply_mrf_index.py || true
fi

echo "==> Verify published datasets"
python3 scripts/verify_dataset_artifacts.py

if [[ -f web/public/audit_data.db ]]; then
  echo "==> Refresh hot shard from full ledger"
  HS_SKIP_VACUUM=1 HS_SKIP_FTS=1 python3 scripts/build_hot_db.py
  python3 scripts/write_dataset_manifest.py
fi

echo "==> Stable checks"
python3 scripts/test_roi_improvements.py
sqlite3 web/public/audit_data.db "SELECT COUNT(*) AS prices FROM prices;" 2>/dev/null || true
sqlite3 scraper/compliance.db "SELECT COUNT(*) AS with_mrf FROM hospitals WHERE COALESCE(mrf_url,'')!='';" 2>/dev/null || echo "(no scraper compliance db)"

echo "✓ Project stable. Public site data: web/public/audit_data.db + audit_hot.db"
echo "  To re-ingest MRFs after aggressive cleanup: ./scripts/grow_mrf_coverage.sh"
