#!/usr/bin/env bash
# Build web/public/audit_data.db + audit_hot.db from CMS ingest + MRF parsing.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STATE="${1:-}"
MRF_LIMIT="${MRF_LIMIT:-0}"
export HS_FULL_CPT_COVERAGE="${HS_FULL_CPT_COVERAGE:-1}"

echo "==> CMS hospital + compliance baseline (ingest.py)"
python3 ingest.py

echo "==> Discovery inputs (CMS enrollments crosswalk)"
python3 scripts/fetch_discovery_inputs.py

INDEX="${MRF_INDEX:-data/mrf_url_index.sqlite}"
if [[ ! -f "$INDEX" ]]; then
  echo "==> Build MRF URL index (health-system cms-hpt harvest)"
  python3 scripts/build_mrf_index.py --out "$INDEX"
fi

if [[ -x "$(command -v cargo)" ]]; then
  echo "==> Scraper discovery + audit${STATE:+ ($STATE)}"
  (
    cd scraper
    if [[ -n "$STATE" ]]; then
      cargo run --release -- --discover-only --state "$STATE"
      cargo run --release -- --audit-only --state "$STATE"
    else
      cargo run --release -- --discover-only
      cargo run --release -- --audit-only
    fi
  )

  echo "==> Apply MRF URL index"
  python3 scripts/apply_mrf_index.py --index "$INDEX" ${STATE:+--state "$STATE"}

  echo "==> Domain/cms-hpt enrichment (fallback)${STATE:+ ($STATE)}"
  ENRICH_ARGS=(python3 scripts/enrich_hospital_urls.py --compliance-db scraper/compliance.db)
  if [[ -n "$STATE" ]]; then
    ENRICH_ARGS+=(--state "$STATE")
  fi
  if [[ "$MRF_LIMIT" -gt 0 ]]; then
    ENRICH_ARGS+=(--limit "$((MRF_LIMIT * 4))")
  fi
  "${ENRICH_ARGS[@]}" || true

  MRF_DIR="${MRF_DIR:-/tmp/healthspend-mrf}"
  mkdir -p "$MRF_DIR"
  echo "==> MRF download + ingest (HS_FULL_CPT_COVERAGE=$HS_FULL_CPT_COVERAGE)"
  MRF_ARGS=(
    python3 scripts/mrf_streamer.py
    --prices-db scraper/prices.db
    --compliance-db scraper/compliance.db
    --mrf-dir "$MRF_DIR"
  )
  if [[ -n "$STATE" ]]; then
    MRF_ARGS+=(--state "$STATE")
  fi
  if [[ "$MRF_LIMIT" -gt 0 ]]; then
    MRF_ARGS+=(--limit "$MRF_LIMIT")
  fi
  "${MRF_ARGS[@]}"
else
  echo "!! cargo not found — skipping MRF ingest; only CMS baseline will be present"
fi

echo "==> Merge scraper → audit_data.db"
python3 scripts/merge_pipeline_data.py

echo "==> Hot shard + manifest"
python3 scripts/build_hot_db.py
python3 scripts/write_dataset_manifest.py
python3 scripts/verify_dataset_artifacts.py

echo "==> Done. Artifacts in web/public/"
