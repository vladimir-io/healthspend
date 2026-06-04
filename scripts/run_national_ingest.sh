#!/usr/bin/env bash
# National ingest: build MRF URL index → apply → ingest → publish DBs.
# Run on a machine with hours of wall time and stable network (not a 30-min CI matrix).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export HS_FULL_CPT_COVERAGE="${HS_FULL_CPT_COVERAGE:-1}"
INDEX="${MRF_INDEX:-data/mrf_url_index.sqlite}"
MRF_DIR="${MRF_DIR:-/tmp/healthspend-mrf-national}"

echo "==> Fetch CMS inputs"
python3 scripts/fetch_discovery_inputs.py

echo "==> Build national MRF URL index (health-system cms-hpt harvest)"
python3 scripts/build_mrf_index.py --out "$INDEX"

echo "==> CMS baseline"
python3 ingest.py

if [[ ! -x "$(command -v cargo)" ]]; then
  echo "cargo required for MRF ingest"
  exit 1
fi

echo "==> Discovery + audit (all states)"
(
  cd scraper
  cargo run --release --bin scraper -- --discover-only
  cargo run --release --bin scraper -- --audit-only
)

echo "==> Apply MRF index to compliance.db"
python3 scripts/apply_mrf_index.py --index "$INDEX"

echo "==> Fallback domain enrichment for hospitals still missing mrf_url"
python3 scripts/enrich_hospital_urls.py --limit 5000 --workers 16 || true

echo "==> MRF download + ingest (all indexed hospitals)"
mkdir -p "$MRF_DIR"
python3 scripts/mrf_streamer.py \
  --prices-db scraper/prices.db \
  --compliance-db scraper/compliance.db \
  --mrf-dir "$MRF_DIR"

echo "==> Merge → public DBs"
python3 scripts/merge_pipeline_data.py
python3 scripts/build_hot_db.py
python3 scripts/write_dataset_manifest.py
python3 scripts/verify_dataset_artifacts.py

echo "==> Done."
