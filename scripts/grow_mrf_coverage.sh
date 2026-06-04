#!/usr/bin/env bash
# Grow national MRF URL coverage: more probe hosts → index → apply → optional ingest.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

WITH_NPPES="${WITH_NPPES:-0}"
SKIP_INGEST="${SKIP_INGEST:-0}"
# Cap guessed enrollment domains — most slugs never host cms-hpt.txt; 400 keeps harvest under ~1hr.
ENROLLMENT_LIMIT="${ENROLLMENT_LIMIT:-400}"
ENDPOINT_LIMIT="${ENDPOINT_LIMIT:-500}"

echo "==> CMS discovery inputs"
python3 scripts/fetch_discovery_inputs.py ${WITH_NPPES:+--with-nppes}

echo "==> Generate extra cms-hpt probe hosts (written to discovery_roots_extra.txt only)"
python3 scripts/generate_discovery_roots.py --enrollment-limit "$ENROLLMENT_LIMIT"
if [[ -f data/discovery/nppes_endpoints_subset.csv ]]; then
  python3 scripts/generate_discovery_roots.py --endpoint-limit "$ENDPOINT_LIMIT"
fi

echo "==> Build MRF URL index (health-system + discovery roots)"
MRF_INDEX_REBUILD=1 python3 scripts/build_mrf_index.py --workers 32 --out data/mrf_url_index.sqlite

echo "==> Apply index to compliance.db"
python3 scripts/apply_mrf_index.py

if [[ "$SKIP_INGEST" == "1" ]]; then
  echo "==> SKIP_INGEST=1 — run mrf_streamer manually when disk allows"
  exit 0
fi

echo "==> Ingest MRFs for hospitals with known URLs (parallel=${MRF_PARALLEL_WORKERS:-4})"
mkdir -p mrf_temp/national
if [[ "${MRF_PARALLEL_WORKERS:-4}" -gt 1 ]]; then
  python3 scripts/mrf_ingest_parallel.py \
    --workers "${MRF_PARALLEL_WORKERS}" \
    --compliance-db scraper/compliance.db \
    --prices-db scraper/prices.db \
    --mrf-dir mrf_temp/national
else
  python3 scripts/mrf_streamer.py \
    --prices-db scraper/prices.db \
    --compliance-db scraper/compliance.db \
    --mrf-dir mrf_temp/national \
    --cleanup
fi

echo "==> Publish public DBs"
HS_SKIP_FTS=1 HS_SKIP_VACUUM=1 ./scripts/finish_public_db.sh

echo "==> Coverage growth pass complete."
