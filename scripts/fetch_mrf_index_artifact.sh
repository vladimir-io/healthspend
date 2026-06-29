#!/usr/bin/env bash
# Download mrf-url-index from the latest successful Build MRF URL Index workflow run.
# GitHub Actions artifacts are per-run; daily/weekly pipelines must pull cross-workflow.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${MRF_INDEX_OUT:-$ROOT/data/mrf_url_index.sqlite}"
mkdir -p "$(dirname "$OUT")"

if [ -f "$OUT" ] && [ "${MRF_INDEX_FORCE:-0}" != "1" ]; then
  echo "✓ MRF URL index already present: $OUT"
  exit 0
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI not available — cannot fetch cross-workflow MRF index" >&2
  exit 1
fi

RUN_ID="$(gh run list \
  --workflow=build-mrf-index.yml \
  --status=success \
  --limit=1 \
  --json databaseId \
  -q '.[0].databaseId' 2>/dev/null || true)"

if [ -z "$RUN_ID" ] || [ "$RUN_ID" = "null" ]; then
  echo "No successful Build MRF URL Index workflow run found" >&2
  exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading mrf-url-index from workflow run $RUN_ID"
gh run download "$RUN_ID" --name mrf-url-index --dir "$TMP"

if [ -f "$TMP/mrf_url_index.sqlite" ]; then
  SRC="$TMP/mrf_url_index.sqlite"
elif [ -f "$TMP/data/mrf_url_index.sqlite" ]; then
  SRC="$TMP/data/mrf_url_index.sqlite"
else
  echo "Download succeeded but mrf_url_index.sqlite not found in artifact" >&2
  exit 1
fi

cp "$SRC" "$OUT"
echo "✓ MRF URL index ready at $OUT"
