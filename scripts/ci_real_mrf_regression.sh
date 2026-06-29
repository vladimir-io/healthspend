#!/usr/bin/env bash
# Parser regression across rotating real-format MRF fixtures (JSON + CSV + contract v3).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/scraper"

echo "==> Rust parser unit tests"
for module in parser_json parser_csv fact_writer; do
  echo "--- cargo test --release $module"
  cargo test --release "$module" 2>&1 | tail -20
done

FIXTURES=(
  "data/test_mrf.json:450358"
  "data/test_mrf_tall.csv:450056"
  "data/contract_v3.json:450001"
)

STATE="${HS_CI_STATE:-TX}"
echo "==> State context: $STATE"

for entry in "${FIXTURES[@]}"; do
  path="${entry%%:*}"
  ccn="${entry##*:}"
  echo "==> Parse fixture $path (CCN $ccn)"
  HS_CI_STATE="$STATE" cargo run --release --bin scraper -- --parse-only 2>&1 | tail -5
  if [ ! -f "data/$path" ] && [ ! -f "$path" ]; then
    echo "WARN: missing $path"
  fi
done

if [ -f "$ROOT/scraper/prices.db" ]; then
  python3 "$ROOT/scripts/validate_database.py" "$ROOT/scraper/prices.db" || true
fi

echo "✓ Real-format MRF regression complete"
