#!/usr/bin/env bash
# Reclaim disk space from safe-to-delete build artifacts and caches.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

AGGRESSIVE="${AGGRESSIVE:-0}"
FREED_KB=0

rm_path() {
  local p="$1"
  if [[ -e "$p" ]]; then
    local sz
    sz="$(du -sk "$p" 2>/dev/null | awk '{print $1}')"
    echo "  remove ${sz}KB → $p"
    rm -rf "$p"
    FREED_KB=$((FREED_KB + sz))
  fi
}

echo "==> Cleanup (AGGRESSIVE=${AGGRESSIVE})"

for p in node-v*.tar.xz node-v20.*; do
  [[ -e "$p" ]] && rm_path "$p"
done

rm_path mrf_temp
rm -rf /tmp/healthspend-mrf-national /tmp/mrf-* 2>/dev/null || true

find data -maxdepth 1 \( -name '*.sqlite.tmp' -o -name '*.sqlite.bak' -o -name '*.sqlite-journal' \) -delete 2>/dev/null || true

rm_path data/discovery/nppes_dissemination.zip
rm_path scraper/target
rm_path data/health_system_roots_curated.txt

if [[ "$AGGRESSIVE" == "1" ]]; then
  rm_path scraper/prices.db
  if [[ "${KEEP_PRICE_ARCHIVE:-0}" != "1" ]]; then
    rm_path data/archive/prices.db
  fi
  echo "  canonical prices: web/public/audit_data.db (re-ingest: ./scripts/grow_mrf_coverage.sh)"
fi

echo "==> Freed ~$((FREED_KB / 1024)) MB"
df -h /System/Volumes/Data 2>/dev/null | tail -1 || df -h . | tail -1
