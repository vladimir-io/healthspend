#!/usr/bin/env bash
# Reorganize history into phase snapshots (non-interactive).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SOURCE="$(git rev-parse HEAD)"
BACKUP="$(git branch --list 'backup-main-*' | tail -1 | tr -d ' *')"
if [[ -z "$BACKUP" ]]; then
  BACKUP="backup-main-$(date +%Y%m%d-%H%M%S)"
  git branch "$BACKUP" "$SOURCE"
fi
echo "Source snapshot: $SOURCE"
echo "Backup branch: $BACKUP"

phases=(
  "phase-1(foundation): initial open source release|2c334fe0"
  "phase-2(web): search, claim-rate UX, and patient-facing UI|fe469d1d"
  "phase-3(pipeline): hot shard, CI, audits, and dataset verification|aa078000"
  "phase-4(data): discovery, MRF enrichment, and ingest fixes|adba5f77"
  "phase-5(ci): national 50-state matrix and index-first MRF harvest|b75c83f7"
  "phase-6(data): index matching, ingest throughput, and publish path|79828e02"
  "phase-7(data): coverage growth, ROI pipeline, and disk stability|${SOURCE}"
)

git checkout -B main-reorg 2cc195fb

for entry in "${phases[@]}"; do
  msg="${entry%%|*}"
  tip="${entry##*|}"
  echo "==> $msg @ ${tip:0:8}"
  git checkout "$tip" -- .
  if git diff-index --cached --quiet HEAD --; then
    echo "  (no changes — skip)"
  else
    git commit -m "$msg"
  fi
done

git branch -M main
echo "✓ Reorganized into ${#phases[@]} phase commits on main"
git log --oneline
