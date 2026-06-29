#!/usr/bin/env bash
# Build and deploy healthspend.lol to Cloudflare Pages.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 scripts/patch_visibility_seo.py

cd web
npm run build

if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  set -a && source .env && set +a
fi

if [[ -n "${CLOUDFLARE_API_TOKEN:-}" ]]; then
  if bash "$ROOT/scripts/verify_cloudflare_token.sh"; then
    npx wrangler@3.90.0 pages deploy dist \
      --project-name="${CLOUDFLARE_PAGES_PROJECT:-healthspend}" \
      --branch=main
    exit 0
  fi
  echo "API token lacks Pages permission — falling back to Wrangler OAuth…" >&2
fi

CLOUDFLARE_API_TOKEN= CLOUDFLARE_ACCOUNT_ID= \
  npx wrangler@3.90.0 pages deploy dist \
    --project-name="${CLOUDFLARE_PAGES_PROJECT:-healthspend}" \
    --branch=main
