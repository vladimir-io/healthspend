#!/usr/bin/env bash
# Verify CLOUDFLARE_API_TOKEN can access the healthspend Pages project.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${CLOUDFLARE_ENV_FILE:-$ROOT/web/.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a && source "$ENV_FILE" && set +a
fi

: "${CLOUDFLARE_API_TOKEN:?Set CLOUDFLARE_API_TOKEN in web/.env or the environment}"
: "${CLOUDFLARE_ACCOUNT_ID:?Set CLOUDFLARE_ACCOUNT_ID in web/.env or the environment}"

PROJECT="${CLOUDFLARE_PAGES_PROJECT:-healthspend}"
URL="https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/pages/projects/${PROJECT}"

HTTP_CODE="$(curl -s -o /tmp/cf_pages_check.json -w "%{http_code}" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" "$URL")"

if [[ "$HTTP_CODE" == "200" ]]; then
  echo "✓ Token can access Cloudflare Pages project: $PROJECT"
  exit 0
fi

echo "✗ Cloudflare token cannot deploy to Pages (HTTP $HTTP_CODE)" >&2
python3 - <<'PY' 2>/dev/null || cat /tmp/cf_pages_check.json >&2
import json
from pathlib import Path
d = json.loads(Path("/tmp/cf_pages_check.json").read_text())
print("errors:", d.get("errors"))
PY
echo >&2
echo "Create a new token with Cloudflare Pages → Edit permission:" >&2
echo "  https://dash.cloudflare.com/profile/api-tokens" >&2
echo "  → Create Token → Edit Cloudflare Workers (includes Pages)" >&2
echo "  → Account Resources: include $CLOUDFLARE_ACCOUNT_ID" >&2
echo "Then update web/.env and run: gh secret set CLOUDFLARE_API_TOKEN < <(grep CLOUDFLARE_API_TOKEN web/.env | cut -d= -f2-)" >&2
exit 1
