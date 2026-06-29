# Cloudflare infrastructure

Declarative config for **healthspend.lol** edge settings. Secrets live in `web/.env` (gitignored).

## WAF — block scanner noise

`waf-scanner-block.json` blocks automated probes for WordPress paths, `.env`, and `.git` that pollute traffic analytics.

**Requires:** API token with **Zone → Firewall Services → Edit**.

```bash
# Optional: set CLOUDFLARE_ZONE_ID in web/.env to skip zone lookup
python3 scripts/apply_cloudflare_waf.py
```

## RUM ledger (Pages Function + KV)

Client beacons from `web/src/rum.ts` POST to `/api/v1/rum`. The Pages Function at `web/functions/api/v1/rum.ts` rolls up:

- `path` + hash routes (`/#search`, `/#hospitals`)
- Search → dispute funnel (`search_to_dispute`, `dispute_open`, `dispute_send`)
- UTM sources and DB warm timings

**Report:**

```bash
python3 scripts/rum_analytics_report.py 7
```

Uses `CLOUDFLARE_API_TOKEN_READ_ONLY` from `web/.env`.

## Hot database shard

Production defaults to `audit_hot.db` (20 high-traffic CPTs, slim hospital rows). Built by:

```bash
python3 scripts/build_hot_db.py
python3 scripts/write_dataset_manifest.py
```

Published to Hugging Face by the weekly/daily audit pipelines.
