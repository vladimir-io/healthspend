# Operations

Day-to-day commands for **healthspend.lol**. See also [infra/cloudflare/README.md](../infra/cloudflare/README.md).

## Deploy site

```bash
bash scripts/deploy_cloudflare.sh
```

Requires `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` in `web/.env`.

## Analytics (RUM)

```bash
python3 scripts/rum_analytics_report.py 7
```

Key funnel section: **Search → dispute** (search → “Use this rate” → email send).

## WAF (clean bot traffic)

```bash
python3 scripts/apply_cloudflare_waf.py
```

Blocks `/wp-admin`, `.env`, `.git` probes. Requires firewall edit permission on the zone token.

## SEO pages

```bash
python3 scripts/generate_compare_pages.py   # /compare/* procedure×state
python3 scripts/patch_visibility_seo.py     # hospital + state visibility CTAs
```

## Data pipeline

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| Daily Hospital Audit | nightly | Incremental MRF ingest |
| Weekly Full Coverage | weekly | Full CPT coverage + HF publish |
| Deploy Cloudflare Pages | on `web/**` push | Site + RUM function |

## Local dev

```bash
cd web && npm install && npm run dev
```

Uses `audit_hot.db` by default. Set `VITE_USE_FULL_DB=true` for the full ledger.
