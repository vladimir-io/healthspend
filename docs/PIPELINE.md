# Pipeline And Auditor Guide

This guide is for maintainers and contributors who need the full ingestion/audit workflow.

## Payer Auditor Architecture

Healthspend includes a payer-auditor foundation in ingestion:

- Streaming JSON parsing for large MRFs (reader-based, no full-file buffering)
- Dimensional negotiated-rate schema (`dim_*`, `fact_negotiated_rate`, `hot_price_compare`)
- Dual-write ingestion path (legacy `prices` table + dimensional facts)
- NPI attribution confidence scoring
- Parser contract tests for CMS v3 JSON/CSV plus malformed fallback fixtures

If you are upgrading an existing database:

```bash
python3 scripts/migrate_schema.py scraper/prices.db
```

## NPPES V2 Enrichment

Load NPPES V2 files into the same database used by ingestion:

```bash
python3 scripts/load_nppes_v2.py \
  --db scraper/prices.db \
  --core /path/to/npidata_pfile_v2.csv \
  --practice /path/to/pl_pfile_v2.csv \
  --other-names /path/to/othername_pfile_v2.csv \
  --deactivations /path/to/npideact_pfile_v2.csv
```

This powers:

- Practice-location state matching for Type 2 org NPIs
- Alias/DBA reference support
- Deactivation-code-aware confidence penalties
- Expanded auditor fields (accessibility, secondary languages, direct email)

Useful follow-up commands:

```bash
python3 scripts/audit_zombie_npi.py --db scraper/prices.db
python3 scripts/audit_license_proxy.py --db scraper/prices.db
python3 scripts/load_nppes_v2.py --db scraper/prices.db --endpoints /path/to/endpoint_pfile_v2.csv
python3 scripts/ping_nppes_endpoints.py --db scraper/prices.db --limit 100
python3 scripts/load_cms_attesters.py --db scraper/prices.db --file /path/to/cms_attesters.csv
python3 scripts/audit_attester_validity.py --db scraper/prices.db
```

## Fast Path

Discovery-only phase:

```bash
cd scraper
cargo run --bin discover
```

Bulk download + ingest phase:

```bash
python3 scripts/run_audit_pipeline.py \
  --db scraper/prices.db \
  --mrf-dir ./mrf_temp \
  --cleanup
```

Optional controls:

- `--state TX` to run one state
- `--mrf-limit 200` to process a bounded sample first
- `--skip-existing-mrfs` to reuse already-downloaded files

Run migration, optional enrichments, parser smoke, and audits in one command:

```bash
python3 scripts/run_audit_pipeline.py --db scraper/prices.db
```

If you stage downloads in a temp folder, cleanup can be automatic:

```bash
python3 scripts/run_audit_pipeline.py \
  --db scraper/prices.db \
  --mrf-dir /path/to/downloads \
  --cleanup
```

Only use cleanup for raw downloads. Keep generated SQLite artifacts in place for the app and deploys.

## Optional VFS Adapter

The web worker supports an adapter flag with fallback:

- `VITE_DB_VFS=sqljs-httpvfs` (default)
- `VITE_DB_VFS=turbolite` (locked to `turbolite@0.2.19` API)

If Turbolite is unavailable, runtime falls back to `sql.js-httpvfs`.

Package/API lock-in test:

```bash
cd web
npm run test:turbolite
```

## Benchmarking

Track before/after query latency:

```bash
python3 scripts/benchmark_hot_queries.py --db scraper/prices.db --label before-hot-origin
python3 scripts/benchmark_hot_queries.py --db scraper/prices.db --label after-hot-origin
```

History is appended to `scripts/benchmarks/hot_query_history.jsonl`.
