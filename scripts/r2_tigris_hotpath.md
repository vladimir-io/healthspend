# Hot Path Deployment Notes

## 1) Refresh the hot database

Generate the latest serving database locally:

```bash
python3 scripts/run_audit_pipeline.py --db scraper/prices.db
```

That pipeline writes `web/public/audit_data.db` and `web/public/data_manifest.json`.

## 2) Publish the static site

After refreshing the database, rebuild the web app and publish the static assets:

```bash
cd web
npm run build
cp -r dist/* public/
```

## 3) Verify query performance

Use benchmark history tooling to compare before and after runs:

```bash
python3 scripts/benchmark_hot_queries.py --db scraper/prices.db --label before-hot-origin
python3 scripts/benchmark_hot_queries.py --db scraper/prices.db --label after-hot-origin
```

Gate criteria:
- p50 <= 30ms for hot queries
- p95 <= 60ms for hot queries

If performance regresses, tune the SQLite query plans and keep the serving DB compact.
