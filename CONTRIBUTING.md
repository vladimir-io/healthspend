# Contributing to Healthspend

Healthspend is an open source project dedicated to making US hospital pricing transparent and actionable. We welcome contributions that improve data accuracy, scraper performance, and the auditing engine.

## Architectural Principles

To maintain the zero infrastructure, high performance model, all contributions must adhere to these standards:

* **Zero Backend:** All data querying must occur client-side using `sql.js-httpvfs`. We do not host a traditional database API.
* **Performance:** Heavy computation (parsing or LLM logic) must run in **Web Workers** to keep the UI responsive.
* **Regulatory Alignment:** Any changes to compliance scoring must be strictly cited against current **CMS regulations** (§180.50 or §180.60).

## Areas of Focus

* **Rust Scrapers:** Improving the ingestion and cleaning of non standard hospital Machine Readable Files (MRFs).
* **Audit Engine:** Refining the deterministic logic used to score hospital transparency compliance.
* **UI/UX:** Enhancing the TypeScript/Vite frontend for high-density data visualization.

## Disk space (local dev)

Large artifacts are gitignored but can fill your disk:

| Path | Typical size | Safe to delete? |
|------|----------------|-----------------|
| `web/public/audit_data.db` | ~2.5GB | **No** — production dataset |
| `scraper/prices.db` | ~5GB | Yes if `audit_data.db` is current — re-ingest via `grow_mrf_coverage.sh` |
| `data/discovery/nppes_dissemination.zip` | ~1GB | Yes — re-fetch with `fetch_discovery_inputs.py --with-nppes` |
| `node-v*/`, `mrf_temp/` | varies | Yes — run `./scripts/cleanup_disk.sh` |

Restore a known-good state:

```bash
chmod +x scripts/stabilize_project.sh scripts/cleanup_disk.sh
./scripts/stabilize_project.sh   # cleanup + verify + refresh hot shard
```

## Pull Request Process

1.  **Fork & Branch:** Create a feature branch from `main`.
2.  **Document:** Provide a brief rationale for your changes, especially if they affect data parsing or scoring logic.
3.  **Validate:** Ensure your build passes (`npm run build`) and respects the zero backend architecture.
4.  **Submit:** Open a PR against the `main` branch for review.

Thank you for helping build the public infrastructure for healthcare transparency.
