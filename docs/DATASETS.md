# Published SQLite artifacts

Dataset repo: [vladimir-io/healthspend-data](https://huggingface.co/datasets/vladimir-io/healthspend-data)

## `audit_data.db` — full price and compliance ledger

**What it is:** The complete published database from the ingest + audit pipeline.

| Component | Contents |
|-----------|----------|
| `hospitals` | CMS facility registry (~40k nodes in current builds) |
| `compliance` | Per-hospital audit scores and violation signals |
| `prices` | Normalized cash-rate rows from ingested MRF data |
| Other tables | Schema for NPPES, star-schema facts, FTS — populated as pipelines fill them |

**What it is not:** A static dump of every CPT at every hospital nationwide. Row counts grow as MRF ingestion expands; the current public build has on the order of **10⁴ price rows** across **11 procedure codes**, not tens of millions.

**Built by:** `ingest.py` → `scripts/merge_pipeline_data.py` → validation → `web/public/audit_data.db` (see CI `daily-audit.yml`).

## `audit_hot.db` — top-CPT hot shard

**What it is:** A smaller file for fast browser load (`sql.js-httpvfs`). The web app loads this first, then falls back to `audit_data.db`.

| Component | Contents |
|-----------|----------|
| `hospitals`, `compliance` | Full copy from `audit_data.db` |
| `prices` | Only rows where `cpt_code` is in the hot CPT list (16 codes; see `scripts/build_hot_db.py`) |
| FTS / `hot_price_compare` | Subset when present in source |

**Built by:** `python3 scripts/build_hot_db.py --source web/public/audit_data.db --out web/public/audit_hot.db`

Hot CPT codes:

`27447`, `27130`, `70551`, `74177`, `71045`, `80053`, `45378`, `99283`, `99285`, `59400`, `12001`, `90686`, `96372`, `99213`, `90791`, `95810`

If the full ledger only contains a subset of those codes, the hot shard includes every code that exists in source (typically the same 11 codes until ingestion widens).

## Verify before publish

```bash
python3 scripts/build_hot_db.py
python3 scripts/verify_dataset_artifacts.py
python3 scripts/write_dataset_manifest.py
```

## Publish

```bash
export HF_TOKEN=...   # or HF
python3 scripts/deploy_huggingface.py \
  --repo-id vladimir-io/healthspend-data \
  --files web/public/audit_data.db web/public/audit_hot.db web/public/dataset_manifest.json
```
