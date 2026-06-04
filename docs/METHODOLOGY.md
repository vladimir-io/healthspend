# Healthspend Methodology

See the in-app **Methodology** tab for the patient-facing summary. This document is the maintainer reference.

## Data sources

1. **CMS Hospital General Information** (`ingest.py`) — facility registry and quality-reporting baseline.
2. **Hospital machine-readable files (MRFs)** — Rust scraper (`scraper/`) discovers, audits, and parses JSON/CSV transparency files per 45 CFR § 180.50.
3. **NPPES** — provider attribution for procedure-to-NPI confidence scoring.

## Facility Audit Index

Scores are merged in `scripts/merge_pipeline_data.py` from:

- CMS baseline signals (reporting completeness)
- MRF audit probes (reachability, schema, dollar amounts vs placeholders, attestation)

All scoring changes must cite applicable CMS rules (see `CONTRIBUTING.md`).

## Search quality

- Default **NPI attribution confidence ≥ 95%** (`web/src/config.ts`).
- FTS5 on `prices.description` when available.
- Fallback scope expansion is capped at **3 rounds** for latency (`MAX_SEARCH_FALLBACK_ROUNDS`).

## Release cadence

- **Daily** state-matrix audits (subset states)
- **Weekly** full CPT coverage (`HS_FULL_CPT_COVERAGE=1`)
- Artifacts published to Hugging Face; hot shard `audit_hot.db` built via `scripts/build_hot_db.py`

## Benchmarks

- Server-side hot queries: `scripts/benchmark_hot_queries.py` (p50 ≤ 30ms, p95 ≤ 60ms on `hot_price_compare`)
- Browser budgets: `web/e2e/perf.spec.ts` (Playwright, Slow 3G profile)
