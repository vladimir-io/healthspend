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

## Pull Request Process

1.  **Fork & Branch:** Create a feature branch from `main`.
2.  **Document:** Provide a brief rationale for your changes, especially if they affect data parsing or scoring logic.
3.  **Validate:** Ensure your build passes (`npm run build`) and respects the zero backend architecture.
4.  **Submit:** Open a PR against the `main` branch for review.

Thank you for helping build the public infrastructure for healthcare transparency.
