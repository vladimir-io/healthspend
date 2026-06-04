#!/usr/bin/env python3
"""Write dataset_manifest.json with row counts for Hugging Face consumers."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from build_hot_db import HOT_CPTS

FULL = Path("web/public/audit_data.db")
HOT = Path("web/public/audit_hot.db")
OUT = Path("web/public/dataset_manifest.json")


def _stats(path: Path) -> dict:
    if not path.is_file():
        return {"present": False}
    conn = sqlite3.connect(path)
    prices = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    hospitals = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    cpts = conn.execute(
        "SELECT COUNT(DISTINCT cpt_code) FROM prices WHERE cpt_code IS NOT NULL AND cpt_code != ''"
    ).fetchone()[0]
    conn.close()
    return {
        "present": True,
        "size_bytes": path.stat().st_size,
        "price_rows": prices,
        "hospital_rows": hospitals,
        "distinct_cpt_codes": cpts,
    }


def main() -> None:
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "hot_cpt_codes": list(HOT_CPTS),
        "artifacts": {
            "audit_data.db": {
                "role": "full_price_and_compliance_ledger",
                **_stats(FULL),
            },
            "audit_hot.db": {
                "role": "top_cpt_hot_shard_for_client_search",
                "hot_cpt_filter_count": len(HOT_CPTS),
                **_stats(HOT),
            },
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
