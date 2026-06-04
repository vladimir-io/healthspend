#!/usr/bin/env python3
"""
Import bulk hospital MRF URLs (e.g. Trilliant CSV export) into mrf_url_index or compliance.db.

Expected CSV columns (case-insensitive): ccn|facility_id, mrf_url|url|mrf
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from apply_mrf_index import normalize_ccn


def pick(row: dict[str, str], *names: str) -> str:
    lower = {k.lower().strip(): v for k, v in row.items()}
    for n in names:
        if n in lower and lower[n].strip():
            return lower[n].strip()
    return ""


def ensure_index_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mrf_urls (
            ccn TEXT NOT NULL,
            mrf_url TEXT NOT NULL,
            cms_hpt_url TEXT,
            website TEXT,
            source TEXT NOT NULL,
            location_name TEXT,
            confidence REAL NOT NULL,
            PRIMARY KEY (ccn, mrf_url)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mrf_urls_ccn ON mrf_urls(ccn)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Import bulk MRF URL CSV")
    parser.add_argument("csv_path")
    parser.add_argument("--index", default="data/mrf_url_index.sqlite")
    parser.add_argument("--compliance-db", default="")
    parser.add_argument("--source", default="bulk_import")
    parser.add_argument("--confidence", type=float, default=0.92)
    args = parser.parse_args()

    path = Path(args.csv_path)
    if not path.exists():
        print(f"Missing {path}", file=sys.stderr)
        return 1

    inserted = 0
    skipped = 0

    if args.compliance_db:
        comp = sqlite3.connect(args.compliance_db)
        for row in csv.DictReader(path.open(encoding="utf-8-sig", newline="")):
            ccn = normalize_ccn(pick(row, "ccn", "facility_id", "facility id"))
            mrf = pick(row, "mrf_url", "mrf", "url", "standard_charges_url")
            if len(ccn) != 6 or not mrf.startswith("http"):
                skipped += 1
                continue
            comp.execute(
                """
                UPDATE hospitals SET mrf_url = ?
                WHERE ccn = ? AND (COALESCE(mrf_url,'') = '' OR mrf_url = ?)
                """,
                (mrf, ccn, mrf),
            )
            if comp.total_changes:
                inserted += 1
        comp.commit()
        with_mrf = comp.execute(
            "SELECT COUNT(*) FROM hospitals WHERE COALESCE(mrf_url,'') <> ''"
        ).fetchone()[0]
        comp.close()
        print(f"✓ compliance: {inserted:,} updated ({with_mrf:,} with mrf_url), skipped={skipped:,}")
        return 0

    idx_path = Path(args.index)
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    idx = sqlite3.connect(idx_path)
    ensure_index_schema(idx)
    for row in csv.DictReader(path.open(encoding="utf-8-sig", newline="")):
        ccn = normalize_ccn(pick(row, "ccn", "facility_id", "facility id"))
        mrf = pick(row, "mrf_url", "mrf", "url", "standard_charges_url")
        if len(ccn) != 6 or not mrf.startswith("http"):
            skipped += 1
            continue
        idx.execute(
            """
            INSERT OR REPLACE INTO mrf_urls
                (ccn, mrf_url, cms_hpt_url, website, source, location_name, confidence)
            VALUES (?, ?, '', '', ?, '', ?)
            """,
            (ccn, mrf, args.source, args.confidence),
        )
        inserted += 1
    idx.commit()
    ccns = idx.execute("SELECT COUNT(DISTINCT ccn) FROM mrf_urls").fetchone()[0]
    idx.close()
    print(f"✓ index: {inserted:,} rows ({ccns:,} CCNs), skipped={skipped:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
