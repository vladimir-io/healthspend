#!/usr/bin/env python3
"""Seed scraper/compliance.db hospitals from hospitals.csv (ingest-only CI path)."""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed scraper hospitals table")
    parser.add_argument("--csv", default="hospitals.csv")
    parser.add_argument("--compliance-db", default="scraper/compliance.db")
    parser.add_argument("--state", default="")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Missing {csv_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.compliance_db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hospitals (
            ccn TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            city TEXT,
            website TEXT,
            cms_hpt_url TEXT,
            mrf_url TEXT,
            last_audited TEXT
        )
        """
    )
    n = 0
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            ccn = re.sub(r"\D", "", row.get("Facility ID") or "")
            if len(ccn) < 6:
                continue
            ccn = ccn.zfill(6)
            state = (row.get("State") or "").strip().upper()
            if args.state and state != args.state.upper():
                continue
            name = (row.get("Facility Name") or "").strip()
            city = (row.get("City/Town") or "").strip()
            conn.execute(
                """
                INSERT INTO hospitals (ccn, name, state, city, website, cms_hpt_url, mrf_url, last_audited)
                VALUES (?, ?, ?, ?, '', '', '', '')
                ON CONFLICT(ccn) DO UPDATE SET
                    name = excluded.name,
                    state = excluded.state,
                    city = excluded.city
                """,
                (ccn, name, state, city),
            )
            n += 1
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    conn.close()
    print(f"✓ Seeded {n:,} hospitals ({total:,} total in {args.compliance_db})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
