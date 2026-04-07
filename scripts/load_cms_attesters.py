#!/usr/bin/env python3
"""
Load CMS attester data for attestation validity audits.
Expected columns include CCN + Attester Name + Attester NPI + Attestation Date.
"""

import argparse
import csv
import sqlite3
from pathlib import Path


def normalize_key(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())


def pick(row, keys, candidates):
    idx = {normalize_key(k): k for k in keys}
    for c in candidates:
        k = normalize_key(c)
        if k in idx:
            return (row.get(idx[k], "") or "").strip()
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Load CMS attester file into cms_attestations")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to prices db")
    parser.add_argument("--file", required=True, help="CSV path")
    args = parser.parse_args()

    db = Path(args.db)
    path = Path(args.file)
    if not db.exists() or not path.exists():
        print("Missing database or attester file")
        return 1

    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cms_attestations (
            ccn TEXT PRIMARY KEY,
            attester_name TEXT,
            attester_npi TEXT,
            attestation_date TEXT,
            source_file TEXT,
            last_seen TEXT
        )
        """
    )

    count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        keys = reader.fieldnames or []
        for row in reader:
            ccn = pick(row, keys, ["CCN", "CMS Certification Number", "Facility ID"])
            if not ccn:
                continue
            name = pick(row, keys, ["Attester Name", "Attestation Name"])
            npi = pick(row, keys, ["Attester NPI", "NPI"])
            date = pick(row, keys, ["Attestation Date", "Date"])
            conn.execute(
                """
                INSERT INTO cms_attestations (ccn, attester_name, attester_npi, attestation_date, source_file, last_seen)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(ccn) DO UPDATE SET
                    attester_name=excluded.attester_name,
                    attester_npi=excluded.attester_npi,
                    attestation_date=excluded.attestation_date,
                    source_file=excluded.source_file,
                    last_seen=datetime('now')
                """,
                (ccn, name, npi, date, str(path)),
            )
            count += 1

    conn.commit()
    conn.close()
    print(f"Loaded attester rows: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
