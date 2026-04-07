#!/usr/bin/env python3
"""
License Proxy Audit: detect CCN vs NPI legal entity mismatches.
"""

import argparse
import re
import sqlite3
from pathlib import Path


def norm(name: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", (name or "").lower())).strip()


def names_match(a: str, b: str) -> bool:
    an = norm(a)
    bn = norm(b)
    if not an or not bn:
        return False
    return an == bn or an in bn or bn in an


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CCN vs NPI license proxy audit")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to prices db")
    args = parser.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"Database not found: {db}")
        return 1

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT DISTINCT
            h.ccn,
            h.name,
            p.provider_npi,
            d.org_name
        FROM prices p
        JOIN dim_hospital h ON h.ccn = p.ccn
        LEFT JOIN dim_provider_npi d ON d.npi = p.provider_npi
        WHERE p.provider_npi IS NOT NULL AND TRIM(p.provider_npi) <> ''
        """
    ).fetchall()

    findings = []
    for ccn, hospital_name, npi, org_name in rows:
        if not hospital_name or not org_name:
            continue
        if names_match(hospital_name, org_name):
            continue

        alias_rows = cur.execute(
            "SELECT other_name FROM nppes_other_names WHERE npi = ?",
            (npi,),
        ).fetchall()
        alias_match = any(names_match(hospital_name, alias[0]) for alias in alias_rows)
        if alias_match:
            continue

        findings.append((ccn, hospital_name, npi, org_name))

    if not findings:
        print("No license proxy mismatches found.")
        conn.close()
        return 0

    print("License Proxy Findings")
    print("=" * 80)
    for ccn, hname, npi, org in findings:
        print(f"CCN={ccn} hospital='{hname}' NPI={npi} legal_entity='{org}'")

    print("-" * 80)
    print(f"Total mismatches: {len(findings)}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
