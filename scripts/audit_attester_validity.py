#!/usr/bin/env python3
"""
Cross-reference CMS attesters against NPPES deactivations.
"""

import argparse
import sqlite3
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit CMS attester validity against NPPES deactivations")
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
        SELECT
            a.ccn,
            a.attester_name,
            a.attester_npi,
            d.reason_code,
            d.reason_text,
            d.deactivation_date
        FROM cms_attestations a
        LEFT JOIN nppes_deactivations d ON d.npi = a.attester_npi
        WHERE a.attester_npi IS NOT NULL AND TRIM(a.attester_npi) <> ''
          AND d.reason_code IS NOT NULL
        ORDER BY CASE d.reason_code WHEN '4' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2 ELSE 3 END
        """
    ).fetchall()

    if not rows:
        print("No invalid/deactivated attesters detected.")
        conn.close()
        return 0

    print("CMS Attester Validity Findings")
    print("=" * 80)
    for ccn, name, npi, code, text, date in rows:
        sev = "CRITICAL" if code == "4" else "HIGH"
        print(f"[{sev}] CCN={ccn} attester='{name}' NPI={npi} code={code} ({text}) deactivated={date or 'unknown'}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
