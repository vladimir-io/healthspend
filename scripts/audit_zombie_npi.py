#!/usr/bin/env python3
"""
Audit potentially active billing rows tied to deactivated NPIs.

Focuses on high-risk reason codes (especially code 4: Misused/Identity Theft).
"""

import argparse
import sqlite3
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit zombie NPI exposure in prices.db")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to prices database")
    parser.add_argument("--snapshot", default=None, help="Optional snapshot date filter (YYYY-MM-DD)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    where = "WHERE p.provider_npi IS NOT NULL AND TRIM(p.provider_npi) <> ''"
    params = []
    if args.snapshot:
        where += " AND h.snapshot_date = ?"
        params.append(args.snapshot)

    query = f"""
    SELECT
      p.ccn,
      p.provider_npi,
      d.reason_code,
      COALESCE(d.reason_text, 'Unknown') AS reason_text,
      d.deactivation_date,
      COUNT(*) as row_count
    FROM prices p
    LEFT JOIN nppes_deactivations d ON d.npi = p.provider_npi
    LEFT JOIN hot_price_compare h ON h.ccn = p.ccn
    {where}
      AND d.reason_code IS NOT NULL
    GROUP BY p.ccn, p.provider_npi, d.reason_code, d.reason_text, d.deactivation_date
    ORDER BY CASE d.reason_code WHEN '4' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2 ELSE 3 END, row_count DESC
    """

    rows = cur.execute(query, params).fetchall()
    if not rows:
        print("No deactivated NPI exposure found in current dataset.")
        conn.close()
        return 0

    print("Zombie NPI Exposure Report")
    print("=" * 80)
    for ccn, npi, code, text, date, count in rows:
        sev = "CRITICAL" if code == "4" else "HIGH" if code in {"1", "2"} else "MEDIUM"
        print(f"[{sev}] CCN={ccn} NPI={npi} reason={code} ({text}) deactivated={date or 'unknown'} rows={count}")

    code4 = sum(r[-1] for r in rows if r[2] == "4")
    print("-" * 80)
    print(f"Total affected rows: {sum(r[-1] for r in rows)}")
    print(f"Code 4 (Misused/Identity Theft) rows: {code4}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
