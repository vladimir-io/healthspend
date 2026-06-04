#!/usr/bin/env python3
"""Apply mrf_url_index.sqlite URLs into scraper compliance.db hospitals table."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply MRF URL index to compliance.db")
    parser.add_argument("--index", default="data/mrf_url_index.sqlite")
    parser.add_argument("--compliance-db", default="scraper/compliance.db")
    parser.add_argument("--state", default="", help="Optional state filter")
    parser.add_argument("--min-confidence", type=float, default=0.75)
    args = parser.parse_args()

    index_path = Path(args.index)
    comp_path = Path(args.compliance_db)
    if not index_path.exists():
        print(f"Missing index: {index_path}", file=sys.stderr)
        return 1
    if not comp_path.exists():
        print(f"Missing compliance db: {comp_path}", file=sys.stderr)
        return 1

    idx = sqlite3.connect(index_path)
    comp = sqlite3.connect(comp_path)

    sql = """
        SELECT ccn, mrf_url, cms_hpt_url, website, MAX(confidence) AS confidence
        FROM mrf_urls
        WHERE confidence >= ?
    """
    params: list = [args.min_confidence]
    if args.state:
        sql += " AND ccn IN (SELECT ccn FROM hospitals WHERE UPPER(state) = UPPER(?))"
        params.append(args.state)
    sql += " GROUP BY ccn"

    rows = idx.execute(sql, params).fetchall()
    idx.close()

    updated = 0
    for ccn, mrf_url, cms_hpt_url, website, _conf in rows:
        cur = comp.execute(
            """
            UPDATE hospitals SET
                mrf_url = CASE WHEN ? <> '' THEN ? ELSE mrf_url END,
                cms_hpt_url = CASE WHEN ? <> '' THEN ? ELSE cms_hpt_url END,
                website = CASE WHEN ? <> '' THEN ? ELSE website END
            WHERE ccn = ?
            """,
            (mrf_url, mrf_url, cms_hpt_url, cms_hpt_url, website, website, ccn),
        )
        updated += cur.rowcount

    comp.commit()
    with_mrf = comp.execute(
        "SELECT COUNT(*) FROM hospitals WHERE COALESCE(mrf_url,'') <> ''"
        + (" AND UPPER(state) = UPPER(?)" if args.state else ""),
        (args.state,) if args.state else (),
    ).fetchone()[0]
    comp.close()

    print(f"✓ Applied index → {comp_path}: {updated:,} hospitals updated ({with_mrf:,} with mrf_url)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
