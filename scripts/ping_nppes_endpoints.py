#!/usr/bin/env python3
"""
Ping NPPES endpoint references to verify endpoint reachability.
"""

import argparse
import sqlite3
import urllib.request
from pathlib import Path


def check(url: str, timeout: int = 8) -> str:
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return str(resp.status)
    except Exception:
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return str(resp.status)
        except Exception as exc:
            return f"ERR:{type(exc).__name__}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Ping NPPES endpoint reference URLs")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to prices db")
    parser.add_argument("--limit", type=int, default=100, help="Max endpoints to test")
    args = parser.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"Database not found: {db}")
        return 1

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT npi, endpoint_url FROM nppes_endpoints ORDER BY id DESC LIMIT ?",
        (args.limit,),
    ).fetchall()

    if not rows:
        print("No endpoints found in nppes_endpoints.")
        conn.close()
        return 0

    ok = 0
    for npi, url in rows:
        status = check(url)
        if status.startswith("2") or status.startswith("3"):
            ok += 1
        print(f"NPI={npi} status={status} url={url}")

    print("-" * 80)
    print(f"Reachable: {ok}/{len(rows)}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
