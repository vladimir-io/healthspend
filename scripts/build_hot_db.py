#!/usr/bin/env python3
"""Build a compact hot-tier SQLite shard for fast first-query client loads."""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# High-traffic CPT codes aligned with web/src/db.ts audit nodes and shortcuts
HOT_CPTS = (
    "27447", "27130", "70551", "74177", "71045", "80053", "45378",
    "99283", "99285", "59400", "12001", "90686", "96372", "99213",
    "90791", "95810",
)


def build_hot_db(source: Path, out: Path) -> dict:
    if not source.is_file():
        raise SystemExit(f"Source database not found: {source}")

    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()

    src = sqlite3.connect(source)
    dst = sqlite3.connect(out)
    dst.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;")
    dst.execute("ATTACH DATABASE ? AS src", (str(source),))

    placeholders = ",".join("?" * len(HOT_CPTS))
    cpt_predicate = f"cpt_code IN ({placeholders})"

    for table in ("hospitals", "compliance"):
        row = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if row and row[0]:
            dst.execute(row[0])
            dst.execute(f"INSERT INTO {table} SELECT * FROM src.{table}")

    prices_row = src.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='prices'"
    ).fetchone()
    if not prices_row or not prices_row[0]:
        raise SystemExit("Source DB missing prices table")

    dst.execute(prices_row[0])
    dst.execute(f"INSERT INTO prices SELECT * FROM src.prices WHERE {cpt_predicate}", HOT_CPTS)

    # Always build FTS on hot shard for fast browser search (small row count).
    try:
        dst.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS prices_fts USING fts5(
                description,
                cpt_code,
                content='prices',
                content_rowid='rowid',
                tokenize='unicode61'
            )
            """
        )
        dst.execute("INSERT INTO prices_fts(prices_fts) VALUES('rebuild')")
    except sqlite3.Error as e:
        print(f"  (hot FTS build skipped: {e})")

    dst.execute(
        """
        CREATE TABLE IF NOT EXISTS hot_price_compare (
            code TEXT PRIMARY KEY,
            row_count INTEGER,
            min_price REAL,
            max_price REAL,
            median_price REAL
        )
        """
    )
    for code in HOT_CPTS:
        row = dst.execute(
            """
            SELECT COUNT(*), MIN(cash_price), MAX(cash_price)
            FROM prices WHERE cpt_code = ? AND cash_price > 0
            """,
            (code,),
        ).fetchone()
        if not row or not row[0]:
            continue
        cnt, mn, mx = row
        med = dst.execute(
            """
            SELECT cash_price FROM prices
            WHERE cpt_code = ? AND cash_price > 0
            ORDER BY cash_price LIMIT 1 OFFSET ?
            """,
            (code, max(0, cnt // 2)),
        ).fetchone()
        dst.execute(
            """
            INSERT OR REPLACE INTO hot_price_compare (code, row_count, min_price, max_price, median_price)
            VALUES (?, ?, ?, ?, ?)
            """,
            (code, cnt, mn, mx, med[0] if med else None),
        )

    dst.execute("CREATE INDEX IF NOT EXISTS idx_hot_prices_cpt ON prices(cpt_code)")
    dst.execute("CREATE INDEX IF NOT EXISTS idx_hot_prices_ein ON prices(ein)")
    dst.commit()
    if os.environ.get("HS_SKIP_VACUUM", "").strip() not in ("1", "true", "yes"):
        try:
            dst.execute("VACUUM")
            dst.commit()
        except sqlite3.OperationalError as e:
            print(f"  (VACUUM skipped: {e})")
    src.close()
    dst.close()

    verify = sqlite3.connect(out)
    row_count = verify.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    verify.close()

    manifest = {
        "tier": "hot",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "hot_cpt_count": len(HOT_CPTS),
        "price_rows": row_count,
    }
    print(f"✓ audit_hot.db: {row_count:,} price rows → {out}")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Build audit_hot.db shard")
    parser.add_argument("--source", default="web/public/audit_data.db")
    parser.add_argument("--out", default="web/public/audit_hot.db")
    args = parser.parse_args()
    build_hot_db(Path(args.source), Path(args.out))


if __name__ == "__main__":
    main()
