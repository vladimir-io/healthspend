#!/usr/bin/env python3
"""Merge per-state scraper/prices.db shards into one prices.db."""

from __future__ import annotations

import argparse
import glob
import sqlite3
import sys
from pathlib import Path


def merge_shards(shard_paths: list[Path], out_path: Path) -> None:
    if not shard_paths:
        raise SystemExit("No price shards to merge.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    main = sqlite3.connect(out_path)

    for i, shard_path in enumerate(shard_paths):
        alias = f"shard_{i}"
        main.execute(f"ATTACH DATABASE ? AS {alias}", (str(shard_path),))

        if i == 0:
            row = main.execute(
                f"SELECT sql FROM {alias}.sqlite_master WHERE type='table' AND name='prices'"
            ).fetchone()
            if not row or not row[0]:
                raise SystemExit(f"{shard_path} has no prices table")
            main.execute(row[0])

        main.execute(
            f"""
            INSERT OR REPLACE INTO prices
                (ccn, cpt_code, description, gross_charge, cash_price,
                 min_negotiated, max_negotiated, payer, plan,
                 provider_npi, attribution_confidence)
            SELECT
                ccn, cpt_code, description, gross_charge, cash_price,
                min_negotiated, max_negotiated, payer, plan,
                provider_npi, attribution_confidence
            FROM {alias}.prices
            """
        )
        main.execute(f"DETACH DATABASE {alias}")

    main.commit()
    count = main.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    cpts = main.execute(
        "SELECT COUNT(DISTINCT cpt_code) FROM prices WHERE cpt_code IS NOT NULL AND cpt_code != ''"
    ).fetchone()[0]
    main.close()
    print(f"✓ Merged {len(shard_paths)} shards → {out_path} ({count:,} prices, {cpts:,} CPTs)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge prices.db shards")
    parser.add_argument(
        "--shards-dir",
        default="/tmp/prices_shards",
        help="Directory containing state subdirs with prices.db",
    )
    parser.add_argument("--out", default="scraper/prices.db")
    parser.add_argument(
        "--glob",
        default="",
        help="Optional glob for shard files (overrides --shards-dir layout)",
    )
    args = parser.parse_args()

    if args.glob:
        shard_paths = [Path(p) for p in sorted(glob.glob(args.glob))]
    else:
        shard_paths = sorted(Path(args.shards_dir).glob("*/prices.db"))

    if not shard_paths:
        print("No shards found.", file=sys.stderr)
        return 1

    merge_shards(shard_paths, Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
