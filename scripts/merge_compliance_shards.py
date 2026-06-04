#!/usr/bin/env python3
"""Merge per-state compliance.db shards (compliance + hospitals tables)."""

from __future__ import annotations

import argparse
import glob
import sqlite3
import sys
from pathlib import Path


def merge_shards(shard_paths: list[Path], out_path: Path) -> None:
    if not shard_paths:
        raise SystemExit("No compliance shards to merge.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    main = sqlite3.connect(out_path)

    main.executescript(
        """
        CREATE TABLE hospitals (
            ccn TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            city TEXT,
            website TEXT,
            cms_hpt_url TEXT,
            mrf_url TEXT,
            last_audited TEXT
        );
        CREATE TABLE compliance (
            ccn TEXT PRIMARY KEY,
            score INTEGER,
            txt_exists BOOLEAN,
            robots_ok BOOLEAN,
            mrf_reachable BOOLEAN,
            mrf_valid BOOLEAN,
            mrf_fresh BOOLEAN,
            shoppable_exists BOOLEAN,
            mrf_machine_readable INTEGER DEFAULT 1,
            waf_blocked INTEGER DEFAULT 0,
            last_checked TEXT,
            evidence_json TEXT
        );
        """
    )

    for shard_path in shard_paths:
        alias = "shard"
        main.execute("ATTACH DATABASE ? AS shard", (str(shard_path),))

        main.execute(
            """
            INSERT INTO hospitals (ccn, name, state, city, website, cms_hpt_url, mrf_url, last_audited)
            SELECT ccn, name, state, city, website, cms_hpt_url, mrf_url, last_audited
            FROM shard.hospitals
            ON CONFLICT(ccn) DO UPDATE SET
                name = COALESCE(excluded.name, hospitals.name),
                state = COALESCE(excluded.state, hospitals.state),
                city = COALESCE(excluded.city, hospitals.city),
                website = CASE
                    WHEN excluded.website IS NOT NULL AND excluded.website <> ''
                         AND excluded.website NOT LIKE '%google.com/search%'
                    THEN excluded.website
                    ELSE hospitals.website
                END,
                cms_hpt_url = CASE
                    WHEN excluded.cms_hpt_url IS NOT NULL AND excluded.cms_hpt_url <> ''
                    THEN excluded.cms_hpt_url
                    ELSE hospitals.cms_hpt_url
                END,
                mrf_url = CASE
                    WHEN excluded.mrf_url IS NOT NULL AND excluded.mrf_url <> ''
                    THEN excluded.mrf_url
                    ELSE hospitals.mrf_url
                END,
                last_audited = COALESCE(excluded.last_audited, hospitals.last_audited)
            """
        )

        main.execute(
            """
            INSERT OR REPLACE INTO compliance (
                ccn, score, txt_exists, robots_ok,
                mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists,
                mrf_machine_readable, waf_blocked, last_checked, evidence_json
            )
            SELECT
                ccn, score, txt_exists, robots_ok,
                mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists,
                mrf_machine_readable, waf_blocked, last_checked, evidence_json
            FROM shard.compliance
            """
        )
        main.execute("DETACH DATABASE shard")

    main.commit()
    h = main.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    c = main.execute("SELECT COUNT(*) FROM compliance").fetchone()[0]
    hpt = main.execute(
        "SELECT COUNT(*) FROM hospitals WHERE COALESCE(cms_hpt_url,'') <> ''"
    ).fetchone()[0]
    mrf = main.execute(
        "SELECT COUNT(*) FROM hospitals WHERE COALESCE(mrf_url,'') <> ''"
    ).fetchone()[0]
    main.close()
    print(
        f"✓ Merged {len(shard_paths)} shards → {out_path} "
        f"({h:,} hospitals, {c:,} compliance, {hpt:,} cms-hpt, {mrf:,} mrf_url)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge compliance.db shards")
    parser.add_argument("--shards-dir", default="/tmp/compliance_shards")
    parser.add_argument("--out", default="scraper/compliance.db")
    parser.add_argument("--glob", default="", help="Optional glob for shard paths")
    args = parser.parse_args()

    if args.glob:
        shard_paths = [Path(p) for p in sorted(glob.glob(args.glob))]
    else:
        shard_paths = sorted(Path(args.shards_dir).glob("*/compliance.db"))

    if not shard_paths:
        print("No shards found.", file=sys.stderr)
        return 1

    merge_shards(shard_paths, Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
