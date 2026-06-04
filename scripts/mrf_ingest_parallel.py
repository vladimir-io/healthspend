#!/usr/bin/env python3
"""
Parallel MRF ingest: partition hospitals across worker processes, each with its own
prices shard, then merge into scraper/prices.db.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


def worker_ingest(
    worker_id: int,
    ccns: list[str],
    repo: str,
    compliance_db: str,
    shard_dir: str,
    mrf_dir: str,
) -> tuple[int, int]:
    if not ccns:
        return worker_id, 0

    root = Path(repo)
    wdir = Path(shard_dir) / f"worker_{worker_id:02d}"
    wdir.mkdir(parents=True, exist_ok=True)
    ccn_file = wdir / "ccns.txt"
    ccn_file.write_text("\n".join(ccns) + "\n", encoding="utf-8")

    prices = wdir / "prices.db"
    if prices.exists():
        prices.unlink()
    # Bootstrap empty prices table from master if present
    master_prices = root / "scraper" / "prices.db"
    if master_prices.exists():
        m = sqlite3.connect(master_prices)
        ddl = m.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='prices'"
        ).fetchone()
        m.close()
        if ddl and ddl[0]:
            p = sqlite3.connect(prices)
            p.execute(ddl[0])
            p.commit()
            p.close()

    mrf = Path(mrf_dir) / f"w{worker_id:02d}"
    mrf.mkdir(parents=True, exist_ok=True)
    rel_prices = os.path.relpath(prices, repo)
    rel_comp = compliance_db
    cmd = [
        sys.executable,
        os.path.join(repo, "scripts", "mrf_streamer.py"),
        "--prices-db",
        rel_prices,
        "--compliance-db",
        rel_comp,
        "--mrf-dir",
        str(mrf),
        "--ccn-file",
        str(ccn_file),
        "--cleanup",
    ]
    subprocess.run(cmd, cwd=repo, check=False)

    ingested = 0
    if prices.exists():
        ingested = sqlite3.connect(prices).execute(
            "SELECT COUNT(DISTINCT ccn) FROM prices"
        ).fetchone()[0]
    return worker_id, ingested


def main() -> int:
    parser = argparse.ArgumentParser(description="Parallel MRF ingest")
    parser.add_argument("--compliance-db", default="scraper/compliance.db")
    parser.add_argument("--prices-db", default="scraper/prices.db")
    parser.add_argument("--mrf-dir", default="mrf_temp/parallel")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shard-dir", default="mrf_temp/ingest_shards")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    comp = repo / args.compliance_db
    if not comp.exists():
        print(f"Missing {comp}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(comp)
    rows = conn.execute(
        """
        SELECT ccn FROM hospitals
        WHERE COALESCE(mrf_url, '') <> '' OR COALESCE(cms_hpt_url, '') <> ''
        ORDER BY CASE WHEN COALESCE(mrf_url,'') <> '' THEN 0 ELSE 1 END, ccn
        """
    ).fetchall()
    conn.close()
    ccns = [r[0] for r in rows]
    if args.limit > 0:
        ccns = ccns[: args.limit]

    if not ccns:
        print("No hospitals with MRF/cms-hpt URLs to ingest")
        return 0

    n_workers = max(1, min(args.workers, len(ccns)))
    chunks: list[list[str]] = [[] for _ in range(n_workers)]
    for i, ccn in enumerate(ccns):
        chunks[i % n_workers].append(ccn)

    shard_dir = repo / args.shard_dir
    mrf_dir = repo / args.mrf_dir
    print(f"Ingesting {len(ccns):,} hospitals across {n_workers} workers…")

    total = 0
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = [
            pool.submit(
                worker_ingest,
                i,
                chunks[i],
                str(repo),
                args.compliance_db,
                str(shard_dir),
                str(mrf_dir),
            )
            for i in range(n_workers)
            if chunks[i]
        ]
        for fut in as_completed(futures):
            wid, n = fut.result()
            total += n
            print(f"  worker {wid}: {n} hospitals with price rows")

    glob_pat = str(shard_dir / "worker_*/prices.db")
    import glob as glob_mod

    shards = sorted(glob_mod.glob(glob_pat))
    if not shards:
        print("No price shards produced", file=sys.stderr)
        return 1

    subprocess.run(
        [
            sys.executable,
            str(repo / "scripts" / "merge_prices_shards.py"),
            "--glob",
            glob_pat,
            "--out",
            args.prices_db,
        ],
        cwd=str(repo),
        check=True,
    )
    print(f"✓ Parallel ingest merged ({total} hospitals with prices in shards)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
