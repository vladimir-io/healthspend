#!/usr/bin/env python3
"""
Build a national MRF URL index by harvesting cms-hpt.txt from health-system roots
and matching location-name blocks to CMS hospitals (CCN).

One HTTP fetch on a parent domain often yields dozens of mrf-url lines — far more
efficient than per-hospital domain guessing.
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
import http.client
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cms_hpt_utils import (
    HospitalRow,
    normalize_name,
    parse_cms_hpt_locations,
    website_from_mrf_url,
)
from hospital_match_index import HospitalMatchIndex

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

ROOT_CMS_PATHS = ("/cms-hpt.txt", "/.well-known/cms-hpt.txt")


def fetch_text(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def load_hospitals(path: Path) -> list[HospitalRow]:
    rows: list[HospitalRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            ccn = re.sub(r"\D", "", row.get("Facility ID") or "")
            if len(ccn) < 6:
                continue
            name = (row.get("Facility Name") or "").strip()
            state = (row.get("State") or "").strip().upper()
            city = (row.get("City/Town") or "").strip()
            if not name or not state:
                continue
            rows.append(
                HospitalRow(
                    ccn=ccn.zfill(6),
                    name=name,
                    state=state,
                    city=city,
                    name_norm=normalize_name(name),
                )
            )
    return rows


def load_roots(paths: list[Path]) -> list[str]:
    hosts: list[str] = []
    seen: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip().lower()
            if not line:
                continue
            line = line.replace("https://", "").replace("http://", "").strip("/")
            if line and line not in seen:
                seen.add(line)
                hosts.append(line)
    return hosts


def init_index_db(path: Path) -> sqlite3.Connection:
    """Open a temp DB; caller must finalize_index_db() to atomically replace path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(tmp)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE mrf_urls (
            ccn TEXT NOT NULL,
            mrf_url TEXT NOT NULL,
            cms_hpt_url TEXT,
            website TEXT,
            source TEXT NOT NULL,
            location_name TEXT,
            confidence REAL NOT NULL,
            PRIMARY KEY (ccn, mrf_url)
        )
        """
    )
    conn.execute("CREATE INDEX idx_mrf_urls_ccn ON mrf_urls(ccn)")
    conn.commit()
    return conn, tmp


def finalize_index_db(path: Path, tmp: Path, conn: sqlite3.Connection) -> None:
    """Atomically promote tmp → path so a failed build never wipes a good index."""
    conn.commit()
    conn.close()
    if not tmp.exists() or tmp.stat().st_size == 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"Index build produced no data at {tmp}")
    backup = path.with_suffix(path.suffix + ".bak")
    if path.exists():
        if backup.exists():
            backup.unlink()
        path.replace(backup)
    tmp.replace(path)
    if backup.exists():
        backup.unlink()


def harvest_root(host: str) -> tuple[str, list[CmsHptLocation]]:
    for scheme in ("https", "http"):
        for path in ROOT_CMS_PATHS:
            cms_url = f"{scheme}://{host}{path}"
            try:
                body = fetch_text(cms_url)
            except (
                urllib.error.URLError,
                http.client.HTTPException,
                TimeoutError,
                OSError,
                ValueError,
            ):
                continue
            if "mrf-url" not in body.lower() and "standardcharge" not in body.lower():
                continue
            website = f"{scheme}://{host}"
            locs = parse_cms_hpt_locations(body, cms_url, website)
            if locs:
                return host, locs
    return host, []


def main() -> int:
    parser = argparse.ArgumentParser(description="Build national MRF URL index")
    parser.add_argument("--hospitals-csv", default="hospitals.csv")
    parser.add_argument("--roots-file", default="data/health_system_roots.txt")
    parser.add_argument(
        "--extra-roots-file",
        action="append",
        default=[],
        help="Additional host list files (e.g. data/discovery_roots_extra.txt)",
    )
    parser.add_argument("--out", default="data/mrf_url_index.sqlite")
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--min-match", type=float, default=0.80)
    args = parser.parse_args()

    hospitals_path = Path(args.hospitals_csv)
    if not hospitals_path.exists():
        print(f"Missing {hospitals_path}", file=sys.stderr)
        return 1

    hospitals = load_hospitals(hospitals_path)
    match_index = HospitalMatchIndex.from_csv_rows(hospitals)
    root_paths = [Path(args.roots_file)]
    default_extra = Path("data/discovery_roots_extra.txt")
    if default_extra.exists():
        root_paths.append(default_extra)
    for p in args.extra_roots_file:
        root_paths.append(Path(p))
    roots = load_roots(root_paths)
    print(f"Hospitals: {len(hospitals):,}  Roots: {len(roots):,}")

    out_path = Path(args.out)
    conn, tmp_path = init_index_db(out_path)
    total_locs = 0
    total_inserted = 0
    roots_hit = 0

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(harvest_root, h): h for h in roots}
        for fut in as_completed(futures):
            try:
                host, locs = fut.result()
            except Exception as exc:
                print(f"  warn harvest {futures[fut]}: {exc}", file=sys.stderr)
                continue
            if not locs:
                continue
            roots_hit += 1
            total_locs += len(locs)
            for loc in locs:
                matched = match_index.resolve(loc, args.min_match)
                if not matched:
                    continue
                ccn, score = matched
                website = loc.website or website_from_mrf_url(loc.mrf_url)
                confidence = min(0.99, 0.55 + score * 0.45)
                existing = conn.execute(
                    "SELECT confidence FROM mrf_urls WHERE ccn = ? AND mrf_url = ?",
                    (ccn, loc.mrf_url),
                ).fetchone()
                if existing and existing[0] >= confidence:
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO mrf_urls
                        (ccn, mrf_url, cms_hpt_url, website, source, location_name, confidence)
                    VALUES (?, ?, ?, ?, 'root_cms_hpt', ?, ?)
                    """,
                    (
                        ccn,
                        loc.mrf_url,
                        loc.cms_hpt_url,
                        website,
                        loc.location_name,
                        confidence,
                    ),
                )
                total_inserted += 1
            if roots_hit % 5 == 0:
                conn.commit()
                print(f"  {host}: +{len(locs)} blocks, indexed so far={total_inserted:,}")

    ccn_count = conn.execute("SELECT COUNT(DISTINCT ccn) FROM mrf_urls").fetchone()[0]
    row_count = conn.execute("SELECT COUNT(*) FROM mrf_urls").fetchone()[0]
    if ccn_count == 0:
        conn.close()
        tmp_path.unlink(missing_ok=True)
        print("✗ Index build found no hospitals", file=sys.stderr)
        return 1

    try:
        finalize_index_db(out_path, tmp_path, conn)
    except OSError as e:
        print(f"✗ Failed to write index: {e}", file=sys.stderr)
        return 1

    print(
        f"✓ Index {args.out}: {row_count:,} mrf rows, {ccn_count:,} hospitals "
        f"({roots_hit}/{len(roots)} roots with cms-hpt, {total_locs:,} location blocks)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
