#!/usr/bin/env python3
"""
Bulk MRF downloader + ingester.

Flow per hospital node:
1) Resolve MRF URL (direct mrf_url or via cms-hpt.txt)
2) Stream file to local mrf-dir
3) Ingest with Rust parser binary
4) Optionally delete raw file immediately
"""

from __future__ import annotations

import argparse
import gzip
import os
import re
import shlex
import sqlite3
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)


def fetch_text(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def resolve_mrf_url(mrf_url: str, cms_hpt_url: str, website: str) -> str:
    if mrf_url and mrf_url.strip():
        return mrf_url.strip()

    if not cms_hpt_url or not cms_hpt_url.strip():
        return ""

    try:
        body = fetch_text(cms_hpt_url.strip())
    except Exception:
        return ""

    candidates = []

    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "standard" in line.lower() or "machine" in line.lower() or line.startswith("http"):
            candidates.append(line.split(",")[0].strip())

    for match in URL_PATTERN.findall(body):
        candidates.append(match.strip())

    for candidate in candidates:
        if not candidate:
            continue
        lowered = candidate.lower()
        if any(ext in lowered for ext in [".json", ".csv", ".json.gz", ".csv.gz", ".zip"]):
            if lowered.startswith("http://") or lowered.startswith("https://"):
                return candidate
            if website:
                return urllib.parse.urljoin(website if website.endswith("/") else website + "/", candidate)
            return urllib.parse.urljoin(cms_hpt_url, candidate)

    return ""


def infer_extension(url: str) -> str:
    path = urllib.parse.urlparse(url).path.lower()
    if path.endswith(".json.gz"):
        return ".json.gz"
    if path.endswith(".csv.gz"):
        return ".csv.gz"
    if path.endswith(".json"):
        return ".json"
    if path.endswith(".csv"):
        return ".csv"
    if path.endswith(".txt"):
        return ".txt"
    if path.endswith(".zip"):
        return ".zip"
    return ".json"


def stream_download(url: str, target: Path, chunk_size: int = 1024 * 1024) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as r, target.open("wb") as out:
        while True:
            chunk = r.read(chunk_size)
            if not chunk:
                break
            out.write(chunk)


def maybe_decompress_gzip(path: Path) -> Path:
    if not path.name.endswith(".gz"):
        return path

    out_path = path.with_suffix("")
    with gzip.open(path, "rb") as src, out_path.open("wb") as dst:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)

    return out_path


def ingest_one(repo: Path, file_path: Path, ccn: str, prices_db: str, compliance_db: str) -> int:
    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "ingest_mrf",
        "--",
        "--file",
        str(file_path),
        "--ccn",
        ccn,
        "--prices-db",
        prices_db,
        "--compliance-db",
        compliance_db,
    ]
    print("$", " ".join(shlex.quote(c) for c in cmd))
    completed = subprocess.run(cmd, cwd=str(repo / "scraper"))
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and ingest hospital MRFs")
    parser.add_argument("--prices-db", default="scraper/prices.db")
    parser.add_argument("--compliance-db", default="scraper/compliance.db")
    parser.add_argument("--mrf-dir", required=True)
    parser.add_argument("--state", default="")
    parser.add_argument("--threads", type=int, default=4, help="Worker hint; ingest remains single-writer for SQLite consistency")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--cleanup", action="store_true", help="Delete each raw MRF after ingest")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    prices_db = str((repo / args.prices_db).resolve())
    compliance_db = str((repo / args.compliance_db).resolve())
    mrf_dir = Path(args.mrf_dir).expanduser().resolve()
    mrf_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(compliance_db)
    try:
        cur = conn.cursor()
        if args.state:
            rows = cur.execute(
                """
                SELECT ccn, COALESCE(mrf_url, ''), COALESCE(cms_hpt_url, ''), COALESCE(website, '')
                FROM hospitals
                WHERE UPPER(state) = UPPER(?)
                ORDER BY ccn
                """,
                (args.state,),
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT ccn, COALESCE(mrf_url, ''), COALESCE(cms_hpt_url, ''), COALESCE(website, '')
                FROM hospitals
                ORDER BY ccn
                """
            ).fetchall()
    finally:
        conn.close()

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    attempted = 0
    ingested = 0
    skipped = 0
    failed = 0

    if args.threads > 1:
        print(
            f"threads={args.threads} requested; using single-writer ingest path for SQLite consistency"
        )

    for ccn, mrf_url, cms_hpt_url, website in rows:
        attempted += 1
        resolved = resolve_mrf_url(mrf_url, cms_hpt_url, website)
        if not resolved:
            skipped += 1
            print(f"skip ccn={ccn}: no resolved MRF URL")
            continue

        ext = infer_extension(resolved)
        if ext == ".zip":
            skipped += 1
            print(f"skip ccn={ccn}: zip MRF not yet supported ({resolved})")
            continue

        raw_path = mrf_dir / f"{ccn}{ext}"
        if args.skip_existing and raw_path.exists():
            print(f"reuse ccn={ccn}: {raw_path}")
        else:
            try:
                print(f"download ccn={ccn} url={resolved}")
                stream_download(resolved, raw_path)
            except Exception as e:
                failed += 1
                print(f"fail download ccn={ccn}: {e}")
                continue

        ingest_path = raw_path
        try:
            ingest_path = maybe_decompress_gzip(raw_path)
        except Exception as e:
            failed += 1
            print(f"fail decompress ccn={ccn}: {e}")
            continue

        rc = ingest_one(repo, ingest_path, ccn, prices_db, compliance_db)
        if rc == 0:
            ingested += 1
            if args.cleanup:
                try:
                    if ingest_path.exists():
                        ingest_path.unlink()
                    if raw_path.exists() and raw_path != ingest_path:
                        raw_path.unlink()
                except Exception as e:
                    print(f"warn cleanup ccn={ccn}: {e}")
        else:
            failed += 1

    print(
        f"done attempted={attempted} ingested={ingested} skipped={skipped} failed={failed} mrf_dir={mrf_dir}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
