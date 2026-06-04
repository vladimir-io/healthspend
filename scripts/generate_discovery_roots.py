#!/usr/bin/env python3
"""
Generate extra cms-hpt probe hosts from NPPES endpoint URLs and hospital names.

Output merges into data/health_system_roots.txt (or a separate file for build_mrf_index).
"""

from __future__ import annotations

import argparse
import csv
import re
import urllib.parse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from enrich_hospital_urls import domain_candidates

SKIP_HOST_SUFFIXES = (
    "google.com",
    "facebook.com",
    "twitter.com",
    "linkedin.com",
    "youtube.com",
    "instagram.com",
    "sharepoint.com",
    "microsoft.com",
    "amazonaws.com",
    "cloudfront.net",
)


def host_from_url(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url.strip()).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def load_hosts_from_endpoints(path: Path, limit: int = 0) -> list[str]:
    hosts: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("Endpoint") or row.get("endpoint") or "").strip()
            if not url.startswith("http"):
                continue
            host = host_from_url(url)
            if not host or host in seen:
                continue
            if any(host.endswith(s) or f".{s}" in host for s in SKIP_HOST_SUFFIXES):
                continue
            seen.add(host)
            hosts.append(host)
            if limit and len(hosts) >= limit:
                break
    return hosts


def load_hosts_from_enrollments(path: Path, limit: int = 0) -> list[str]:
    hosts: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in csv.DictReader(f):
            org = (
                row.get("DOING BUSINESS AS NAME")
                or row.get("ORGANIZATION NAME")
                or ""
            ).strip()
            if not org:
                continue
            for domain in domain_candidates(org):
                if domain in seen:
                    continue
                seen.add(domain)
                hosts.append(domain)
                if limit and len(hosts) >= limit:
                    return hosts
    return hosts


def merge_roots_file(path: Path, new_hosts: list[str]) -> int:
    existing = set()
    lines: list[str] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            raw = line.split("#", 1)[0].strip().lower()
            if not raw:
                lines.append(line)
                continue
            host = raw.replace("https://", "").replace("http://", "").strip("/")
            existing.add(host)
            lines.append(line)

    added = 0
    for host in new_hosts:
        h = host.lower().strip()
        if not h or h in existing:
            continue
        existing.add(h)
        lines.append(h)
        added += 1

    if added:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return added


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate cms-hpt probe hosts")
    parser.add_argument(
        "--endpoints",
        default="data/discovery/nppes_endpoints_subset.csv",
        help="NPPES endpoint subset CSV",
    )
    parser.add_argument(
        "--enrollments",
        default="data/discovery/hospital_enrollments.csv",
    )
    parser.add_argument("--out", default="data/health_system_roots.txt")
    parser.add_argument("--endpoint-limit", type=int, default=800)
    parser.add_argument("--enrollment-limit", type=int, default=1200)
    parser.add_argument("--merge", action="store_true", help="Append into --out")
    parser.add_argument(
        "--write-extra",
        default="data/discovery_roots_extra.txt",
        help="Always write new hosts here",
    )
    args = parser.parse_args()

    hosts: list[str] = []
    ep = Path(args.endpoints)
    if args.endpoint_limit > 0 and ep.exists():
        ep_hosts = load_hosts_from_endpoints(ep, args.endpoint_limit)
        print(f"  endpoints → {len(ep_hosts):,} hosts")
        hosts.extend(ep_hosts)
    elif args.endpoint_limit > 0:
        print(f"  skip endpoints (missing {ep}); run fetch_discovery_inputs.py --with-nppes")

    enr = Path(args.enrollments)
    if args.enrollment_limit > 0 and enr.exists():
        enr_hosts = load_hosts_from_enrollments(enr, args.enrollment_limit)
        print(f"  enrollments → {len(enr_hosts):,} candidate hosts")
        hosts.extend(enr_hosts)

    seen: set[str] = set()
    unique: list[str] = []
    for h in hosts:
        if h not in seen:
            seen.add(h)
            unique.append(h)

    extra_path = Path(args.write_extra)
    extra_path.parent.mkdir(parents=True, exist_ok=True)
    extra_path.write_text("\n".join(unique) + "\n", encoding="utf-8")
    print(f"✓ Wrote {len(unique):,} hosts → {extra_path}")

    if args.merge:
        added = merge_roots_file(Path(args.out), unique)
        print(f"✓ Merged {added:,} new hosts into {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
