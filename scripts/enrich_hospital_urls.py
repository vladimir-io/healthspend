#!/usr/bin/env python3
"""
Probe candidate hospital domains for cms-hpt.txt and update compliance.db hospitals.

Used when CMS CSV has no website column and NPPES endpoints are mostly non-HTTP.
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

STOPWORDS = {
    "hospital",
    "medical",
    "center",
    "centre",
    "health",
    "healthcare",
    "system",
    "systems",
    "regional",
    "community",
    "memorial",
    "general",
    "the",
    "of",
    "and",
    "at",
    "llc",
    "inc",
    "corp",
    "ltd",
    "lp",
    "pllc",
    "dba",
}


def fetch_text(url: str, timeout: int = 12) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def slug_words(name: str) -> list[str]:
    raw = re.sub(r"[^a-zA-Z0-9\s]", " ", name.lower())
    words = [w for w in raw.split() if w and w not in STOPWORDS]
    return words


def domain_candidates(org_name: str) -> list[str]:
    words = slug_words(org_name)
    if not words:
        return []

    joined = "".join(words)
    hyphen = "-".join(words)
    acro = "".join(w[0] for w in words if w)

    bases: list[str] = []
    for base in (joined, hyphen, acro):
        if len(base) >= 4:
            bases.append(base)

    if len(words) >= 2:
        pair = "".join(words[:2])
        if len(pair) >= 4:
            bases.append(pair)

    domains: list[str] = []
    seen: set[str] = set()
    for base in bases:
        for tld in ("org", "com", "net"):
            host = f"{base}.{tld}"
            if host not in seen:
                seen.add(host)
                domains.append(host)
    return domains


def parse_cms_hpt(body: str) -> tuple[str, str]:
    """Return (cms_hpt_url, mrf_url) from cms-hpt.txt body."""
    mrf_urls: list[str] = []
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("*"):
            continue
        lower = line.lower()
        if lower.startswith("mrf-url:"):
            val = line.split(":", 1)[1].strip()
            if val.startswith("http"):
                mrf_urls.append(val)
        elif line.startswith("http") and any(
            ext in lower for ext in (".json", ".csv", ".ashx", ".zip", ".gz")
        ):
            mrf_urls.append(line.split(",")[0].strip())

    mrf_url = mrf_urls[0] if mrf_urls else ""
    return "", mrf_url


def probe_domain(domain: str) -> tuple[str, str, str] | None:
    for scheme in ("https", "http"):
        cms_url = f"{scheme}://{domain}/cms-hpt.txt"
        try:
            body = fetch_text(cms_url)
        except (urllib.error.URLError, TimeoutError):
            continue
        if "mrf-url" not in body.lower() and "http" not in body.lower():
            continue
        _, mrf_url = parse_cms_hpt(body)
        website = f"{scheme}://{domain}"
        return website, cms_url, mrf_url
    return None


def load_enrollment_names(path: Path) -> dict[str, str]:
    by_ccn: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in csv.DictReader(f):
            ccn = re.sub(r"\D", "", row.get("CCN") or row.get("CAH OR HOSPITAL CCN") or "")
            if len(ccn) < 6:
                continue
            ccn = ccn.zfill(6)
            name = (
                row.get("DOING BUSINESS AS NAME")
                or row.get("ORGANIZATION NAME")
                or ""
            ).strip()
            if name:
                by_ccn[ccn] = name
    return by_ccn


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe cms-hpt.txt for hospital domains")
    parser.add_argument("--compliance-db", default="scraper/compliance.db")
    parser.add_argument("--enrollments", default="data/discovery/hospital_enrollments.csv")
    parser.add_argument("--state", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    enrollments = Path(args.enrollments)
    if not enrollments.exists():
        print(f"Missing {enrollments}; run scripts/fetch_discovery_inputs.py first")
        return 1

    org_by_ccn = load_enrollment_names(enrollments)
    conn = sqlite3.connect(args.compliance_db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = """
        SELECT ccn, COALESCE(name, '') AS name, COALESCE(website, '') AS website,
               COALESCE(cms_hpt_url, '') AS cms_hpt_url, COALESCE(mrf_url, '') AS mrf_url
        FROM hospitals
    """
    params: tuple = ()
    if args.state:
        sql += " WHERE UPPER(state) = UPPER(?)"
        params = (args.state,)
    rows = cur.execute(sql, params).fetchall()

    todo: list[tuple[str, str]] = []
    for row in rows:
        ccn = row["ccn"]
        if row["mrf_url"] or row["cms_hpt_url"]:
            continue
        if row["website"] and "google.com" not in row["website"]:
            continue
        org = org_by_ccn.get(ccn) or row["name"]
        if not org:
            continue
        todo.append((ccn, org))

    if args.limit and args.limit > 0:
        todo = todo[: args.limit]

    print(f"Probing {len(todo)} hospitals (workers={args.workers})…")
    updated = 0

    def work(item: tuple[str, str]) -> tuple[str, str, str, str] | None:
        ccn, org = item
        for domain in domain_candidates(org):
            hit = probe_domain(domain)
            if hit:
                website, cms_hpt, mrf = hit
                return ccn, website, cms_hpt, mrf
        return None

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(work, item): item[0] for item in todo}
        for fut in as_completed(futures):
            result = fut.result()
            if not result:
                continue
            ccn, website, cms_hpt, mrf = result
            cur.execute(
                """
                UPDATE hospitals SET
                    website = ?,
                    cms_hpt_url = CASE WHEN ? <> '' THEN ? ELSE cms_hpt_url END,
                    mrf_url = CASE WHEN ? <> '' THEN ? ELSE mrf_url END
                WHERE ccn = ?
                """,
                (website, cms_hpt, cms_hpt, mrf, mrf, ccn),
            )
            updated += 1
            if updated % 25 == 0:
                print(f"  enriched {updated}…")

    conn.commit()
    with_hpt = cur.execute(
        "SELECT COUNT(*) FROM hospitals WHERE COALESCE(cms_hpt_url,'') <> ''"
    ).fetchone()[0]
    with_mrf = cur.execute(
        "SELECT COUNT(*) FROM hospitals WHERE COALESCE(mrf_url,'') <> ''"
    ).fetchone()[0]
    conn.close()
    print(f"✓ Updated {updated} hospitals ({with_hpt} with cms-hpt, {with_mrf} with mrf_url)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
