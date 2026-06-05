#!/usr/bin/env python3
"""Generate machine-readable agent assets: manifest.json and procedures.json."""
from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"
PUBLIC = WEB / "public"
CPT_TS = WEB / "src" / "cpt_catalog.ts"
NODES_DIR = PUBLIC / "api" / "v1" / "nodes"
EMBED_DIR = PUBLIC / "embed" / "data"
OUT_MANIFEST = PUBLIC / "api" / "v1" / "manifest.json"
OUT_PROCEDURES = PUBLIC / "api" / "v1" / "procedures.json"
SITEMAP = PUBLIC / "sitemap.xml"

BASE = "https://healthspend.lol"
HF = "https://huggingface.co/datasets/vladimir-io/healthspend-data/resolve/main"

# High-traffic procedures for search dominance (hot shard + common queries)
FEATURED_CODES = {
    "70551", "73721", "74177", "71045", "80053", "80061", "45378", "45380",
    "27447", "99283", "99285", "99213", "96372", "90686", "77067", "85025",
    "84443", "83036", "76700", "72148", "70450", "71250", "76805", "36415",
}


def parse_cpt_catalog() -> list[dict]:
    text = CPT_TS.read_text(encoding="utf-8")
    entries = []
    for m in re.finditer(
        r"\{\s*code:\s*'([^']+)',\s*plain:\s*'([^']*)',\s*technical:\s*'([^']*)',\s*category:\s*'([^']+)'\s*\}",
        text,
    ):
        entries.append({
            "cpt_code": m.group(1),
            "name": m.group(2),
            "technical": m.group(3),
            "category": m.group(4),
        })
    return entries


def search_url(query: str, state: str = "") -> str:
    from urllib.parse import quote_plus
    url = f"{BASE}/?q={quote_plus(query)}"
    if state:
        url += f"&state={state.upper()}"
    return url + "#search"


def build_procedures(catalog: list[dict]) -> dict:
    featured = [e for e in catalog if e["cpt_code"] in FEATURED_CODES]
    featured.sort(key=lambda e: e["name"])
    procedures = []
    for e in featured:
        procedures.append({
            **e,
            "search_url": search_url(e["cpt_code"]),
            "search_url_natural": search_url(e["name"]),
        })
    return {
        "updated": date.today().isoformat(),
        "count": len(procedures),
        "procedures": procedures,
    }


def build_manifest(node_count: int, state_count: int) -> dict:
    return {
        "name": "Healthspend",
        "description": "US hospital price transparency — published cash rates from CMS filings for 7,400+ hospitals.",
        "version": "1.0",
        "updated": date.today().isoformat(),
        "site": BASE,
        "guides": {
            "llms_txt": f"{BASE}/llms.txt",
            "llms_full_txt": f"{BASE}/llms-full.txt",
            "ai_txt": f"{BASE}/ai.txt",
            "robots_txt": f"{BASE}/robots.txt",
            "developers": f"{BASE}/developers.html",
            "documentation": f"{BASE}/api/v1/documentation.html",
            "sitemap": f"{BASE}/sitemap.xml",
        },
        "search": {
            "web_app": f"{BASE}/#search",
            "deep_link_template": f"{BASE}/?q={{query}}&state={{state}}#search",
            "examples": [
                f"{BASE}/?q=MRI#search",
                f"{BASE}/?q=80053&state=CA#search",
                f"{BASE}/?q=Knee+Replacement#search",
            ],
        },
        "endpoints": [
            {
                "method": "GET",
                "path": "/api/v1/manifest.json",
                "description": "This file — machine-readable site index",
            },
            {
                "method": "GET",
                "path": "/api/v1/procedures.json",
                "description": "Common CPT procedures with search deep links",
            },
            {
                "method": "GET",
                "path": "/api/v1/nodes/{ccn}.json",
                "description": "Per-hospital published cash rates (CCN = 6-digit CMS ID)",
                "count": node_count,
                "example": f"{BASE}/api/v1/nodes/010001.json",
            },
            {
                "method": "GET",
                "path": "/embed/data/{state}.json",
                "description": "State-level price samples (lowercase 2-letter state code)",
                "count": state_count,
                "example": f"{BASE}/embed/data/ca.json",
            },
        ],
        "static_html": {
            "hospital_pages": f"{BASE}/visibility/node-{{ccn}}.html",
            "state_pages": f"{BASE}/visibility/state-{{state}}.html",
        },
        "datasets": [
            {
                "name": "audit_hot.db",
                "url": f"{HF}/audit_hot.db",
                "description": "Hot search shard — 16 high-traffic CPT codes",
            },
            {
                "name": "audit_data.db",
                "url": f"{HF}/audit_data.db",
                "description": "Full price ledger — all ingested rows",
            },
            {
                "name": "dataset_manifest.json",
                "url": f"{HF}/dataset_manifest.json",
                "description": "Schema and row counts",
            },
        ],
        "source": "https://github.com/vladimir-io/healthspend",
        "license": "Open source — see GitHub repository",
    }


AGENT_SITEMAP_URLS = [
    (f"{BASE}/llms.txt", "0.9"),
    (f"{BASE}/llms-full.txt", "0.9"),
    (f"{BASE}/ai.txt", "0.8"),
    (f"{BASE}/developers.html", "0.8"),
    (f"{BASE}/api/v1/manifest.json", "0.8"),
    (f"{BASE}/api/v1/procedures.json", "0.8"),
    (f"{BASE}/api/v1/documentation.html", "0.8"),
]


def patch_sitemap() -> bool:
    text = SITEMAP.read_text(encoding="utf-8")
    changed = False
    for url, priority in AGENT_SITEMAP_URLS:
        if url in text:
            continue
        entry = f'    <url><loc>{url}</loc><priority>{priority}</priority></url>\n'
        text = text.replace(
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + entry,
        )
        changed = True
    if changed:
        SITEMAP.write_text(text, encoding="utf-8")
    return changed


def main() -> None:
    catalog = parse_cpt_catalog()
    node_count = len(list(NODES_DIR.glob("*.json"))) if NODES_DIR.is_dir() else 0
    state_count = len(list(EMBED_DIR.glob("*.json"))) if EMBED_DIR.is_dir() else 0

    procedures = build_procedures(catalog)
    manifest = build_manifest(node_count, state_count)

    OUT_PROCEDURES.write_text(json.dumps(procedures, indent=2) + "\n", encoding="utf-8")
    OUT_MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    sitemap_patched = patch_sitemap()

    print(f"Wrote {OUT_PROCEDURES} ({procedures['count']} procedures)")
    print(f"Wrote {OUT_MANIFEST} ({node_count} nodes, {state_count} states)")
    if sitemap_patched:
        print(f"Patched {SITEMAP} with agent URLs")


if __name__ == "__main__":
    main()
