#!/usr/bin/env python3
"""Generate programmatic procedure×state comparison landing pages for SEO."""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date
from html import escape
from pathlib import Path
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parents[1]
COMPARE_DIR = ROOT / "web" / "public" / "compare"
SITEMAP = ROOT / "web" / "public" / "sitemap-compare.xml"
DB_PATH = ROOT / "web" / "public" / "audit_hot.db"
if not DB_PATH.is_file():
    DB_PATH = ROOT / "web" / "public" / "audit_data.db"

BASE = "https://healthspend.lol"
OG_IMAGE = f"{BASE}/og-image.png"
YEAR = date.today().year

STATE_NAMES = {
    "al": "Alabama", "ak": "Alaska", "az": "Arizona", "ar": "Arkansas",
    "ca": "California", "co": "Colorado", "ct": "Connecticut", "de": "Delaware",
    "fl": "Florida", "ga": "Georgia", "hi": "Hawaii", "id": "Idaho",
    "il": "Illinois", "in": "Indiana", "ia": "Iowa", "ks": "Kansas",
    "ky": "Kentucky", "la": "Louisiana", "me": "Maine", "md": "Maryland",
    "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota", "ms": "Mississippi",
    "mo": "Missouri", "mt": "Montana", "ne": "Nebraska", "nv": "Nevada",
    "nh": "New Hampshire", "nj": "New Jersey", "nm": "New Mexico", "ny": "New York",
    "nc": "North Carolina", "nd": "North Dakota", "oh": "Ohio", "ok": "Oklahoma",
    "or": "Oregon", "pa": "Pennsylvania", "ri": "Rhode Island", "sc": "South Carolina",
    "sd": "South Dakota", "tn": "Tennessee", "tx": "Texas", "ut": "Utah",
    "vt": "Vermont", "va": "Virginia", "wa": "Washington", "wv": "West Virginia",
    "wi": "Wisconsin", "wy": "Wyoming",
}

# slug, CPT, short label, SEO phrase
PROCEDURES = (
    ("mri", "70551", "Brain MRI", "MRI"),
    ("knee-mri", "73721", "Knee MRI", "knee MRI"),
    ("colonoscopy", "45378", "Colonoscopy", "colonoscopy"),
    ("ct-scan", "74177", "CT Scan", "CT scan"),
    ("chest-xray", "71045", "Chest X-Ray", "chest X-ray"),
    ("blood-panel", "80053", "Metabolic Panel", "blood panel"),
    ("er-visit", "99283", "ER Visit (Level 3)", "ER visit"),
    ("knee-replacement", "27447", "Knee Replacement", "knee replacement"),
)


def money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"${value:,.0f}"


def query_state_stats(conn: sqlite3.Connection, state: str, cpt: str) -> dict:
    row = conn.execute(
        """
        SELECT COUNT(*), MIN(p.cash_price), MAX(p.cash_price)
        FROM prices p
        JOIN hospitals h ON h.ccn = p.ein
        WHERE h.state = ? AND p.cpt_code = ? AND p.cash_price > 0
        """,
        (state.upper(), cpt),
    ).fetchone()
    count, mn, mx = row if row else (0, None, None)
    median = None
    if count and count > 0:
        med = conn.execute(
            """
            SELECT p.cash_price FROM prices p
            JOIN hospitals h ON h.ccn = p.ein
            WHERE h.state = ? AND p.cpt_code = ? AND p.cash_price > 0
            ORDER BY p.cash_price LIMIT 1 OFFSET ?
            """,
            (state.upper(), cpt, max(0, count // 2)),
        ).fetchone()
        median = med[0] if med else None
    rows = conn.execute(
        """
        SELECT h.name, h.city, p.cash_price
        FROM prices p
        JOIN hospitals h ON h.ccn = p.ein
        WHERE h.state = ? AND p.cpt_code = ? AND p.cash_price > 0
        ORDER BY p.cash_price ASC
        LIMIT 5
        """,
        (state.upper(), cpt),
    ).fetchall()
    return {"count": count, "min": mn, "max": mx, "median": median, "samples": rows}


def search_href(state: str, label: str, medium: str) -> str:
    q = quote_plus(label)
    return (
        f"{BASE}/?q={q}&state={state.upper()}"
        f"&utm_source=compare&utm_medium={medium}#search"
    )


def render_page(
    slug: str,
    cpt: str,
    label: str,
    phrase: str,
    state_code: str,
    state_name: str,
    stats: dict,
) -> str:
    filename = f"{slug}-cost-in-{state_code}.html"
    canonical = f"{BASE}/compare/{filename}"
    title = f"{phrase.title()} Cost in {state_name} ({YEAR}) — Hospital Cash Prices"
    desc = (
        f"Compare published hospital cash prices for {label} in {state_name}. "
        f"See lowest cash rates hospitals posted under CMS price transparency rules."
    )
    cta = search_href(state_code, label, slug)

    stats_bits = []
    if stats["count"]:
        stats_bits.append(f"<strong>{stats['count']}</strong> hospitals with published cash rates")
        if stats["min"] is not None and stats["max"] is not None:
            stats_bits.append(
                f"range <strong>{money(stats['min'])}</strong> – <strong>{money(stats['max'])}</strong>"
            )
        if stats["median"] is not None:
            stats_bits.append(f"median <strong>{money(stats['median'])}</strong>")
    stats_html = (
        " · ".join(stats_bits)
        if stats_bits
        else "Search live published cash rates — hospitals must post prices under federal transparency rules."
    )

    rows_html = ""
    for name, city, price in stats["samples"]:
        loc = f"{city}, {state_code.upper()}" if city else state_code.upper()
        rows_html += (
            f'<div class="row"><span>{escape(name)} <span class="muted">({escape(loc)})</span></span>'
            f'<span class="price">{money(price)}</span></div>\n'
        )
    if not rows_html:
        rows_html = (
            '<p class="muted">No cached rows for this state yet — open search for live hospital filings.</p>'
        )

    schema = {
        "@context": "https://schema.org",
        "@type": "MedicalWebPage",
        "name": title,
        "description": desc,
        "url": canonical,
        "about": {"@type": "MedicalProcedure", "name": label, "procedureCode": cpt},
    }

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape(title)}</title>
  <meta name="description" content="{escape(desc)}">
  <link rel="canonical" href="{canonical}">
  <meta property="og:title" content="{escape(title)}">
  <meta property="og:description" content="{escape(desc)}">
  <meta property="og:url" content="{canonical}">
  <meta property="og:type" content="website">
  <meta property="og:image" content="{OG_IMAGE}">
  <meta name="robots" content="index,follow">
  <script type="application/ld+json">{json.dumps(schema)}</script>
  <style>
    :root {{ --orange: #FF6000; --bg: #050505; }}
    body {{ font-family: Inter, system-ui, sans-serif; background: var(--bg); color: #eee;
      max-width: 820px; margin: 0 auto; padding: 40px 20px; line-height: 1.6; }}
    h1 {{ font-size: clamp(1.8rem, 4vw, 2.6rem); font-weight: 900; letter-spacing: -0.03em; color: #fff; }}
    a {{ color: var(--orange); font-weight: 700; text-decoration: none; }}
    .pill {{ display: inline-block; background: rgba(255,96,0,.12); color: var(--orange);
      padding: 6px 12px; border-radius: 999px; font-size: .75rem; font-weight: 800;
      text-transform: uppercase; margin-bottom: 16px; }}
    .card {{ background: rgba(255,255,255,.04); border: 1px solid rgba(255,255,255,.08);
      border-radius: 16px; padding: 20px; margin: 28px 0; }}
    .row {{ display: flex; justify-content: space-between; gap: 12px; padding: 10px 0;
      border-bottom: 1px solid rgba(255,255,255,.06); font-size: .92rem; }}
    .price {{ color: var(--orange); font-weight: 800; font-family: ui-monospace, monospace; white-space: nowrap; }}
    .muted {{ color: #888; }}
    .cta {{ display: inline-block; margin-top: 20px; background: linear-gradient(180deg,#FF6000,#D95200);
      color: #fff; padding: 14px 26px; border-radius: 12px; font-weight: 800; }}
    .links {{ margin-top: 48px; font-size: .85rem; color: #666; }}
  </style>
</head>
<body>
  <div class="pill">{escape(state_name)} · CPT {cpt}</div>
  <h1>{escape(phrase.title())} cost in {escape(state_name)}</h1>
  <p class="muted">{stats_html}</p>
  <div class="card">
    <h2 style="margin:0 0 12px; font-size:1rem; color:#aaa;">Lowest published cash rates</h2>
    {rows_html}
  </div>
  <a class="cta" href="{cta}">Search all {escape(label)} prices in {escape(state_name)}</a>
  <p class="links">
    <a href="{BASE}/visibility/state-{state_code}.html">All {escape(state_name)} hospitals</a>
    · <a href="{BASE}/">Healthspend home</a>
  </p>
</body>
</html>
"""


def write_sitemap(urls: list[str]) -> None:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for url in urls:
        lines.append(f"  <url><loc>{url}</loc><priority>0.85</priority></url>")
    lines.append("</urlset>")
    SITEMAP.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    COMPARE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH) if DB_PATH.is_file() else None
    urls: list[str] = []
    written = 0

    for state_code, state_name in STATE_NAMES.items():
        for slug, cpt, label, phrase in PROCEDURES:
            stats = (
                query_state_stats(conn, state_code, cpt)
                if conn
                else {"count": 0, "min": None, "max": None, "median": None, "samples": []}
            )
            html = render_page(slug, cpt, label, phrase, state_code, state_name, stats)
            out = COMPARE_DIR / f"{slug}-cost-in-{state_code}.html"
            out.write_text(html, encoding="utf-8")
            urls.append(f"{BASE}/compare/{out.name}")
            written += 1

    if conn:
        conn.close()

    write_sitemap(urls)
    print(f"✓ compare pages: {written} → {COMPARE_DIR}")
    print(f"✓ sitemap: {SITEMAP} ({len(urls)} URLs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
