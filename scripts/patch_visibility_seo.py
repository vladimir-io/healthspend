#!/usr/bin/env python3
"""Refresh SEO visibility HTML: plain language, canonical/OG tags, and app CTAs."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VIS = ROOT / "web" / "public" / "visibility"
TRANSPARENCY_INDEX = ROOT / "web" / "public" / "transparency-index.html"
BASE = "https://healthspend.lol"
OG_IMAGE = f"{BASE}/og-image.png"

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

SUBSCRIBE_BLOCK_RE = re.compile(
    r"<div style=\"background: #000; border-radius: 20px;.*?Zero Spam\. Strictly Data\.</p>\s*</div>\s*",
    re.DOTALL,
)

TITLE_RE = re.compile(r"<title>([^<]+)</title>", re.IGNORECASE)
DESC_RE = re.compile(
    r'<meta\s+name="description"\s+content="([^"]*)"',
    re.IGNORECASE,
)
H1_RE = re.compile(r"<h1[^>]*>([^<]+)</h1>", re.IGNORECASE)
CITY_STATE_RE = re.compile(r"([A-Z][A-Za-z .'-]+),\s*([A-Z]{2})\s")

REPLACEMENTS = [
    ("Hospital Pricing Oracle", "Hospital prices"),
    ("Pricing Oracle", "Hospital prices"),
    ("Institutional Node Instance:", "CCN"),
    ("Institutional Node Index", "Hospital directory"),
    ("National Oracle Index", "National price transparency directory"),
    ("Clinical Data Ledger", "Sample published prices"),
    ("Clinical Price Ledger", "Sample published prices"),
    ("Synchronized CY 2026", "CMS transparency filing"),
    ("Federal Transparency Audit", "Transparency score (CMS signals)"),
    ("Compliance Index", "Transparency score"),
    ("search terminal", "price search"),
    ("Clinical ledger provided by", "Prices from"),
    ("distributed data node", "hospital price page"),
    ("audit nodes", "hospital pages"),
    ("Audit Node", "Hospital prices"),
    ("Healthcare Pricing Ledger", "Hospital price transparency data"),
]

SCORE_VAL_RE = re.compile(r'<div class="score-val">(\d+)')
VIOLATION_BLOCK_RE = re.compile(
    r'\s*<a href="https://twitter\.com/intent/tweet\?[^"]*"[^>]*class="(?:oracle-btn|cta-btn)"[^>]*>'
    r'Report Violation</a>\s*',
    re.IGNORECASE,
)


def patch_violation_link(text: str) -> str:
    """Drop misleading violation tweets on hospitals that score ≥50."""
    score_m = SCORE_VAL_RE.search(text)
    score = int(score_m.group(1)) if score_m else 0
    if score >= 50:
        text = VIOLATION_BLOCK_RE.sub("\n", text)
    text = text.replace('class="oracle-btn"', 'class="cta-btn"')
    text = text.replace(".oracle-btn", ".cta-btn")
    return text


CTA_BLOCK = """
<div class="hs-cta" style="background:var(--bg-card, rgba(255,255,255,0.04)); border:1px solid var(--border, rgba(255,255,255,0.1)); border-radius:20px; padding:32px 24px; text-align:center; margin:48px 0;">
  <h2 style="color:#fff; font-size:1.35rem; font-weight:800; margin:0 0 10px; letter-spacing:-0.03em;">Compare full prices</h2>
  <p style="color:#888; font-size:0.95rem; line-height:1.55; max-width:440px; margin:0 auto 20px;">Search every published cash rate for this hospital and email them with the posted price.</p>
  <a href="{cta_href}" style="display:inline-block; background:linear-gradient(180deg,#FF6000 0%,#D95200 100%); color:#fff; padding:14px 28px; border-radius:12px; font-weight:800; font-size:0.9rem; text-decoration:none;">Open price search</a>
</div>
"""


def _quote_plus(value: str) -> str:
    from urllib.parse import quote_plus
    return quote_plus(value)


def with_utm(href: str, medium: str) -> str:
    if "utm_source=" in href:
        return href
    base, frag = (href.split("#", 1) + [""])[:2]
    sep = "&" if "?" in base else "?"
    out = f"{base}{sep}utm_source=visibility&utm_medium={medium}"
    if frag:
        out += f"#{frag}"
    return out


def ensure_head_tags(text: str, *, canonical: str, og_title: str, og_desc: str) -> str:
    if 'rel="canonical"' not in text:
        block = (
            f'    <link rel="canonical" href="{canonical}">\n'
            f'    <meta property="og:title" content="{og_title}">\n'
            f'    <meta property="og:description" content="{og_desc}">\n'
            f'    <meta property="og:url" content="{canonical}">\n'
            f'    <meta property="og:type" content="website">\n'
            f'    <meta property="og:image" content="{OG_IMAGE}">\n'
            f'    <meta name="twitter:card" content="summary_large_image">\n'
            f'    <meta name="twitter:title" content="{og_title}">\n'
            f'    <meta name="twitter:description" content="{og_desc}">\n'
            f'    <meta name="twitter:image" content="{OG_IMAGE}">\n'
            f'    <meta name="robots" content="index,follow,max-image-preview:large">\n'
        )
        text = re.sub(
            r'(<meta\s+name="viewport"[^>]*>\s*)',
            r"\1" + block,
            text,
            count=1,
            flags=re.IGNORECASE,
        )
    return text


def patch_json_ld(text: str) -> str:
    return text.replace("Clinical Price Ledger", "Sample published prices")


def apply_replacements(text: str) -> str:
    for old, new in REPLACEMENTS:
        text = text.replace(old, new)
    text = SUBSCRIBE_BLOCK_RE.sub("", text)
    return patch_json_ld(text)


def node_cta_href(path: Path, text: str) -> str:
    ccn = path.stem.replace("node-", "")
    h1 = (H1_RE.search(text).group(1).strip() if H1_RE.search(text) else "")
    state = ""
    m = CITY_STATE_RE.search(text)
    if m:
        state = m.group(2)
    if h1:
        q = _quote_plus(h1)
        href = f"{BASE}/?q={q}"
        if state:
            href += f"&state={state}"
        return with_utm(href + "#search", "hospital")
    return with_utm(f"{BASE}/#search", "hospital")


def patch_node(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    orig = text
    text = apply_replacements(text)
    text = patch_violation_link(text)

    canonical = f"{BASE}/visibility/{path.name}"
    title_m = TITLE_RE.search(text)
    desc_m = DESC_RE.search(text)
    og_title = (title_m.group(1).strip() if title_m else path.stem)
    og_desc = (desc_m.group(1).strip() if desc_m else f"Published hospital prices for {path.stem}.")
    text = ensure_head_tags(text, canonical=canonical, og_title=og_title, og_desc=og_desc)

    if 'class="hs-cta"' not in text:
        cta = CTA_BLOCK.format(cta_href=node_cta_href(path, text))
        if "</body>" in text:
            text = text.replace("</body>", cta + "\n</body>", 1)
    else:
        href = node_cta_href(path, text)
        text = re.sub(
            r'(<div class="hs-cta"[^>]*>.*?<a href=")[^"]+(")',
            rf"\1{href}\2",
            text,
            count=1,
            flags=re.DOTALL,
        )

    if text != orig:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def patch_state(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    orig = text
    code = path.stem.replace("state-", "").lower()
    state_name = STATE_NAMES.get(code, code.upper())

    text = apply_replacements(text)
    text = patch_violation_link(text)
    text = text.replace(f"{state_name} Pricing Oracle", f"Hospital prices in {state_name}")
    text = text.replace(f"{state_name} Hospital Price Transparency Index", f"Hospital prices in {state_name}")

    canonical = f"{BASE}/visibility/{path.name}"
    og_title = f"Hospital prices in {state_name} | Healthspend"
    og_desc = f"Compare published hospital cash rates and transparency scores in {state_name}."
    text = ensure_head_tags(text, canonical=canonical, og_title=og_title, og_desc=og_desc)

    if 'class="hs-cta"' not in text:
        cta_href = with_utm(f"{BASE}/?state={code.upper()}#search", "state")
        cta = CTA_BLOCK.format(cta_href=cta_href).replace(
            "for this hospital", f"in {state_name}"
        )
        if "</body>" in text:
            text = text.replace("</body>", cta + "\n</body>", 1)
    else:
        cta_href = with_utm(f"{BASE}/?state={code.upper()}#search", "state")
        text = re.sub(
            r'(<div class="hs-cta"[^>]*>.*?<a href=")[^"]+(")',
            rf"\1{cta_href}\2",
            text,
            count=1,
            flags=re.DOTALL,
        )

    if text != orig:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def patch_transparency_index() -> bool:
    if not TRANSPARENCY_INDEX.is_file():
        return False
    text = TRANSPARENCY_INDEX.read_text(encoding="utf-8")
    orig = text
    text = apply_replacements(text)
    if text != orig:
        TRANSPARENCY_INDEX.write_text(text, encoding="utf-8")
        return True
    return False


def main() -> int:
    if not VIS.is_dir():
        print(f"Skip: {VIS} not found", file=sys.stderr)
        return 0

    nodes = n_states = 0
    for p in VIS.glob("node-*.html"):
        if patch_node(p):
            nodes += 1
    for p in VIS.glob("state-*.html"):
        if patch_state(p):
            n_states += 1
    idx = 1 if patch_transparency_index() else 0

    print(f"Patched visibility: {nodes} node pages, {n_states} state pages, index={idx}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
