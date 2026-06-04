#!/usr/bin/env python3
"""Refresh SEO visibility HTML: plain language + CTA to main app (no oracle jargon)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VIS = ROOT / "web" / "public" / "visibility"

SUBSCRIBE_BLOCK_RE = re.compile(
    r"<div style=\"background: #000; border-radius: 20px;.*?Zero Spam\. Strictly Data\.</p>\s*</div>\s*",
    re.DOTALL,
)

CTA_BLOCK = """
<div class="hs-cta" style="background:var(--bg-card, rgba(255,255,255,0.04)); border:1px solid var(--border, rgba(255,255,255,0.1)); border-radius:20px; padding:32px 24px; text-align:center; margin:48px 0;">
  <h2 style="color:#fff; font-size:1.35rem; font-weight:800; margin:0 0 10px; letter-spacing:-0.03em;">Compare full prices</h2>
  <p style="color:#888; font-size:0.95rem; line-height:1.55; max-width:440px; margin:0 auto 20px;">Search every published cash rate for this hospital and email them with the posted price.</p>
  <a href="https://healthspend.lol/#search" style="display:inline-block; background:linear-gradient(180deg,#FF6000 0%,#D95200 100%); color:#fff; padding:14px 28px; border-radius:12px; font-weight:800; font-size:0.9rem; text-decoration:none;">Open price search</a>
</div>
"""

REPLACEMENTS = [
    ("Hospital Pricing Oracle", "Hospital prices"),
    ("Institutional Node Instance:", "CCN"),
    ("Clinical Data Ledger", "Sample published prices"),
    ("Synchronized CY 2026", "CMS transparency filing"),
    ("Federal Transparency Audit", "Transparency score (CMS signals)"),
    ("Compliance Index", "Transparency score"),
    ("search terminal", "price search"),
    ("Clinical ledger provided by", "Prices from"),
]


def patch_file(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    orig = text
    for old, new in REPLACEMENTS:
        text = text.replace(old, new)
    text = SUBSCRIBE_BLOCK_RE.sub(CTA_BLOCK + "\n", text)
    if text == orig:
        return False
    path.write_text(text, encoding="utf-8")
    return True


def main() -> None:
    if not VIS.is_dir():
        print(f"Skip: {VIS} not found")
        return
    n = 0
    for p in VIS.glob("node-*.html"):
        if patch_file(p):
            n += 1
    print(f"Patched {n} visibility pages")


if __name__ == "__main__":
    main()
