"""Shared cms-hpt.txt parsing and hospital name matching."""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from difflib import SequenceMatcher

MRF_EXT_MARKERS = (".json", ".csv", ".ashx", ".zip", ".gz", ".txt", "standardcharges")


@dataclass
class CmsHptLocation:
    location_name: str
    mrf_url: str
    source_page_url: str
    cms_hpt_url: str
    website: str


@dataclass
class HospitalRow:
    ccn: str
    name: str
    state: str
    city: str
    name_norm: str


def normalize_name(raw: str) -> str:
    s = raw.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    for tok in (
        "hospital",
        "medical",
        "center",
        "centre",
        "health",
        "healthcare",
        "system",
        "regional",
        "memorial",
        "llc",
        "inc",
        "corp",
        "the",
        "dba",
    ):
        s = re.sub(rf"\b{tok}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_cms_hpt_locations(body: str, cms_hpt_url: str, website: str) -> list[CmsHptLocation]:
    """Parse multi-hospital cms-hpt.txt (location-name blocks)."""
    current: dict[str, str] = {}
    out: list[CmsHptLocation] = []

    def flush() -> None:
        mrf = (current.get("mrf_url") or "").strip()
        if not mrf or not mrf.startswith("http"):
            return
        out.append(
            CmsHptLocation(
                location_name=(current.get("location_name") or "").strip(),
                mrf_url=mrf,
                source_page_url=(current.get("source_page_url") or "").strip(),
                cms_hpt_url=cms_hpt_url,
                website=website,
            )
        )

    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("*"):
            continue
        lower = line.lower()
        if lower.startswith("location-name:"):
            flush()
            current = {"location_name": line.split(":", 1)[1].strip()}
            continue
        if lower.startswith("mrf-url:"):
            val = line.split(":", 1)[1].strip()
            if val.startswith("http"):
                current["mrf_url"] = val
            continue
        if lower.startswith("source-page-url:"):
            val = line.split(":", 1)[1].strip()
            if val.startswith("http"):
                current["source_page_url"] = val
            continue
        if line.startswith("http") and any(m in lower for m in MRF_EXT_MARKERS):
            current.setdefault("mrf_url", line.split(",")[0].strip())

    flush()

    if not out and body.strip():
        for match in re.findall(r"https?://[^\s\"'<>]+", body, flags=re.IGNORECASE):
            low = match.lower()
            if any(m in low for m in MRF_EXT_MARKERS):
                out.append(
                    CmsHptLocation(
                        location_name="",
                        mrf_url=match.strip(),
                        source_page_url="",
                        cms_hpt_url=cms_hpt_url,
                        website=website,
                    )
                )
                break

    return out


def name_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 0.92
    return SequenceMatcher(None, a, b).ratio()


def match_ccn(
    location_name: str,
    hospitals: list[HospitalRow],
    min_score: float = 0.86,
) -> tuple[str, float] | None:
    target = normalize_name(location_name)
    if not target:
        return None

    best_ccn = ""
    best = 0.0
    for h in hospitals:
        score = name_score(target, h.name_norm)
        if score > best:
            best = score
            best_ccn = h.ccn

    if best >= min_score and best_ccn:
        return best_ccn, best
    return None


def website_from_mrf_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
    return ""
