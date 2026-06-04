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


# US state hints in cms-hpt location-name lines (e.g. "Memorial Hermann, TX")
_STATE_NAME_TO_ABBR = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}
_VALID_STATE_ABBRS = set(_STATE_NAME_TO_ABBR.values())


def extract_state_hint(location_name: str) -> str:
    raw = location_name.strip()
    if not raw:
        return ""
    m = re.search(r",\s*([A-Za-z]{2})\s*(?:\d{5})?\s*$", raw)
    if m and m.group(1).upper() in _VALID_STATE_ABBRS:
        return m.group(1).upper()
    low = raw.lower()
    for name, abbr in sorted(_STATE_NAME_TO_ABBR.items(), key=lambda x: -len(x[0])):
        if re.search(rf"\b{re.escape(name)}\b", low):
            return abbr
    return ""


def match_ccn(
    location_name: str,
    hospitals: list[HospitalRow],
    min_score: float = 0.82,
) -> tuple[str, float] | None:
    target = normalize_name(location_name)
    if not target:
        return None

    state_hint = extract_state_hint(location_name)
    scored: list[tuple[float, str]] = []
    for h in hospitals:
        score = name_score(target, h.name_norm)
        if state_hint:
            if h.state == state_hint:
                score = min(0.99, score + 0.06)
            else:
                score *= 0.72
        scored.append((score, h.ccn))

    scored.sort(reverse=True)
    if not scored:
        return None

    best, best_ccn = scored[0]
    if best < min_score:
        return None

    if len(scored) > 1:
        second, second_ccn = scored[1]
        if second_ccn != best_ccn and (best - second) < 0.04:
            if not state_hint:
                return None
            best_state = next((h.state for h in hospitals if h.ccn == best_ccn), "")
            if best_state != state_hint:
                return None

    return best_ccn, best


def website_from_mrf_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
    return ""
