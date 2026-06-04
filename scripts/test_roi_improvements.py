#!/usr/bin/env python3
"""Smoke tests for ROI pipeline improvements (steps 1–8)."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from cms_hpt_utils import CmsHptLocation, HospitalRow, normalize_name
from hospital_match_index import HospitalMatchIndex


def test_match_index_state_partition() -> None:
    hospitals = [
        HospitalRow("450001", "Houston Methodist", "TX", "Houston", normalize_name("Houston Methodist")),
        HospitalRow("360001", "Albany Med", "NY", "Albany", normalize_name("Albany Med")),
    ]
    idx = HospitalMatchIndex(hospitals)
    loc = CmsHptLocation("Houston Methodist, TX", "http://x.com/a.json", "", "", "", "450001", "")
    m = idx.resolve(loc, 0.80)
    assert m and m[0] == "450001", m


def test_incremental_merge_mode_env() -> None:
    import merge_pipeline_data as mp

    assert hasattr(mp, "merge_prices")


def test_bulk_import_module() -> None:
    import import_bulk_mrf_urls as bulk

    assert hasattr(bulk, "normalize_ccn")


def test_index_exists() -> None:
    p = ROOT / "data" / "mrf_url_index.sqlite"
    if not p.exists():
        print("  skip: no mrf_url_index.sqlite")
        return
    conn = sqlite3.connect(p)
    n = conn.execute("SELECT COUNT(DISTINCT ccn) FROM mrf_urls").fetchone()[0]
    conn.close()
    assert n > 0, "index empty"


def main() -> int:
    tests = [
        test_match_index_state_partition,
        test_incremental_merge_mode_env,
        test_bulk_import_module,
        test_index_exists,
    ]
    for t in tests:
        t()
        print(f"  ok {t.__name__}")
    print("✓ ROI smoke tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
