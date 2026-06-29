#!/usr/bin/env python3
"""Verify audit_data.db and audit_hot.db match published dataset descriptions."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sqlite3
import sys
from pathlib import Path

MIN_HOSPITALS = int(os.environ.get("HS_VERIFY_MIN_HOSPITALS", "5000"))
MIN_PRICES = int(os.environ.get("HS_VERIFY_MIN_PRICES", "10000"))
MIN_CPTS = int(os.environ.get("HS_VERIFY_MIN_CPTS", "20"))
MIN_HOT_HOSPITALS = int(os.environ.get("HS_VERIFY_MIN_HOT_HOSPITALS", "1000"))

_SCRIPTS = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("build_hot_db", _SCRIPTS / "build_hot_db.py")
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
HOT_CPTS: tuple[str, ...] = _mod.HOT_CPTS


def _cpt_set(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT cpt_code FROM prices WHERE cpt_code IS NOT NULL AND cpt_code != ''"
    ).fetchall()
    return {str(r[0]).strip() for r in rows}


def verify_full_db(path: Path) -> list[str]:
    errors: list[str] = []
    if not path.is_file():
        return [f"Missing file: {path}"]

    conn = sqlite3.connect(path)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    for required in ("hospitals", "compliance", "prices"):
        if required not in tables:
            errors.append(f"{path.name}: missing table {required}")

    price_rows = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    hospital_rows = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    cpts = _cpt_set(conn)

    if hospital_rows < MIN_HOSPITALS:
        errors.append(
            f"{path.name}: expected ≥{MIN_HOSPITALS:,} hospitals, got {hospital_rows:,}"
        )
    if price_rows < MIN_PRICES:
        errors.append(
            f"{path.name}: expected ≥{MIN_PRICES:,} price rows, got {price_rows:,}"
        )
    if len(cpts) < MIN_CPTS:
        errors.append(
            f"{path.name}: expected ≥{MIN_CPTS} CPTs, got {len(cpts)} — "
            "run build_public_db.sh or CI MRF ingest with HS_FULL_CPT_COVERAGE=1"
        )

    conn.close()
    print(f"✓ {path.name}: full ledger — {price_rows:,} prices, {hospital_rows:,} hospitals, {len(cpts):,} CPTs")
    return errors


def verify_hot_db(path: Path, full_path: Path | None) -> list[str]:
    errors: list[str] = []
    if not path.is_file():
        return [f"Missing file: {path}"]

    conn = sqlite3.connect(path)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    for required in ("hospitals", "compliance", "prices"):
        if required not in tables:
            errors.append(f"{path.name}: missing table {required}")

    cpts = _cpt_set(conn)
    extra = cpts - set(HOT_CPTS)
    missing = set(HOT_CPTS) - cpts

    if extra:
        errors.append(f"{path.name}: hot shard must only contain HOT_CPTS; found extra: {sorted(extra)[:5]}")
    if len(cpts) == 0:
        errors.append(f"{path.name}: no price rows")

    price_rows = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    hot_hospitals = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]

    if full_path and full_path.is_file():
        full = sqlite3.connect(full_path)
        full_prices = full.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        full_hospitals = full.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
        full.close()
        if price_rows > full_prices:
            errors.append(
                f"{path.name}: hot price rows cannot exceed full ({price_rows:,} > {full_prices:,})"
            )
        full_cpt_n = len(_cpt_set(sqlite3.connect(full_path)))
        if price_rows == full_prices and len(cpts) >= full_cpt_n:
            print(
                f"  note: {path.name} has same price row count as full — "
                "full build may only contain hot-path CPTs until MRF ingestion widens"
            )
        if hot_hospitals > full_hospitals:
            errors.append(
                f"{path.name}: hot hospitals cannot exceed full ({hot_hospitals:,} > {full_hospitals:,})"
            )
        elif hot_hospitals < MIN_HOT_HOSPITALS:
            errors.append(
                f"{path.name}: expected ≥{MIN_HOT_HOSPITALS:,} hot hospitals, got {hot_hospitals:,}"
            )
        elif hot_hospitals < full_hospitals:
            print(
                f"  note: {path.name} slim hospital set — {hot_hospitals:,} of {full_hospitals:,} "
                "(hospitals with hot CPT prices only)"
            )

    conn.close()
    status = "ok" if not missing else f"partial CPT coverage ({len(missing)} missing from HOT_CPTS)"
    print(f"✓ {path.name}: hot shard — {price_rows:,} prices, CPTs {len(cpts)}/{len(HOT_CPTS)} ({status})")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Healthspend SQLite dataset artifacts")
    parser.add_argument("--full", type=Path, default=Path("web/public/audit_data.db"))
    parser.add_argument("--hot", type=Path, default=Path("web/public/audit_hot.db"))
    args = parser.parse_args()

    errors: list[str] = []
    errors.extend(verify_full_db(args.full))
    errors.extend(verify_hot_db(args.hot, args.full))

    if errors:
        for e in errors:
            print(f"✗ {e}", file=sys.stderr)
        return 1
    print("All dataset checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
