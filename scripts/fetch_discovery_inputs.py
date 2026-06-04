#!/usr/bin/env python3
"""Download CMS hospital enrollments (CCN→NPI) and optional NPPES endpoint subset."""

from __future__ import annotations

import argparse
import csv
import io
import re
import zipfile
from pathlib import Path

import requests

ENROLLMENTS_URL = (
    "https://data.cms.gov/sites/default/files/2026-05/"
    "4c668d34-e45a-4b9e-b5f7-dec7f1c333e1/Hospital_Enrollments_2026.05.01.csv"
)
NPPES_ZIP_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_May_2026_V2.zip"
ENDPOINT_CSV_NAME = "endpoint_pfile_20050523-20260510.csv"

OUT_DIR = Path("data/discovery")


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  reuse {dest}")
        return
    print(f"  download {url}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
    print(f"  wrote {dest} ({dest.stat().st_size:,} bytes)")


def load_hospital_npis(enrollments: Path) -> set[str]:
    npis: set[str] = set()
    with enrollments.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in csv.DictReader(f):
            npi = re.sub(r"\D", "", row.get("NPI") or "")
            if len(npi) == 10:
                npis.add(npi)
    return npis


def build_nppes_endpoint_subset(
    enrollments: Path, zip_path: Path, out_csv: Path
) -> int:
    hospital_npis = load_hospital_npis(enrollments)
    print(f"  hospital NPIs from enrollments: {len(hospital_npis):,}")

    written = 0
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf, out_csv.open("w", encoding="utf-8", newline="") as out:
        writer = None
        with zf.open(ENDPOINT_CSV_NAME) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
            reader = csv.reader(text)
            header = next(reader)
            writer = csv.writer(out)
            writer.writerow(header)
            idx_npi = header.index("NPI")
            idx_endpoint = header.index("Endpoint")
            idx_type = header.index("Endpoint Type")
            for row in reader:
                if len(row) <= max(idx_npi, idx_endpoint, idx_type):
                    continue
                npi = re.sub(r"\D", "", row[idx_npi])
                if npi not in hospital_npis:
                    continue
                endpoint = (row[idx_endpoint] or "").strip()
                etype = (row[idx_type] or "").strip().upper()
                if not endpoint.startswith("http"):
                    continue
                if etype in {"DIRECT"}:
                    continue
                writer.writerow(row)
                written += 1
    print(f"  wrote {written:,} HTTP endpoint rows → {out_csv}")
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch discovery enrichment files")
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    parser.add_argument(
        "--with-nppes",
        action="store_true",
        help="Also download NPPES zip and build endpoint subset (~1GB download)",
    )
    args = parser.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    enrollments = out_dir / "hospital_enrollments.csv"
    download(ENROLLMENTS_URL, enrollments)

    if args.with_nppes:
        zip_path = out_dir / "nppes_dissemination.zip"
        download(NPPES_ZIP_URL, zip_path)
        build_nppes_endpoint_subset(
            enrollments, zip_path, out_dir / "nppes_endpoints_subset.csv"
        )

    print("✓ Discovery inputs ready in", out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
