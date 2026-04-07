#!/usr/bin/env python3
"""
Load NPPES v2 data files into prices.db reference tables.

Supports:
- Core NPI file (npidata v2)
- Practice location reference file
- Other name / DBA reference file
- Monthly deactivation file
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BATCH_SIZE = 5000


def norm_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def get_first(row: Dict[str, str], fieldnames: Sequence[str], candidates: Sequence[str]) -> str:
    if not fieldnames:
        return ""

    index = {norm_key(name): name for name in fieldnames}
    for cand in candidates:
        key = norm_key(cand)
        if key in index:
            raw = row.get(index[key], "")
            return (raw or "").strip()
    return ""


def iter_csv(path: Path) -> Iterable[Tuple[Dict[str, str], List[str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            yield row, fieldnames


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS nppes_provider_core (
            npi TEXT PRIMARY KEY,
            entity_type TEXT,
            org_name TEXT,
            primary_taxonomy TEXT,
            practice_state TEXT,
            practice_city TEXT,
            practice_postal_code TEXT,
            accessibility TEXT,
            secondary_languages TEXT,
            direct_email TEXT,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS nppes_practice_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT NOT NULL,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            phone TEXT,
            is_primary INTEGER DEFAULT 0,
            FOREIGN KEY (npi) REFERENCES nppes_provider_core(npi)
        );

        CREATE INDEX IF NOT EXISTS idx_nppes_practice_locations_npi ON nppes_practice_locations(npi);

        CREATE TABLE IF NOT EXISTS nppes_other_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT NOT NULL,
            other_name TEXT NOT NULL,
            type_code TEXT,
            FOREIGN KEY (npi) REFERENCES nppes_provider_core(npi)
        );

        CREATE INDEX IF NOT EXISTS idx_nppes_other_names_npi ON nppes_other_names(npi);

        CREATE TABLE IF NOT EXISTS nppes_deactivations (
            npi TEXT PRIMARY KEY,
            deactivation_date TEXT,
            reactivation_date TEXT,
            reason_code TEXT,
            reason_text TEXT
        );

        CREATE TABLE IF NOT EXISTS nppes_endpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT NOT NULL,
            endpoint_url TEXT NOT NULL,
            endpoint_type TEXT,
            use_case TEXT,
            affiliation TEXT,
            last_seen TEXT,
            UNIQUE(npi, endpoint_url)
        );

        CREATE INDEX IF NOT EXISTS idx_nppes_endpoints_npi ON nppes_endpoints(npi);

        CREATE TABLE IF NOT EXISTS cms_attestations (
            ccn TEXT PRIMARY KEY,
            attester_name TEXT,
            attester_npi TEXT,
            attestation_date TEXT,
            source_file TEXT,
            last_seen TEXT
        );

        CREATE TABLE IF NOT EXISTS npi_audit_findings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            ccn TEXT NOT NULL,
            npi TEXT NOT NULL,
            finding_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            reason_code TEXT,
            notes TEXT,
            UNIQUE(snapshot_date, ccn, npi, finding_type)
        );
        """
    )


def load_core(conn: sqlite3.Connection, path: Path) -> int:
    rows = []
    count = 0
    for row, fields in iter_csv(path):
        npi = get_first(row, fields, ["NPI", "NPI Number"])
        if not npi:
            continue

        entity_type = get_first(row, fields, ["Entity Type Code", "Entity Type"])
        org_name = get_first(
            row,
            fields,
            [
                "Provider Organization Name (Legal Business Name)",
                "Organization Name",
                "Provider Name",
            ],
        )
        primary_taxonomy = get_first(
            row,
            fields,
            [
                "Healthcare Provider Taxonomy Code_1",
                "Provider Taxonomy Code 1",
                "Taxonomy Code",
            ],
        )
        practice_state = get_first(
            row,
            fields,
            ["Provider Business Practice Location Address State Name", "Practice State"],
        )
        practice_city = get_first(
            row,
            fields,
            ["Provider Business Practice Location Address City Name", "Practice City"],
        )
        practice_postal = get_first(
            row,
            fields,
            ["Provider Business Practice Location Address Postal Code", "Practice Postal Code"],
        )

        accessibility = get_first(
            row,
            fields,
            ["Accessibility", "Accessibility Indicator", "Is Accessibility Enabled"],
        )
        secondary_languages = get_first(
            row,
            fields,
            ["Secondary Languages", "Languages", "Languages Spoken"],
        )
        direct_email = get_first(
            row,
            fields,
            ["Provider Business Practice Location Address Electronic Mail Address", "Direct Email", "Email"],
        )
        last_updated = get_first(row, fields, ["Last Update Date", "Enumeration Date"])

        rows.append(
            (
                npi,
                entity_type,
                org_name,
                primary_taxonomy,
                practice_state,
                practice_city,
                practice_postal,
                accessibility,
                secondary_languages,
                direct_email,
                last_updated,
            )
        )
        if len(rows) >= BATCH_SIZE:
            conn.executemany(
                """
                INSERT INTO nppes_provider_core (
                    npi, entity_type, org_name, primary_taxonomy,
                    practice_state, practice_city, practice_postal_code,
                    accessibility, secondary_languages, direct_email, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(npi) DO UPDATE SET
                    entity_type=excluded.entity_type,
                    org_name=excluded.org_name,
                    primary_taxonomy=excluded.primary_taxonomy,
                    practice_state=excluded.practice_state,
                    practice_city=excluded.practice_city,
                    practice_postal_code=excluded.practice_postal_code,
                    accessibility=excluded.accessibility,
                    secondary_languages=excluded.secondary_languages,
                    direct_email=excluded.direct_email,
                    last_updated=excluded.last_updated
                """,
                rows,
            )
            count += len(rows)
            rows.clear()

    if rows:
        conn.executemany(
            """
            INSERT INTO nppes_provider_core (
                npi, entity_type, org_name, primary_taxonomy,
                practice_state, practice_city, practice_postal_code,
                accessibility, secondary_languages, direct_email, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(npi) DO UPDATE SET
                entity_type=excluded.entity_type,
                org_name=excluded.org_name,
                primary_taxonomy=excluded.primary_taxonomy,
                practice_state=excluded.practice_state,
                practice_city=excluded.practice_city,
                practice_postal_code=excluded.practice_postal_code,
                accessibility=excluded.accessibility,
                secondary_languages=excluded.secondary_languages,
                direct_email=excluded.direct_email,
                last_updated=excluded.last_updated
            """,
            rows,
        )
        count += len(rows)
    return count


def load_practice_locations(conn: sqlite3.Connection, path: Path) -> int:
    conn.execute("DELETE FROM nppes_practice_locations")
    rows = []
    count = 0
    for row, fields in iter_csv(path):
        npi = get_first(row, fields, ["NPI", "NPI Number"])
        if not npi:
            continue
        rows.append(
            (
                npi,
                get_first(row, fields, ["Address Line 1", "Practice Location Address Line 1"]),
                get_first(row, fields, ["Address Line 2", "Practice Location Address Line 2"]),
                get_first(row, fields, ["City", "Practice City"]),
                get_first(row, fields, ["State", "Practice State"]),
                get_first(row, fields, ["Postal Code", "Practice Postal Code"]),
                get_first(row, fields, ["Phone", "Practice Phone"]),
                1 if get_first(row, fields, ["Primary Location", "Is Primary"]).lower() in {"y", "yes", "1", "true"} else 0,
            )
        )
        if len(rows) >= BATCH_SIZE:
            conn.executemany(
                """
                INSERT INTO nppes_practice_locations (
                    npi, address_1, address_2, city, state, postal_code, phone, is_primary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            count += len(rows)
            rows.clear()

    if rows:
        conn.executemany(
            """
            INSERT INTO nppes_practice_locations (
                npi, address_1, address_2, city, state, postal_code, phone, is_primary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        count += len(rows)
    return count


def load_other_names(conn: sqlite3.Connection, path: Path) -> int:
    conn.execute("DELETE FROM nppes_other_names")
    rows = []
    count = 0
    for row, fields in iter_csv(path):
        npi = get_first(row, fields, ["NPI", "NPI Number"])
        other_name = get_first(row, fields, ["Other Provider Organization Name", "Other Name", "DBA Name"])
        if not npi or not other_name:
            continue

        rows.append(
            (
                npi,
                other_name,
                get_first(row, fields, ["Other Name Type Code", "Type Code"]),
            )
        )
        if len(rows) >= BATCH_SIZE:
            conn.executemany(
                "INSERT INTO nppes_other_names (npi, other_name, type_code) VALUES (?, ?, ?)",
                rows,
            )
            count += len(rows)
            rows.clear()

    if rows:
        conn.executemany(
            "INSERT INTO nppes_other_names (npi, other_name, type_code) VALUES (?, ?, ?)",
            rows,
        )
        count += len(rows)
    return count


def reason_text(code: str) -> str:
    code = (code or "").strip()
    return {
        "1": "Death",
        "2": "Retirement",
        "3": "Failure to revalidate",
        "4": "Misused/Identity Theft",
        "5": "Other",
    }.get(code, "Unknown")


def load_deactivations(conn: sqlite3.Connection, path: Path) -> int:
    rows = []
    count = 0
    for row, fields in iter_csv(path):
        npi = get_first(row, fields, ["NPI", "NPI Number"])
        if not npi:
            continue
        reason_code = get_first(row, fields, ["Deactivation Reason Code", "Reason Code"])
        rows.append(
            (
                npi,
                get_first(row, fields, ["Deactivation Date", "NPI Deactivation Date"]),
                get_first(row, fields, ["Reactivation Date", "NPI Reactivation Date"]),
                reason_code,
                reason_text(reason_code),
            )
        )
        if len(rows) >= BATCH_SIZE:
            conn.executemany(
                """
                INSERT INTO nppes_deactivations (
                    npi, deactivation_date, reactivation_date, reason_code, reason_text
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(npi) DO UPDATE SET
                    deactivation_date=excluded.deactivation_date,
                    reactivation_date=excluded.reactivation_date,
                    reason_code=excluded.reason_code,
                    reason_text=excluded.reason_text
                """,
                rows,
            )
            count += len(rows)
            rows.clear()

    if rows:
        conn.executemany(
            """
            INSERT INTO nppes_deactivations (
                npi, deactivation_date, reactivation_date, reason_code, reason_text
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(npi) DO UPDATE SET
                deactivation_date=excluded.deactivation_date,
                reactivation_date=excluded.reactivation_date,
                reason_code=excluded.reason_code,
                reason_text=excluded.reason_text
            """,
            rows,
        )
        count += len(rows)
    return count


def load_endpoints(conn: sqlite3.Connection, path: Path) -> int:
    rows = []
    count = 0
    for row, fields in iter_csv(path):
        npi = get_first(row, fields, ["NPI", "NPI Number"])
        endpoint_url = get_first(row, fields, ["Endpoint", "Endpoint URL", "Fhir Endpoint"])
        if not npi or not endpoint_url:
            continue

        rows.append(
            (
                npi,
                endpoint_url,
                get_first(row, fields, ["Endpoint Type", "Endpoint Type Description"]),
                get_first(row, fields, ["Use Case", "Endpoint Use Case"]),
                get_first(row, fields, ["Affiliation", "Affiliated Organization"]),
                get_first(row, fields, ["Last Updated", "Last Seen", "Last Update Date"]),
            )
        )

        if len(rows) >= BATCH_SIZE:
            conn.executemany(
                """
                INSERT INTO nppes_endpoints (
                    npi, endpoint_url, endpoint_type, use_case, affiliation, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(npi, endpoint_url) DO UPDATE SET
                    endpoint_type=excluded.endpoint_type,
                    use_case=excluded.use_case,
                    affiliation=excluded.affiliation,
                    last_seen=excluded.last_seen
                """,
                rows,
            )
            count += len(rows)
            rows.clear()

    if rows:
        conn.executemany(
            """
            INSERT INTO nppes_endpoints (
                npi, endpoint_url, endpoint_type, use_case, affiliation, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(npi, endpoint_url) DO UPDATE SET
                endpoint_type=excluded.endpoint_type,
                use_case=excluded.use_case,
                affiliation=excluded.affiliation,
                last_seen=excluded.last_seen
            """,
            rows,
        )
        count += len(rows)
    return count


def sync_provider_dimension(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE dim_provider_npi
        SET
            entity_type = COALESCE((SELECT c.entity_type FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), entity_type),
            org_name = COALESCE((SELECT c.org_name FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), org_name),
            primary_taxonomy = COALESCE((SELECT c.primary_taxonomy FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), primary_taxonomy),
            nppes_last_seen = COALESCE((SELECT c.last_updated FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), nppes_last_seen),
            deactivation_date = COALESCE((SELECT d.deactivation_date FROM nppes_deactivations d WHERE d.npi = dim_provider_npi.npi), deactivation_date),
            deactivation_reason_code = COALESCE((SELECT d.reason_code FROM nppes_deactivations d WHERE d.npi = dim_provider_npi.npi), deactivation_reason_code),
            accessibility = COALESCE((SELECT c.accessibility FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), accessibility),
            secondary_languages = COALESCE((SELECT c.secondary_languages FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), secondary_languages),
            direct_email = COALESCE((SELECT c.direct_email FROM nppes_provider_core c WHERE c.npi = dim_provider_npi.npi), direct_email)
        """
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Load NPPES v2 files into HealthSpend prices.db")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to prices SQLite DB")
    parser.add_argument("--core", type=Path, help="Path to npidata v2 core CSV")
    parser.add_argument("--practice", type=Path, help="Path to practice location CSV")
    parser.add_argument("--other-names", dest="other_names", type=Path, help="Path to other names/DBA CSV")
    parser.add_argument("--deactivations", type=Path, help="Path to monthly deactivation CSV")
    parser.add_argument("--endpoints", type=Path, help="Path to endpoint reference CSV")
    args = parser.parse_args()

    if not any([args.core, args.practice, args.other_names, args.deactivations, args.endpoints]):
        print("No input files provided. Use --core and optional --practice/--other-names/--deactivations/--endpoints.")
        return 1

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_schema(conn)

    try:
        if args.core:
            count = load_core(conn, args.core)
            print(f"Loaded core NPPES rows: {count:,}")
        if args.practice:
            count = load_practice_locations(conn, args.practice)
            print(f"Loaded practice locations: {count:,}")
        if args.other_names:
            count = load_other_names(conn, args.other_names)
            print(f"Loaded other names/DBAs: {count:,}")
        if args.deactivations:
            count = load_deactivations(conn, args.deactivations)
            print(f"Loaded deactivations: {count:,}")
        if args.endpoints:
            count = load_endpoints(conn, args.endpoints)
            print(f"Loaded endpoint references: {count:,}")

        sync_provider_dimension(conn)
        conn.commit()
        print("Synced dim_provider_npi with NPPES V2 references.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
