use rusqlite::{Connection, Result};

pub fn init_db() -> Result<Connection> {
    let conn = Connection::open("compliance.db")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS hospitals (
            ccn TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            city TEXT,
            website TEXT,
            cms_hpt_url TEXT,
            mrf_url TEXT,
            last_audited TEXT
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS compliance (
            ccn TEXT PRIMARY KEY,
            score INTEGER,
            txt_exists BOOLEAN,
            robots_ok BOOLEAN,
            mrf_reachable BOOLEAN,
            mrf_valid BOOLEAN,
            mrf_fresh BOOLEAN,
            shoppable_exists BOOLEAN,
            mrf_machine_readable INTEGER NOT NULL DEFAULT 1,
            waf_blocked INTEGER NOT NULL DEFAULT 0,
            last_checked TEXT,
            evidence_json TEXT
        )",
        [],
    )?;

    // Safe migration: add columns to existing DBs that predate this schema version
    let _ = conn.execute("ALTER TABLE compliance ADD COLUMN mrf_machine_readable INTEGER NOT NULL DEFAULT 1", []);
    let _ = conn.execute("ALTER TABLE compliance ADD COLUMN waf_blocked INTEGER NOT NULL DEFAULT 0", []);

    // We'll put prices in a separate DB file but we can define the schema here
    let prices_conn = Connection::open("prices.db")?;
    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            ccn TEXT,
            cpt_code TEXT,
            description TEXT,
            gross_charge REAL,
            cash_price REAL,
            min_negotiated REAL,
            max_negotiated REAL,
            payer TEXT,
            plan TEXT,
            provider_npi TEXT,
            attribution_confidence REAL,
            last_updated TEXT
        )",
        [],
    )?;

    let _ = prices_conn.execute("ALTER TABLE prices ADD COLUMN provider_npi TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE prices ADD COLUMN attribution_confidence REAL", []);

    prices_conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_upsert_key
         ON prices (ccn, cpt_code, description, payer, plan)",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS dim_hospital (
            hospital_id INTEGER PRIMARY KEY,
            ccn TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            state TEXT,
            city TEXT,
            website TEXT,
            effective_date TEXT NOT NULL,
            retired_date TEXT
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS dim_payer (
            payer_id INTEGER PRIMARY KEY,
            payer_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS dim_plan (
            plan_id INTEGER PRIMARY KEY,
            payer_id INTEGER NOT NULL,
            plan_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            plan_type TEXT,
            UNIQUE(payer_id, normalized_name),
            FOREIGN KEY (payer_id) REFERENCES dim_payer(payer_id)
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS dim_provider_npi (
            provider_id INTEGER PRIMARY KEY,
            npi TEXT UNIQUE NOT NULL,
            entity_type TEXT,
            org_name TEXT,
            primary_taxonomy TEXT,
            nppes_last_seen TEXT,
            deactivation_date TEXT,
            deactivation_reason_code TEXT,
            accessibility TEXT,
            secondary_languages TEXT,
            direct_email TEXT
        )",
        [],
    )?;

    let _ = prices_conn.execute("ALTER TABLE dim_provider_npi ADD COLUMN deactivation_date TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE dim_provider_npi ADD COLUMN deactivation_reason_code TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE dim_provider_npi ADD COLUMN accessibility TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE dim_provider_npi ADD COLUMN secondary_languages TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE dim_provider_npi ADD COLUMN direct_email TEXT", []);

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS nppes_provider_core (
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
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS nppes_practice_locations (
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
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nppes_practice_locations_npi
         ON nppes_practice_locations (npi)",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS nppes_other_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT NOT NULL,
            other_name TEXT NOT NULL,
            type_code TEXT,
            FOREIGN KEY (npi) REFERENCES nppes_provider_core(npi)
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nppes_other_names_npi
         ON nppes_other_names (npi)",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS nppes_deactivations (
            npi TEXT PRIMARY KEY,
            deactivation_date TEXT,
            reactivation_date TEXT,
            reason_code TEXT,
            reason_text TEXT
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS nppes_endpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT NOT NULL,
            endpoint_url TEXT NOT NULL,
            endpoint_type TEXT,
            use_case TEXT,
            affiliation TEXT,
            last_seen TEXT,
            UNIQUE(npi, endpoint_url)
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nppes_endpoints_npi
         ON nppes_endpoints (npi)",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS cms_attestations (
            ccn TEXT PRIMARY KEY,
            attester_name TEXT,
            attester_npi TEXT,
            attestation_date TEXT,
            source_file TEXT,
            last_seen TEXT
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS npi_audit_findings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            ccn TEXT NOT NULL,
            npi TEXT NOT NULL,
            finding_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            reason_code TEXT,
            notes TEXT,
            UNIQUE(snapshot_date, ccn, npi, finding_type)
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_npi_audit_findings_lookup
         ON npi_audit_findings (snapshot_date, ccn, npi)",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS dim_procedure (
            procedure_id INTEGER PRIMARY KEY,
            code_type TEXT NOT NULL,
            code TEXT NOT NULL,
            description TEXT,
            UNIQUE(code_type, code)
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS fact_negotiated_rate (
            fact_id INTEGER PRIMARY KEY,
            snapshot_date TEXT NOT NULL,
            hospital_id INTEGER NOT NULL,
            procedure_id INTEGER NOT NULL,
            payer_id INTEGER,
            plan_id INTEGER,
            provider_id INTEGER,
            negotiated_rate REAL,
            allowed_median REAL,
            allowed_p10 REAL,
            allowed_p90 REAL,
            currency TEXT DEFAULT 'USD',
            source_file TEXT,
            source_row_hash TEXT UNIQUE,
            attribution_confidence REAL NOT NULL,
            FOREIGN KEY (hospital_id) REFERENCES dim_hospital(hospital_id),
            FOREIGN KEY (procedure_id) REFERENCES dim_procedure(procedure_id),
            FOREIGN KEY (payer_id) REFERENCES dim_payer(payer_id),
            FOREIGN KEY (plan_id) REFERENCES dim_plan(plan_id),
            FOREIGN KEY (provider_id) REFERENCES dim_provider_npi(provider_id)
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fact_lookup
         ON fact_negotiated_rate (snapshot_date, hospital_id, procedure_id, payer_id, plan_id)",
        [],
    )?;

    prices_conn.execute(
        "CREATE TABLE IF NOT EXISTS hot_price_compare (
            snapshot_date TEXT NOT NULL,
            ccn TEXT NOT NULL,
            hospital_name TEXT,
            code_type TEXT NOT NULL,
            code TEXT NOT NULL,
            payer_name TEXT,
            plan_name TEXT,
            cash_price REAL,
            negotiated_rate REAL,
            allowed_median REAL,
            allowed_p10 REAL,
            allowed_p90 REAL,
            attribution_confidence REAL,
            zombie_status TEXT,
            zombie_reason_code TEXT,
            accessibility TEXT,
            license_proxy_suspected INTEGER DEFAULT 0,
            delta_abs REAL,
            delta_pct REAL,
            PRIMARY KEY (snapshot_date, ccn, code_type, code, payer_name, plan_name)
        )",
        [],
    )?;

    let _ = prices_conn.execute("ALTER TABLE hot_price_compare ADD COLUMN hospital_name TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE hot_price_compare ADD COLUMN zombie_status TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE hot_price_compare ADD COLUMN zombie_reason_code TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE hot_price_compare ADD COLUMN accessibility TEXT", []);
    let _ = prices_conn.execute("ALTER TABLE hot_price_compare ADD COLUMN license_proxy_suspected INTEGER DEFAULT 0", []);

    // Parse errors audit table for tracking data quality issues
    conn.execute(
        "CREATE TABLE IF NOT EXISTS parse_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ccn TEXT NOT NULL,
            file_path TEXT NOT NULL,
            error_type TEXT NOT NULL,
            error_detail TEXT,
            file_size_bytes INTEGER,
            timestamp TEXT NOT NULL,
            resolved INTEGER DEFAULT 0,
            resolution_note TEXT
        )",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_parse_errors_ccn ON parse_errors(ccn)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_parse_errors_timestamp ON parse_errors(timestamp)",
        [],
    )?;

    // Metrics table for data quality tracking
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            state TEXT,
            metric_name TEXT NOT NULL,
            value REAL NOT NULL,
            unit TEXT,
            UNIQUE(date, state, metric_name)
        )",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_metrics_date ON metrics(date)",
        [],
    )?;

    // Pipeline runs table for tracking
    conn.execute(
        "CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL,
            timestamp TEXT NOT NULL,
            status TEXT DEFAULT 'success',
            notes TEXT
        )",
        [],
    )?;

    Ok(conn)
}
