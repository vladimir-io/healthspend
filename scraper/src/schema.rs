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
            last_updated TEXT
        )",
        [],
    )?;

    prices_conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_upsert_key
         ON prices (ccn, cpt_code, description, payer, plan)",
        [],
    )?;

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
