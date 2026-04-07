use crate::shoppable::should_store_code;
use crate::fact_writer::{FactWriter, PriceFactInput};
use crate::error_logger::{log_parse_error_to_database, ParseError};
use rusqlite::Connection;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::{info, warn};

pub struct ParseResult {
    pub records_inserted: usize,
    pub mrf_machine_readable: bool,
}

/// Sanitize a raw price string from a hospital CSV into a clean f64.
/// Handles: "$1,200.00", "N/A", "See Contract", "-", "", blanks.
fn parse_charge(raw: &str) -> f64 {
    let cleaned = raw
        .trim()
        .trim_start_matches('$')
        .replace(',', "");

    match cleaned.to_ascii_lowercase().as_str() {
        "" | "n/a" | "na" | "see contract" | "-" | "not available" | "none" | "varies" => 0.0,
        s => s.parse::<f64>().unwrap_or(0.0),
    }
}

/// Find the row index where actual CMS data begins.
/// Hospitals sometimes prefix files with 15 rows of metadata before real headers.
/// We scan for the canonical CMS header marker `code|1` or `description`.
fn find_header_row(path: &str) -> Option<usize> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for (i, line) in reader.lines().enumerate() {
        if i > 30 { break; } // Don't scan forever
        if let Ok(l) = line {
            let lower = l.to_ascii_lowercase();
            if lower.contains("code|1") || lower.contains("description") {
                return Some(i);
            }
        }
    }
    None
}

/// Fuzzy header matching — handles hospitals remapping standard CMS column names.
fn match_header(h: &str) -> Option<&'static str> {
    let lower = h.to_ascii_lowercase();
    if lower.contains("code|1") || lower == "cpt_code" || lower == "cpt" { return Some("code"); }
    if lower == "description" || lower == "item_description" || lower == "service_description" { return Some("desc"); }
    if lower.contains("gross") { return Some("gross"); }
    if lower.contains("discounted_cash") || lower == "cash_price" || lower == "cash" { return Some("cash"); }
    if lower.contains("negotiated_dollar") || lower.contains("negotiated") { return Some("negotiated"); }
    if lower.contains("allowed_amount_median") || lower.contains("allowed_median") { return Some("allowed_median"); }
    if lower.contains("allowed_amount_10th") || lower.contains("allowed_p10") { return Some("allowed_p10"); }
    if lower.contains("allowed_amount_90th") || lower.contains("allowed_p90") { return Some("allowed_p90"); }
    if lower == "payer_name" || lower == "payer" || lower == "insurance" { return Some("payer"); }
    if lower == "plan_name" || lower == "plan" || lower == "plan_type" { return Some("plan"); }
    if lower == "provider_npi" || lower == "npi" || lower.contains("npi") { return Some("provider_npi"); }
    None
}

pub fn parse_csv_tall(file_path: &str, ccn: &str) -> ParseResult {
    parse_csv_tall_with_dbs(file_path, ccn, "prices.db", "compliance.db")
}

pub fn parse_csv_tall_with_dbs(
    file_path: &str,
    ccn: &str,
    prices_db_path: &str,
    compliance_db_path: &str,
) -> ParseResult {
    info!("Parsing CSV MRF: {}", file_path);

    // Step 1: Find where real data starts (skip garbage header rows)
    let header_row = find_header_row(file_path).unwrap_or(0);
    if header_row > 0 {
        warn!("Skipping {} preamble rows before data headers in {}", header_row, file_path);
    }

    let mut conn = match Connection::open(prices_db_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to open prices.db: {}", e);
            return ParseResult { records_inserted: 0, mrf_machine_readable: false };
        }
    };

    // Also open compliance.db for error logging
    let compliance_conn = match Connection::open(compliance_db_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to open compliance.db for error logging: {}", e);
            return ParseResult { records_inserted: 0, mrf_machine_readable: false };
        }
    };

    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(e) => {
            warn!("Cannot open MRF file {}: {}", file_path, e);
            let error = ParseError::new(
                ccn.to_string(),
                file_path.to_string(),
                "FileNotFound".to_string(),
                e.to_string(),
                0,
            );
            let _ = log_parse_error_to_database(error, &compliance_conn);
            return ParseResult { records_inserted: 0, mrf_machine_readable: false };
        }
    };

    // Get file size for logging
    let file_size = std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);

    // Skip preamble rows
    let mut reader = BufReader::new(file);
    for _ in 0..header_row {
        let mut discard = String::new();
        let _ = reader.read_line(&mut discard);
    }

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(reader);

    let headers = match rdr.headers() {
        Ok(h) => h.clone(),
        Err(e) => {
            warn!("Cannot read CSV headers in {}: {}", file_path, e);
            let error = ParseError::new(
                ccn.to_string(),
                file_path.to_string(),
                "CSVHeaderError".to_string(),
                e.to_string(),
                file_size,
            );
            let _ = log_parse_error_to_database(error, &compliance_conn);
            return ParseResult { records_inserted: 0, mrf_machine_readable: false };
        }
    };

    // Step 2: Fuzzy match columns
    let mut idx_code = None;
    let mut idx_desc = None;
    let mut idx_gross = None;
    let mut idx_cash = None;
    let mut idx_negotiated = None;
    let mut idx_allowed_median = None;
    let mut idx_allowed_p10 = None;
    let mut idx_allowed_p90 = None;
    let mut idx_payer = None;
    let mut idx_plan = None;
    let mut idx_provider_npi = None;

    for (i, h) in headers.iter().enumerate() {
        match match_header(h) {
            Some("code") => idx_code = Some(i),
            Some("desc") => idx_desc = Some(i),
            Some("gross") => idx_gross = Some(i),
            Some("cash") => idx_cash = Some(i),
            Some("negotiated") => idx_negotiated = Some(i),
            Some("allowed_median") => idx_allowed_median = Some(i),
            Some("allowed_p10") => idx_allowed_p10 = Some(i),
            Some("allowed_p90") => idx_allowed_p90 = Some(i),
            Some("payer") => idx_payer = Some(i),
            Some("plan") => idx_plan = Some(i),
            Some("provider_npi") => idx_provider_npi = Some(i),
            _ => {}
        }
    }

    if idx_code.is_none() || idx_desc.is_none() {
        warn!("Missing essential headers (code/description) in {} -- marking as machine-unreadable.", file_path);
        return ParseResult { records_inserted: 0, mrf_machine_readable: false };
    }

    if conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;").is_err() {
        warn!("DB transaction failed");
        return ParseResult { records_inserted: 0, mrf_machine_readable: false };
    }

    let snapshot_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let writer = FactWriter::new(snapshot_date);

    let mut ct = 0;
    let mut skipped = 0;

    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(_) => continue, // Skip malformed rows
        };

        let code = idx_code.and_then(|i| record.get(i)).unwrap_or("").trim().to_string();

        // Step 3: CPT coverage policy (shoppable-only by default, full via env toggle)
        if !should_store_code(&code) {
            skipped += 1;
            continue;
        }

        let desc = idx_desc.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let gross  = idx_gross.and_then(|i| record.get(i)).map(parse_charge).unwrap_or(0.0);
        let cash   = idx_cash.and_then(|i| record.get(i)).map(parse_charge).unwrap_or(0.0);
        let neg    = idx_negotiated.and_then(|i| record.get(i)).map(parse_charge).unwrap_or(0.0);
        let allowed_median = idx_allowed_median.and_then(|i| record.get(i)).map(parse_charge);
        let allowed_p10 = idx_allowed_p10.and_then(|i| record.get(i)).map(parse_charge);
        let allowed_p90 = idx_allowed_p90.and_then(|i| record.get(i)).map(parse_charge);
        let payer  = idx_payer.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let plan   = idx_plan.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let provider_npi = idx_provider_npi.and_then(|i| record.get(i)).map(|s| s.trim().to_string());

        writer.write_dual(&mut conn, &PriceFactInput {
            ccn,
            code_type: "CPT",
            code: &code,
            description: &desc,
            gross_charge: gross,
            cash_price: cash,
            negotiated_rate: neg,
            payer: &payer,
            plan: &plan,
            provider_npi: provider_npi.as_deref(),
            allowed_median,
            allowed_p10,
            allowed_p90,
        });

        ct += 1;
        if ct % 1000 == 0 {
            if let Err(e) = conn.execute_batch("COMMIT; BEGIN IMMEDIATE TRANSACTION;") {
                warn!("Batch commit failed at row {}: {}", ct, e);
                return ParseResult { records_inserted: ct, mrf_machine_readable: true };
            }
        }
    }

    let _ = conn.execute_batch("COMMIT;");
    info!("CSV parse complete: {} inserted, {} skipped (non-shoppable)", ct, skipped);
    ParseResult { records_inserted: ct, mrf_machine_readable: ct > 0 || skipped > 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_test_dbs(prices_db: &str, compliance_db: &str) {
        let prices = Connection::open(prices_db).unwrap();
        prices.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS prices (
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
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_upsert_key
                ON prices (ccn, cpt_code, description, payer, plan);
            CREATE TABLE IF NOT EXISTS dim_hospital (hospital_id INTEGER PRIMARY KEY, ccn TEXT UNIQUE NOT NULL, name TEXT NOT NULL, effective_date TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS dim_payer (payer_id INTEGER PRIMARY KEY, payer_name TEXT NOT NULL, normalized_name TEXT NOT NULL UNIQUE);
            CREATE TABLE IF NOT EXISTS dim_plan (plan_id INTEGER PRIMARY KEY, payer_id INTEGER NOT NULL, plan_name TEXT NOT NULL, normalized_name TEXT NOT NULL, UNIQUE(payer_id, normalized_name));
            CREATE TABLE IF NOT EXISTS dim_provider_npi (provider_id INTEGER PRIMARY KEY, npi TEXT UNIQUE NOT NULL, entity_type TEXT, org_name TEXT, primary_taxonomy TEXT, nppes_last_seen TEXT);
            CREATE TABLE IF NOT EXISTS dim_procedure (procedure_id INTEGER PRIMARY KEY, code_type TEXT NOT NULL, code TEXT NOT NULL, description TEXT, UNIQUE(code_type, code));
            CREATE TABLE IF NOT EXISTS fact_negotiated_rate (fact_id INTEGER PRIMARY KEY, snapshot_date TEXT NOT NULL, hospital_id INTEGER NOT NULL, procedure_id INTEGER NOT NULL, payer_id INTEGER, plan_id INTEGER, provider_id INTEGER, negotiated_rate REAL, allowed_median REAL, allowed_p10 REAL, allowed_p90 REAL, currency TEXT DEFAULT 'USD', source_file TEXT, source_row_hash TEXT UNIQUE, attribution_confidence REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS hot_price_compare (snapshot_date TEXT NOT NULL, ccn TEXT NOT NULL, code_type TEXT NOT NULL, code TEXT NOT NULL, payer_name TEXT, plan_name TEXT, cash_price REAL, negotiated_rate REAL, allowed_median REAL, allowed_p10 REAL, allowed_p90 REAL, attribution_confidence REAL, delta_abs REAL, delta_pct REAL, PRIMARY KEY (snapshot_date, ccn, code_type, code, payer_name, plan_name));
            ",
        ).unwrap();

        let compliance = Connection::open(compliance_db).unwrap();
        compliance.execute_batch(
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
            );",
        ).unwrap();
    }

    #[test]
    fn parses_contract_v3_csv_fixture() {
        let root = env!("CARGO_MANIFEST_DIR");
        let csv_path = format!("{}/data/contract_v3.csv", root);
        let prices_db = format!("{}/target/test-contract-v3-csv-prices.db", root);
        let compliance_db = format!("{}/target/test-contract-v3-csv-compliance.db", root);
        let _ = std::fs::remove_file(&prices_db);
        let _ = std::fs::remove_file(&compliance_db);
        setup_test_dbs(&prices_db, &compliance_db);

        let result = parse_csv_tall_with_dbs(&csv_path, "450056", &prices_db, &compliance_db);
        assert!(result.mrf_machine_readable);
        assert!(result.records_inserted >= 1);
    }

    #[test]
    fn rejects_malformed_csv_missing_required_headers() {
        let root = env!("CARGO_MANIFEST_DIR");
        let csv_path = format!("{}/data/contract_v3_malformed.csv", root);
        let prices_db = format!("{}/target/test-malformed-csv-prices.db", root);
        let compliance_db = format!("{}/target/test-malformed-csv-compliance.db", root);
        let _ = std::fs::remove_file(&prices_db);
        let _ = std::fs::remove_file(&compliance_db);
        setup_test_dbs(&prices_db, &compliance_db);

        let result = parse_csv_tall_with_dbs(&csv_path, "450056", &prices_db, &compliance_db);
        assert!(!result.mrf_machine_readable);
        assert_eq!(result.records_inserted, 0);
    }
}
