use crate::shoppable::should_store_code;
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
    if lower == "payer_name" || lower == "payer" || lower == "insurance" { return Some("payer"); }
    if lower == "plan_name" || lower == "plan" || lower == "plan_type" { return Some("plan"); }
    None
}

pub fn parse_csv_tall(file_path: &str, ccn: &str) -> ParseResult {
    info!("Parsing CSV MRF: {}", file_path);

    // Step 1: Find where real data starts (skip garbage header rows)
    let header_row = find_header_row(file_path).unwrap_or(0);
    if header_row > 0 {
        warn!("Skipping {} preamble rows before data headers in {}", header_row, file_path);
    }

    let mut conn = match Connection::open("prices.db") {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to open prices.db: {}", e);
            return ParseResult { records_inserted: 0, mrf_machine_readable: false };
        }
    };

    // Also open compliance.db for error logging
    let compliance_conn = match Connection::open("compliance.db") {
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
    let mut idx_payer = None;
    let mut idx_plan = None;

    for (i, h) in headers.iter().enumerate() {
        match match_header(h) {
            Some("code") => idx_code = Some(i),
            Some("desc") => idx_desc = Some(i),
            Some("gross") => idx_gross = Some(i),
            Some("cash") => idx_cash = Some(i),
            Some("negotiated") => idx_negotiated = Some(i),
            Some("payer") => idx_payer = Some(i),
            Some("plan") => idx_plan = Some(i),
            _ => {}
        }
    }

    if idx_code.is_none() || idx_desc.is_none() {
        warn!("Missing essential headers (code/description) in {} -- marking as machine-unreadable.", file_path);
        return ParseResult { records_inserted: 0, mrf_machine_readable: false };
    }

    let mut tx = match conn.transaction() {
        Ok(t) => t,
        Err(e) => {
            warn!("DB transaction failed: {}", e);
            return ParseResult { records_inserted: 0, mrf_machine_readable: false };
        }
    };

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
        let payer  = idx_payer.and_then(|i| record.get(i)).unwrap_or("").to_string();
        let plan   = idx_plan.and_then(|i| record.get(i)).unwrap_or("").to_string();

        let _ = tx.execute(
            "INSERT INTO prices (ccn, cpt_code, description, gross_charge, cash_price, min_negotiated, max_negotiated, payer, plan, last_updated)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'))
                         ON CONFLICT(ccn, cpt_code, description, payer, plan)
                         DO UPDATE SET
                             gross_charge = excluded.gross_charge,
                             cash_price = excluded.cash_price,
                             min_negotiated = excluded.min_negotiated,
                             max_negotiated = excluded.max_negotiated,
                             last_updated = datetime('now')",
            rusqlite::params![ccn, code, desc, gross, cash, neg, neg, payer, plan],
        );

        ct += 1;
        if ct % 1000 == 0 {
            if let Err(e) = tx.commit() {
                warn!("Batch commit failed at row {}: {}", ct, e);
                return ParseResult { records_inserted: ct, mrf_machine_readable: true };
            }
            tx = match conn.transaction() {
                Ok(t) => t,
                Err(e) => {
                    warn!("Failed to restart transaction: {}", e);
                    return ParseResult { records_inserted: ct, mrf_machine_readable: true };
                }
            };
        }
    }

    let _ = tx.commit();
    info!("CSV parse complete: {} inserted, {} skipped (non-shoppable)", ct, skipped);
    ParseResult { records_inserted: ct, mrf_machine_readable: ct > 0 || skipped > 0 }
}
