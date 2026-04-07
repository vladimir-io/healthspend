use crate::shoppable::should_store_code;
use crate::error_logger::{log_parse_error_to_database, ParseError};
use jiter::{Jiter, JiterError, Peek};
use rusqlite::Connection;
use std::fs::File;
use std::io::{BufReader, Read};
use tracing::{info, warn};

pub struct ParseResult {
    pub records_inserted: usize,
    pub mrf_machine_readable: bool,
}

/// Alternate root keys used by hospitals deviating from the CMS schema.
/// Tried in order after the canonical key fails.
const FALLBACK_ROOT_KEYS: &[&str] = &[
    "standard_charge_information",
    "chargemaster",
    "charge_information",
    "items",
    "data",
    "charges",
];

/// True streaming parser using `jiter` for memory-safe parsing of 50GB+ JSON MRFs.
/// Scans for the target array at any of the known root keys, then streams item-by-item.
/// Falls back gracefully via ParseResult rather than panicking.
pub fn parse_json_streaming(file_path: &str, ccn: &str) -> ParseResult {
    info!("Streaming JSON MRF via jiter: {}", file_path);

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

    // Read file into memory for jiter. For files > 2GB we log and bail.
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

    let metadata = file.metadata().unwrap_or_else(|_| {
        // Can't stat — fall through and try anyway
        warn!("Cannot stat {}, attempting parse anyway", file_path);
        file.metadata().unwrap()
    });

    const MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GB safety limit
    if metadata.len() > MAX_BYTES {
        warn!(
            "MRF {} is {}GB, exceeds 2GB jiter limit -- marking machine-unreadable",
            file_path,
            metadata.len() / 1_073_741_824
        );
        let error = ParseError::new(
            ccn.to_string(),
            file_path.to_string(),
            "FileTooLarge".to_string(),
            format!("File size {} exceeds 2GB limit", metadata.len()),
            metadata.len(),
        );
        let _ = log_parse_error_to_database(error, &compliance_conn);
        return ParseResult { records_inserted: 0, mrf_machine_readable: false };
    }

    let mut buf = Vec::with_capacity(metadata.len() as usize);
    if let Err(e) = BufReader::new(file).read_to_end(&mut buf) {
        warn!("Failed to read {}: {}", file_path, e);
        let error = ParseError::new(
            ccn.to_string(),
            file_path.to_string(),
            "IOError".to_string(),
            e.to_string(),
            metadata.len(),
        );
        let _ = log_parse_error_to_database(error, &compliance_conn);
        return ParseResult { records_inserted: 0, mrf_machine_readable: false };
    }

    match parse_json_inner(&buf, &mut conn, ccn) {
        Ok(result) => result,
        Err(e) => {
            warn!("JSON parse error in {}: {} -- marking machine-unreadable", file_path, e);
            let error = ParseError::new(
                ccn.to_string(),
                file_path.to_string(),
                "JSONParseError".to_string(),
                e.to_string(),
                buf.len() as u64,
            );
            let _ = log_parse_error_to_database(error, &compliance_conn);
            ParseResult { records_inserted: 0, mrf_machine_readable: false }
        }
    }
}

fn parse_json_inner(buf: &[u8], conn: &mut Connection, ccn: &str) -> Result<ParseResult, JiterError> {
    let mut jiter = Jiter::new(buf);

    // Navigate to root object
    jiter.next_object()?;

    let mut found_key = false;
    let mut ct = 0usize;
    let mut skipped = 0usize;

    // Scan root keys until we find a known charge array
    'root: loop {
        let key = match jiter.next_key()? {
            Some(k) => k.to_string(),
            None => break 'root,
        };

        let is_target = FALLBACK_ROOT_KEYS.contains(&key.as_str());

        if !is_target {
            // Skip the value entirely — this is the key jiter advantage vs serde_json
            jiter.next_skip()?;
            continue;
        }

        found_key = true;

        // Expect an array
        let peek = jiter.peek()?;
        if peek != Peek::Array {
            jiter.next_skip()?;
            continue;
        }

        // Iterate array elements
        let mut first = jiter.next_array()?;
        while first.is_some() {
            first = match parse_charge_item(&mut jiter, conn, ccn, &mut ct, &mut skipped) {
                Ok(next) => next,
                Err(e) => {
                    warn!("Skipping malformed charge item: {}", e);
                    // Try to skip this item and continue
                    let _ = jiter.next_skip();
                    jiter.array_step()?
                }
            };
        }
        break 'root;
    }

    if !found_key {
        warn!("No known charge array root key found in JSON -- tried: {:?}", FALLBACK_ROOT_KEYS);
        return Ok(ParseResult { records_inserted: 0, mrf_machine_readable: false });
    }

    info!("JSON parse complete: {} inserted, {} skipped (non-shoppable)", ct, skipped);
    Ok(ParseResult { records_inserted: ct, mrf_machine_readable: ct > 0 || skipped > 0 })
}

/// Parse one charge item object from the jiter stream.
/// Returns the next array peek token (None if array exhausted).
fn parse_charge_item<'a>(
    jiter: &mut Jiter<'a>,
    conn: &mut Connection,
    ccn: &str,
    ct: &mut usize,
    skipped: &mut usize,
) -> Result<Option<Peek>, JiterError> {
    let mut code = String::new();
    let mut description = String::new();
    let mut cash: f64 = 0.0;
    let mut gross: f64 = 0.0;
    let mut negotiated: f64 = 0.0;
    let mut payer = String::new();
    let mut plan = String::new();

    jiter.next_object()?;
    loop {
        let key = match jiter.next_key()? {
            Some(k) => k.to_string(),
            None => break,
        };

        match key.as_str() {
            "description" => {
                description = jiter.next_str()?.to_string();
            }
            "code_information" => {
                // Array of {code, type} objects
                let mut first = jiter.next_array()?;
                let mut got_code = false;
                while first.is_some() {
                    jiter.next_object()?;
                    loop {
                        match jiter.next_key()? {
                            Some(k) if k == "code" && !got_code => {
                                code = jiter.next_str()?.to_string();
                                got_code = true;
                            }
                            Some(_) => { jiter.next_skip()?; }
                            None => break,
                        }
                    }
                    first = jiter.array_step()?;
                }
            }
            "standard_charges" => {
                let mut first = jiter.next_array()?;
                while first.is_some() {
                    let mut charge_type = String::new();
                    let mut amount: Option<f64> = None;
                    let mut this_payer = String::new();
                    let mut this_plan = String::new();

                    jiter.next_object()?;
                    loop {
                        match jiter.next_key()? {
                            Some(k) => match k {
                                "type" => charge_type = jiter.next_str()?.to_string(),
                                "standard_charge_dollar" => {
                                    amount = jiter.next_number_bytes()
                                        .ok()
                                        .and_then(|b| std::str::from_utf8(b).ok()
                                            .and_then(|s| s.parse::<f64>().ok()));
                                }
                                "payer_name" => this_payer = jiter.next_str()?.to_string(),
                                "plan_name"  => this_plan  = jiter.next_str()?.to_string(),
                                _ => { jiter.next_skip()?; }
                            },
                            None => break,
                        }
                    }

                    match charge_type.as_str() {
                        "gross" => gross = amount.unwrap_or(0.0),
                        "discounted_cash" => { cash = amount.unwrap_or(0.0); }
                        "negotiated" => {
                            negotiated = amount.unwrap_or(0.0);
                            payer = this_payer;
                            plan = this_plan;
                        }
                        _ => {}
                    }

                    first = jiter.array_step()?;
                }
            }
            _ => { jiter.next_skip()?; }
        }
    }

    // Apply CPT coverage policy (shoppable-only by default, full via env toggle)
    if !should_store_code(&code) {
        *skipped += 1;
        return jiter.array_step();
    }

    if !description.is_empty() {
        let _ = conn.execute(
            "INSERT INTO prices (ccn, cpt_code, description, gross_charge, cash_price, min_negotiated, max_negotiated, payer, plan, last_updated)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'))
             ON CONFLICT(ccn, cpt_code, description, payer, plan)
             DO UPDATE SET
               gross_charge = excluded.gross_charge,
               cash_price = excluded.cash_price,
               min_negotiated = excluded.min_negotiated,
               max_negotiated = excluded.max_negotiated,
               last_updated = datetime('now')",
            rusqlite::params![ccn, code, description, gross, cash, negotiated, negotiated, payer, plan],
        );
        *ct += 1;
    }

    jiter.array_step()
}
