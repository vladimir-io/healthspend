use crate::shoppable::should_store_code;
use crate::fact_writer::{FactWriter, PriceFactInput};
use crate::error_logger::{log_parse_error_to_database, ParseError};
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use rusqlite::Connection;
use std::fs::File;
use std::fmt;
use std::io::BufReader;
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

#[derive(Deserialize)]
struct CodeInfo {
    #[serde(default)]
    code: String,
}

#[derive(Deserialize)]
struct StandardCharge {
    #[serde(rename = "type", default)]
    charge_type: String,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    standard_charge_dollar: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64", alias = "allowed_amount_median")]
    allowed_median: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64", alias = "allowed_amount_10th")]
    allowed_p10: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64", alias = "allowed_amount_90th")]
    allowed_p90: Option<f64>,
    #[serde(default)]
    payer_name: String,
    #[serde(default)]
    plan_name: String,
}

#[derive(Deserialize)]
struct ProviderReference {
    #[serde(default)]
    npi: String,
}

#[derive(Deserialize)]
struct ChargeItem {
    #[serde(default)]
    description: String,
    #[serde(default)]
    code_information: Vec<CodeInfo>,
    #[serde(default)]
    standard_charges: Vec<StandardCharge>,
    #[serde(default)]
    provider_references: Vec<ProviderReference>,
    #[serde(default)]
    provider_npi: String,
}

struct ParseContext<'a> {
    conn: &'a mut Connection,
    writer: FactWriter,
    ccn: String,
    records_inserted: usize,
    skipped: usize,
}

impl<'a> ParseContext<'a> {
    fn new(conn: &'a mut Connection, ccn: &'a str) -> Self {
        let snapshot_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
        Self {
            conn,
            writer: FactWriter::new(snapshot_date),
            ccn: ccn.to_string(),
            records_inserted: 0,
            skipped: 0,
        }
    }

    fn process_charge_item(&mut self, item: ChargeItem) {
        let code = item
            .code_information
            .iter()
            .find_map(|ci| {
                let trimmed = ci.code.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .unwrap_or_default();

        if !should_store_code(&code) {
            self.skipped += 1;
            return;
        }

        if item.description.trim().is_empty() {
            return;
        }

        let mut gross = 0.0;
        let mut cash = 0.0;
        let mut negotiated = 0.0;
        let mut allowed_median = None;
        let mut allowed_p10 = None;
        let mut allowed_p90 = None;
        let mut payer = String::new();
        let mut plan = String::new();

        let provider_npi = item
            .provider_references
            .iter()
            .find_map(|p| {
                let trimmed = p.npi.trim();
                if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
            })
            .or_else(|| {
                let trimmed = item.provider_npi.trim();
                if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
            });

        for charge in item.standard_charges {
            let amount = charge.standard_charge_dollar.unwrap_or(0.0);
            if allowed_median.is_none() {
                allowed_median = charge.allowed_median;
            }
            if allowed_p10.is_none() {
                allowed_p10 = charge.allowed_p10;
            }
            if allowed_p90.is_none() {
                allowed_p90 = charge.allowed_p90;
            }
            match charge.charge_type.as_str() {
                "gross" => gross = amount,
                "discounted_cash" => cash = amount,
                "negotiated" => {
                    negotiated = amount;
                    payer = charge.payer_name;
                    plan = charge.plan_name;
                }
                _ => {}
            }
        }

        self.writer.write_dual(self.conn, &PriceFactInput {
            ccn: &self.ccn,
            code_type: "CPT",
            code: &code,
            description: &item.description,
            gross_charge: gross,
            cash_price: cash,
            negotiated_rate: negotiated,
            payer: &payer,
            plan: &plan,
            provider_npi: provider_npi.as_deref(),
            allowed_median,
            allowed_p10,
            allowed_p90,
        });

        self.records_inserted += 1;
    }
}

struct RootSeed<'a, 'ctx> {
    ctx: &'ctx mut ParseContext<'a>,
}

impl<'de, 'a, 'ctx> DeserializeSeed<'de> for RootSeed<'a, 'ctx> {
    type Value = bool;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RootVisitor { ctx: self.ctx })
    }
}

struct RootVisitor<'a, 'ctx> {
    ctx: &'ctx mut ParseContext<'a>,
}

impl<'de, 'a, 'ctx> Visitor<'de> for RootVisitor<'a, 'ctx> {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object containing a known CMS charge array")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut found_key = false;

        while let Some(key) = map.next_key::<String>()? {
            if FALLBACK_ROOT_KEYS.contains(&key.as_str()) {
                found_key = true;
                map.next_value_seed(ChargeArraySeed { ctx: self.ctx })?;
            } else {
                let _: IgnoredAny = map.next_value()?;
            }
        }

        Ok(found_key)
    }
}

struct ChargeArraySeed<'a, 'ctx> {
    ctx: &'ctx mut ParseContext<'a>,
}

impl<'de, 'a, 'ctx> DeserializeSeed<'de> for ChargeArraySeed<'a, 'ctx> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ChargeArrayVisitor { ctx: self.ctx })
    }
}

struct ChargeArrayVisitor<'a, 'ctx> {
    ctx: &'ctx mut ParseContext<'a>,
}

impl<'de, 'a, 'ctx> Visitor<'de> for ChargeArrayVisitor<'a, 'ctx> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an array of CMS charge items")
    }

    fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
    where
        S: SeqAccess<'de>,
    {
        while let Some(raw_item) = seq.next_element::<serde_json::Value>()? {
            match serde_json::from_value::<ChargeItem>(raw_item) {
                Ok(item) => self.ctx.process_charge_item(item),
                Err(_) => {
                    continue;
                }
            }
        }

        Ok(())
    }
}

fn deserialize_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumericValue {
        Number(f64),
        String(String),
        Null,
    }

    match NumericValue::deserialize(deserializer)? {
        NumericValue::Number(n) => Ok(Some(n)),
        NumericValue::String(s) => {
            let trimmed = s.trim().trim_start_matches('$').replace(',', "");
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<f64>()
                    .map(Some)
                    .map_err(de::Error::custom)
            }
        }
        NumericValue::Null => Ok(None),
    }
}

/// True streaming parser using serde_json reader-based deserialization.
/// This avoids loading entire MRFs into memory and removes the old 2GB ceiling.
pub fn parse_json_streaming(file_path: &str, ccn: &str) -> ParseResult {
    parse_json_streaming_with_dbs(file_path, ccn, "prices.db", "compliance.db")
}

pub fn parse_json_streaming_with_dbs(
    file_path: &str,
    ccn: &str,
    prices_db_path: &str,
    compliance_db_path: &str,
) -> ParseResult {
    info!("Streaming JSON MRF via serde_json reader: {}", file_path);

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

    let file_size = std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);
    let reader = BufReader::new(file);
    let _ = conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;");

    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let mut ctx = ParseContext::new(&mut conn, ccn);

    let parse_result = RootSeed { ctx: &mut ctx }.deserialize(&mut deserializer);

    let _ = ctx.conn.execute_batch("COMMIT;");

    match parse_result {
        Ok(found_key) => {
            if !found_key {
                warn!("No known charge array root key found in JSON -- tried: {:?}", FALLBACK_ROOT_KEYS);
                return ParseResult { records_inserted: 0, mrf_machine_readable: false };
            }

            info!(
                "JSON parse complete: {} inserted, {} skipped (non-shoppable)",
                ctx.records_inserted,
                ctx.skipped
            );
            ParseResult {
                records_inserted: ctx.records_inserted,
                mrf_machine_readable: ctx.records_inserted > 0 || ctx.skipped > 0,
            }
        }
        Err(e) => {
            warn!("JSON parse error in {}: {} -- marking machine-unreadable", file_path, e);
            let error = ParseError::new(
                ccn.to_string(),
                file_path.to_string(),
                "JSONParseError".to_string(),
                e.to_string(),
                file_size,
            );
            let _ = log_parse_error_to_database(error, &compliance_conn);
            ParseResult { records_inserted: 0, mrf_machine_readable: false }
        }
    }
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
    fn parses_contract_v3_json_fixture() {
        let root = env!("CARGO_MANIFEST_DIR");
        let json_path = format!("{}/data/contract_v3.json", root);
        let prices_db = format!("{}/target/test-contract-v3-json-prices.db", root);
        let compliance_db = format!("{}/target/test-contract-v3-json-compliance.db", root);
        let _ = std::fs::remove_file(&prices_db);
        let _ = std::fs::remove_file(&compliance_db);
        setup_test_dbs(&prices_db, &compliance_db);

        let result = parse_json_streaming_with_dbs(&json_path, "450358", &prices_db, &compliance_db);
        assert!(result.mrf_machine_readable);
        assert!(result.records_inserted >= 1);
    }

    #[test]
    fn marks_malformed_json_without_known_root_unreadable() {
        let root = env!("CARGO_MANIFEST_DIR");
        let json_path = format!("{}/data/contract_v3_malformed.json", root);
        let prices_db = format!("{}/target/test-malformed-json-prices.db", root);
        let compliance_db = format!("{}/target/test-malformed-json-compliance.db", root);
        let _ = std::fs::remove_file(&prices_db);
        let _ = std::fs::remove_file(&compliance_db);
        setup_test_dbs(&prices_db, &compliance_db);

        let result = parse_json_streaming_with_dbs(&json_path, "450358", &prices_db, &compliance_db);
        assert!(!result.mrf_machine_readable);
        assert_eq!(result.records_inserted, 0);
    }
}
