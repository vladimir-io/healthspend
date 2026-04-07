use crate::npi::attribution::{score, AttributionSignals};
use rusqlite::{params, Connection};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

struct ProviderEvidence {
    org_name: Option<String>,
    entity_type: Option<String>,
    primary_taxonomy: Option<String>,
    practice_state: Option<String>,
    deactivation_reason_code: Option<String>,
    deactivation_date: Option<String>,
    accessibility: Option<String>,
    secondary_languages: Option<String>,
    direct_email: Option<String>,
}

pub struct PriceFactInput<'a> {
    pub ccn: &'a str,
    pub code_type: &'a str,
    pub code: &'a str,
    pub description: &'a str,
    pub gross_charge: f64,
    pub cash_price: f64,
    pub negotiated_rate: f64,
    pub payer: &'a str,
    pub plan: &'a str,
    pub provider_npi: Option<&'a str>,
    pub allowed_median: Option<f64>,
    pub allowed_p10: Option<f64>,
    pub allowed_p90: Option<f64>,
}

pub struct FactWriter {
    snapshot_date: String,
}

impl FactWriter {
    pub fn new(snapshot_date: String) -> Self {
        Self { snapshot_date }
    }

    pub fn write_dual(&self, conn: &mut Connection, input: &PriceFactInput<'_>) {
        let provider_id = self.upsert_provider(conn, input.provider_npi);
        let confidence = self.compute_confidence(conn, input.ccn, input.provider_npi, input.payer, input.plan);
        let hospital_name = self.query_hospital_name(conn, input.ccn).unwrap_or_default();
        let evidence = input
            .provider_npi
            .and_then(|n| self.query_provider_evidence(conn, n.trim()));
        let (zombie_status, zombie_reason_code) = match evidence
            .as_ref()
            .and_then(|e| e.deactivation_reason_code.clone())
        {
            Some(code) => ("deactivated".to_string(), code),
            None => ("active".to_string(), "".to_string()),
        };
        let accessibility = evidence
            .as_ref()
            .and_then(|e| e.accessibility.clone())
            .unwrap_or_default();
        let license_proxy_suspected = self.detect_license_proxy(conn, input.ccn, input.provider_npi, evidence.as_ref());

        // Legacy wide table (compat path)
        let _ = conn.execute(
            "INSERT INTO prices (
                ccn, cpt_code, description, gross_charge, cash_price,
                min_negotiated, max_negotiated, payer, plan,
                provider_npi, attribution_confidence, last_updated
            )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, datetime('now'))
             ON CONFLICT(ccn, cpt_code, description, payer, plan)
             DO UPDATE SET
                gross_charge = excluded.gross_charge,
                cash_price = excluded.cash_price,
                min_negotiated = excluded.min_negotiated,
                max_negotiated = excluded.max_negotiated,
                provider_npi = excluded.provider_npi,
                attribution_confidence = excluded.attribution_confidence,
                last_updated = datetime('now')",
            params![
                input.ccn,
                input.code,
                input.description,
                input.gross_charge,
                input.cash_price,
                input.negotiated_rate,
                input.negotiated_rate,
                input.payer,
                input.plan,
                input.provider_npi.unwrap_or(""),
                confidence,
            ],
        );

        let hospital_id = self.upsert_hospital(conn, input.ccn);
        let procedure_id = self.upsert_procedure(conn, input.code_type, input.code, input.description);
        let payer_id = self.upsert_payer(conn, input.payer);
        let plan_id = self.upsert_plan(conn, payer_id, input.plan);
        let source_row_hash = self.make_row_hash(input, confidence);

        let _ = conn.execute(
            "INSERT OR REPLACE INTO fact_negotiated_rate (
                snapshot_date, hospital_id, procedure_id, payer_id, plan_id, provider_id,
                negotiated_rate, allowed_median, allowed_p10, allowed_p90,
                currency, source_file, source_row_hash, attribution_confidence
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'USD', '', ?11, ?12)",
            params![
                &self.snapshot_date,
                hospital_id,
                procedure_id,
                payer_id,
                plan_id,
                provider_id,
                input.negotiated_rate,
                input.allowed_median,
                input.allowed_p10,
                input.allowed_p90,
                source_row_hash,
                confidence,
            ],
        );

        let delta_abs = if input.cash_price > 0.0 && input.negotiated_rate > 0.0 {
            input.negotiated_rate - input.cash_price
        } else {
            0.0
        };
        let delta_pct = if input.cash_price > 0.0 && input.negotiated_rate > 0.0 {
            ((input.negotiated_rate - input.cash_price) / input.cash_price) * 100.0
        } else {
            0.0
        };

        let _ = conn.execute(
            "INSERT OR REPLACE INTO hot_price_compare (
                snapshot_date, ccn, hospital_name, code_type, code, payer_name, plan_name,
                cash_price, negotiated_rate, allowed_median, allowed_p10, allowed_p90,
                attribution_confidence, zombie_status, zombie_reason_code, accessibility,
                license_proxy_suspected, delta_abs, delta_pct
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            params![
                &self.snapshot_date,
                input.ccn,
                hospital_name,
                input.code_type,
                input.code,
                input.payer,
                input.plan,
                input.cash_price,
                input.negotiated_rate,
                input.allowed_median,
                input.allowed_p10,
                input.allowed_p90,
                confidence,
                zombie_status,
                zombie_reason_code,
                accessibility,
                if license_proxy_suspected { 1 } else { 0 },
                delta_abs,
                delta_pct,
            ],
        );
    }

    fn normalize(s: &str) -> String {
        s.trim().to_ascii_lowercase()
    }

    fn upsert_hospital(&self, conn: &mut Connection, ccn: &str) -> i64 {
        let placeholder = format!("Hospital {}", ccn);
        let _ = conn.execute(
            "INSERT OR IGNORE INTO dim_hospital (ccn, name, effective_date) VALUES (?1, ?2, datetime('now'))",
            params![ccn, placeholder],
        );
        conn
            .query_row(
                "SELECT hospital_id FROM dim_hospital WHERE ccn = ?1",
                params![ccn],
                |row| row.get(0),
            )
            .unwrap_or(0)
    }

    fn upsert_procedure(&self, conn: &mut Connection, code_type: &str, code: &str, description: &str) -> i64 {
        let _ = conn.execute(
            "INSERT OR IGNORE INTO dim_procedure (code_type, code, description) VALUES (?1, ?2, ?3)",
            params![code_type, code, description],
        );
        conn
            .query_row(
                "SELECT procedure_id FROM dim_procedure WHERE code_type = ?1 AND code = ?2",
                params![code_type, code],
                |row| row.get(0),
            )
            .unwrap_or(0)
    }

    fn upsert_payer(&self, conn: &mut Connection, payer: &str) -> Option<i64> {
        if payer.trim().is_empty() {
            return None;
        }
        let normalized = Self::normalize(payer);
        let _ = conn.execute(
            "INSERT OR IGNORE INTO dim_payer (payer_name, normalized_name) VALUES (?1, ?2)",
            params![payer.trim(), normalized],
        );
        conn
            .query_row(
                "SELECT payer_id FROM dim_payer WHERE normalized_name = ?1",
                params![normalized],
                |row| row.get(0),
            )
            .ok()
    }

    fn upsert_plan(&self, conn: &mut Connection, payer_id: Option<i64>, plan: &str) -> Option<i64> {
        let pid = payer_id?;
        if plan.trim().is_empty() {
            return None;
        }
        let normalized = Self::normalize(plan);
        let _ = conn.execute(
            "INSERT OR IGNORE INTO dim_plan (payer_id, plan_name, normalized_name) VALUES (?1, ?2, ?3)",
            params![pid, plan.trim(), normalized],
        );
        conn
            .query_row(
                "SELECT plan_id FROM dim_plan WHERE payer_id = ?1 AND normalized_name = ?2",
                params![pid, normalized],
                |row| row.get(0),
            )
            .ok()
    }

    fn upsert_provider(&self, conn: &mut Connection, provider_npi: Option<&str>) -> Option<i64> {
        let npi = provider_npi?.trim();
        if npi.is_empty() {
            return None;
        }

        let _ = conn.execute(
            "INSERT OR IGNORE INTO dim_provider_npi (npi) VALUES (?1)",
            params![npi],
        );

        // Hydrate provider dimension from NPPES V2 reference tables when available.
        let _ = conn.execute(
            "UPDATE dim_provider_npi
             SET
                entity_type = COALESCE((SELECT entity_type FROM nppes_provider_core WHERE npi = ?1), entity_type),
                org_name = COALESCE((SELECT org_name FROM nppes_provider_core WHERE npi = ?1), org_name),
                primary_taxonomy = COALESCE((SELECT primary_taxonomy FROM nppes_provider_core WHERE npi = ?1), primary_taxonomy),
                nppes_last_seen = COALESCE((SELECT last_updated FROM nppes_provider_core WHERE npi = ?1), nppes_last_seen),
                deactivation_date = COALESCE((SELECT deactivation_date FROM nppes_deactivations WHERE npi = ?1), deactivation_date),
                deactivation_reason_code = COALESCE((SELECT reason_code FROM nppes_deactivations WHERE npi = ?1), deactivation_reason_code),
                accessibility = COALESCE((SELECT accessibility FROM nppes_provider_core WHERE npi = ?1), accessibility),
                secondary_languages = COALESCE((SELECT secondary_languages FROM nppes_provider_core WHERE npi = ?1), secondary_languages),
                direct_email = COALESCE((SELECT direct_email FROM nppes_provider_core WHERE npi = ?1), direct_email)
             WHERE npi = ?1",
            params![npi],
        );

        conn
            .query_row(
                "SELECT provider_id FROM dim_provider_npi WHERE npi = ?1",
                params![npi],
                |row| row.get(0),
            )
            .ok()
    }

    fn compute_confidence(
        &self,
        conn: &mut Connection,
        ccn: &str,
        provider_npi: Option<&str>,
        payer: &str,
        plan: &str,
    ) -> f64 {
        let mut exact_type2 = false;
        let mut address_proximity_match = false;
        let mut taxonomy_compatible = false;
        let mut confidence_penalty: f64 = 0.0;

        if let Some(npi) = provider_npi {
            let clean = npi.trim();
            if clean.len() == 10 && clean.chars().all(|c| c.is_ascii_digit()) {
                if let Some(evidence) = self.query_provider_evidence(conn, clean) {
                    exact_type2 = evidence.entity_type.as_deref() == Some("2");
                    taxonomy_compatible = evidence
                        .primary_taxonomy
                        .as_deref()
                        .map(|t| !t.trim().is_empty())
                        .unwrap_or(false);

                    let hospital_state = self.query_hospital_state(conn, ccn);
                    address_proximity_match = self.matches_hospital_state(conn, clean, hospital_state.as_deref(), evidence.practice_state.as_deref());

                    if evidence
                        .accessibility
                        .as_deref()
                        .map(|v| !v.trim().is_empty())
                        .unwrap_or(false)
                    {
                        let _ = conn.execute(
                            "INSERT OR REPLACE INTO npi_audit_findings (
                                snapshot_date, ccn, npi, finding_type, severity, reason_code, notes
                            ) VALUES (?1, ?2, ?3, 'accessibility_signal', 'info', NULL, 'NPPES indicates accessibility metadata present')",
                            params![&self.snapshot_date, ccn, clean],
                        );
                    }

                    if evidence
                        .secondary_languages
                        .as_deref()
                        .map(|v| !v.trim().is_empty())
                        .unwrap_or(false)
                    {
                        let _ = conn.execute(
                            "INSERT OR REPLACE INTO npi_audit_findings (
                                snapshot_date, ccn, npi, finding_type, severity, reason_code, notes
                            ) VALUES (?1, ?2, ?3, 'language_coverage_signal', 'info', NULL, 'NPPES secondary languages present')",
                            params![&self.snapshot_date, ccn, clean],
                        );
                    }

                    if evidence
                        .direct_email
                        .as_deref()
                        .map(|v| !v.trim().is_empty())
                        .unwrap_or(false)
                    {
                        let _ = conn.execute(
                            "INSERT OR REPLACE INTO npi_audit_findings (
                                snapshot_date, ccn, npi, finding_type, severity, reason_code, notes
                            ) VALUES (?1, ?2, ?3, 'direct_contact_signal', 'info', NULL, 'NPPES direct email is available')",
                            params![&self.snapshot_date, ccn, clean],
                        );
                    }

                    if let Some(code) = evidence.deactivation_reason_code.as_deref() {
                        let (severity, penalty, label) = match code {
                            "4" => ("critical", 0.35, "misuse/identity theft"),
                            "1" => ("high", 0.20, "death"),
                            "2" => ("high", 0.15, "retirement"),
                            _ => ("medium", 0.10, "deactivation"),
                        };
                        confidence_penalty = confidence_penalty.max(penalty);
                        let notes = format!(
                            "NPI {} flagged as {} (code {}) on {}",
                            clean,
                            label,
                            code,
                            evidence.deactivation_date.as_deref().unwrap_or("unknown date")
                        );
                        let _ = conn.execute(
                            "INSERT OR REPLACE INTO npi_audit_findings (
                                snapshot_date, ccn, npi, finding_type, severity, reason_code, notes
                            ) VALUES (?1, ?2, ?3, 'zombie_npi', ?4, ?5, ?6)",
                            params![
                                &self.snapshot_date,
                                ccn,
                                clean,
                                severity,
                                code,
                                notes,
                            ],
                        );
                    }
                }
            }
        }

        let signals = AttributionSignals {
            exact_type2_npi_match: exact_type2,
            address_proximity_match,
            taxonomy_compatible,
            payer_plan_consistent: !payer.trim().is_empty() && !plan.trim().is_empty(),
        };

        (score(&signals) - confidence_penalty).clamp(0.0, 1.0)
    }

    fn query_provider_evidence(&self, conn: &mut Connection, npi: &str) -> Option<ProviderEvidence> {
        conn
            .query_row(
                "SELECT
                    org_name,
                    entity_type,
                    primary_taxonomy,
                    (SELECT practice_state FROM nppes_provider_core WHERE npi = ?1),
                    deactivation_reason_code,
                    deactivation_date,
                    accessibility,
                    secondary_languages,
                    direct_email
                 FROM dim_provider_npi
                 WHERE npi = ?1",
                params![npi],
                |row| {
                    Ok(ProviderEvidence {
                        org_name: row.get(0)?,
                        entity_type: row.get(1)?,
                        primary_taxonomy: row.get(2)?,
                        practice_state: row.get(3)?,
                        deactivation_reason_code: row.get(4)?,
                        deactivation_date: row.get(5)?,
                        accessibility: row.get(6)?,
                        secondary_languages: row.get(7)?,
                        direct_email: row.get(8)?,
                    })
                },
            )
            .ok()
    }

    fn query_hospital_name(&self, conn: &mut Connection, ccn: &str) -> Option<String> {
        conn
            .query_row(
                "SELECT name FROM dim_hospital WHERE ccn = ?1",
                params![ccn],
                |row| row.get(0),
            )
            .ok()
    }

    fn detect_license_proxy(
        &self,
        conn: &mut Connection,
        ccn: &str,
        provider_npi: Option<&str>,
        evidence: Option<&ProviderEvidence>,
    ) -> bool {
        let hospital = self
            .query_hospital_name(conn, ccn)
            .unwrap_or_default();
        let provider_org = evidence
            .and_then(|e| e.org_name.clone())
            .unwrap_or_default();

        if hospital.trim().is_empty() || provider_org.trim().is_empty() {
            return false;
        }

        if Self::names_match(&hospital, &provider_org) {
            return false;
        }

        let npi = match provider_npi {
            Some(n) if !n.trim().is_empty() => n.trim(),
            _ => return true,
        };

        let mut stmt = match conn.prepare("SELECT other_name FROM nppes_other_names WHERE npi = ?1") {
            Ok(s) => s,
            Err(_) => return true,
        };
        let aliases = match stmt.query_map(params![npi], |row| row.get::<_, String>(0)) {
            Ok(a) => a,
            Err(_) => return true,
        };

        for alias in aliases.flatten() {
            if Self::names_match(&hospital, &alias) {
                return false;
            }
        }

        true
    }

    fn names_match(a: &str, b: &str) -> bool {
        fn normalize_name(s: &str) -> String {
            s.to_ascii_lowercase()
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { ' ' })
                .collect::<String>()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        }

        let an = normalize_name(a);
        let bn = normalize_name(b);
        an == bn || an.contains(&bn) || bn.contains(&an)
    }

    fn query_hospital_state(&self, conn: &mut Connection, ccn: &str) -> Option<String> {
        conn
            .query_row(
                "SELECT state FROM dim_hospital WHERE ccn = ?1",
                params![ccn],
                |row| row.get(0),
            )
            .ok()
    }

    fn matches_hospital_state(
        &self,
        conn: &mut Connection,
        npi: &str,
        hospital_state: Option<&str>,
        provider_core_state: Option<&str>,
    ) -> bool {
        let hs = match hospital_state {
            Some(s) if !s.trim().is_empty() => s.trim().to_ascii_uppercase(),
            _ => return false,
        };

        if provider_core_state
            .map(|s| s.trim().eq_ignore_ascii_case(&hs))
            .unwrap_or(false)
        {
            return true;
        }

        let mut stmt = match conn.prepare("SELECT state FROM nppes_practice_locations WHERE npi = ?1") {
            Ok(s) => s,
            Err(_) => return false,
        };

        let rows = match stmt.query_map(params![npi], |row| row.get::<_, Option<String>>(0)) {
            Ok(r) => r,
            Err(_) => return false,
        };

        for row in rows.flatten() {
            if let Some(state) = row {
                if state.trim().eq_ignore_ascii_case(&hs) {
                    return true;
                }
            }
        }

        false
    }

    fn make_row_hash(&self, input: &PriceFactInput<'_>, confidence: f64) -> String {
        let mut hasher = DefaultHasher::new();
        input.ccn.hash(&mut hasher);
        input.code_type.hash(&mut hasher);
        input.code.hash(&mut hasher);
        input.description.hash(&mut hasher);
        input.payer.hash(&mut hasher);
        input.plan.hash(&mut hasher);
        input.provider_npi.unwrap_or("").hash(&mut hasher);
        self.snapshot_date.hash(&mut hasher);
        confidence.to_bits().hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE prices (
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
            CREATE UNIQUE INDEX idx_prices_upsert_key ON prices (ccn, cpt_code, description, payer, plan);
            CREATE TABLE dim_hospital (hospital_id INTEGER PRIMARY KEY, ccn TEXT UNIQUE NOT NULL, name TEXT NOT NULL, state TEXT, effective_date TEXT NOT NULL);
            CREATE TABLE dim_payer (payer_id INTEGER PRIMARY KEY, payer_name TEXT NOT NULL, normalized_name TEXT NOT NULL UNIQUE);
            CREATE TABLE dim_plan (plan_id INTEGER PRIMARY KEY, payer_id INTEGER NOT NULL, plan_name TEXT NOT NULL, normalized_name TEXT NOT NULL, UNIQUE(payer_id, normalized_name));
            CREATE TABLE dim_provider_npi (
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
            );
            CREATE TABLE dim_procedure (procedure_id INTEGER PRIMARY KEY, code_type TEXT NOT NULL, code TEXT NOT NULL, description TEXT, UNIQUE(code_type, code));
            CREATE TABLE fact_negotiated_rate (fact_id INTEGER PRIMARY KEY, snapshot_date TEXT NOT NULL, hospital_id INTEGER NOT NULL, procedure_id INTEGER NOT NULL, payer_id INTEGER, plan_id INTEGER, provider_id INTEGER, negotiated_rate REAL, allowed_median REAL, allowed_p10 REAL, allowed_p90 REAL, currency TEXT, source_file TEXT, source_row_hash TEXT UNIQUE, attribution_confidence REAL NOT NULL);
            CREATE TABLE hot_price_compare (snapshot_date TEXT NOT NULL, ccn TEXT NOT NULL, code_type TEXT NOT NULL, code TEXT NOT NULL, payer_name TEXT, plan_name TEXT, cash_price REAL, negotiated_rate REAL, allowed_median REAL, allowed_p10 REAL, allowed_p90 REAL, attribution_confidence REAL, delta_abs REAL, delta_pct REAL, PRIMARY KEY (snapshot_date, ccn, code_type, code, payer_name, plan_name));
            CREATE TABLE nppes_provider_core (npi TEXT PRIMARY KEY, entity_type TEXT, org_name TEXT, primary_taxonomy TEXT, practice_state TEXT, practice_city TEXT, practice_postal_code TEXT, accessibility TEXT, secondary_languages TEXT, direct_email TEXT, last_updated TEXT);
            CREATE TABLE nppes_practice_locations (id INTEGER PRIMARY KEY AUTOINCREMENT, npi TEXT NOT NULL, address_1 TEXT, address_2 TEXT, city TEXT, state TEXT, postal_code TEXT, phone TEXT, is_primary INTEGER DEFAULT 0);
            CREATE TABLE nppes_deactivations (npi TEXT PRIMARY KEY, deactivation_date TEXT, reactivation_date TEXT, reason_code TEXT, reason_text TEXT);
            CREATE TABLE npi_audit_findings (id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL, ccn TEXT NOT NULL, npi TEXT NOT NULL, finding_type TEXT NOT NULL, severity TEXT NOT NULL, reason_code TEXT, notes TEXT, UNIQUE(snapshot_date, ccn, npi, finding_type));
            ",
        ).unwrap();
        conn
    }

    #[test]
    fn flags_zombie_npi_for_reason_code_4_and_penalizes_confidence() {
        let mut conn = setup_conn();
        conn.execute(
            "INSERT INTO dim_hospital (ccn, name, state, effective_date) VALUES ('450056', 'Test Hospital', 'TX', datetime('now'))",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO nppes_provider_core (npi, entity_type, org_name, primary_taxonomy, practice_state, accessibility, secondary_languages, direct_email, last_updated)
             VALUES ('1234567890', '2', 'Test Org', '282N00000X', 'TX', 'Y', 'EN,ES', 'ops@test.org', '2026-03-01')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO nppes_deactivations (npi, deactivation_date, reason_code, reason_text)
             VALUES ('1234567890', '2026-02-15', '4', 'Misused/Identity Theft')",
            [],
        ).unwrap();

        let writer = FactWriter::new("2026-04-09".to_string());
        writer.write_dual(
            &mut conn,
            &PriceFactInput {
                ccn: "450056",
                code_type: "CPT",
                code: "70450",
                description: "Head CT",
                gross_charge: 1200.0,
                cash_price: 450.0,
                negotiated_rate: 1100.0,
                payer: "Example Health",
                plan: "Gold PPO",
                provider_npi: Some("1234567890"),
                allowed_median: Some(1000.0),
                allowed_p10: Some(700.0),
                allowed_p90: Some(1400.0),
            },
        );

        let confidence: f64 = conn.query_row(
            "SELECT attribution_confidence FROM prices WHERE ccn='450056' AND cpt_code='70450'",
            [],
            |r| r.get(0),
        ).unwrap();
        assert!(confidence < 0.9);

        let zombie_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM npi_audit_findings WHERE finding_type='zombie_npi' AND reason_code='4'",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(zombie_count, 1);
    }
}
