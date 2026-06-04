use crate::schema::init_db;
use reqwest::{Client, StatusCode, Proxy};
use std::time::Duration;
use tokio::time::sleep;
// use std::sync::{Arc, Mutex};
use tracing::{info, warn};
use futures::stream::{StreamExt, FuturesUnordered};

/// Browser-spoofing User-Agent. Identical to Chrome 124 on macOS.
const USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
     AppleWebKit/537.36 (KHTML, like Gecko) \
     Chrome/124.0.0.0 Safari/537.36";

pub struct AuditInput {
    pub mrf_machine_readable: bool,
}

/// Fetch a URL with exponential backoff on 429/503.
/// Returns (status_code, body_text, waf_blocked).
async fn fetch_with_backoff(client: &Client, url: &str) -> (Option<StatusCode>, Option<String>, bool) {
    let delays = [0, 2, 4, 8]; // seconds between retries

    for (attempt, delay_secs) in delays.iter().enumerate() {
        if *delay_secs > 0 {
            sleep(Duration::from_secs(*delay_secs)).await;
        }

        let resp = match client.get(url)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Connection", "keep-alive")
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Request to {} failed (attempt {}): {}", url, attempt + 1, e);
                continue;
            }
        };

        let status = resp.status();

        // WAF challenge / active block
        if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED {
            warn!("WAF block detected at {} ({})", url, status);

            // Attempt residential proxy fallback if WAF is blocking Microsoft Azure CI IPs
            if let Ok(proxy_url) = std::env::var("FALLBACK_PROXY") {
                if attempt < delays.len() - 1 {
                    warn!("Deploying residential proxy for {}...", url);
                    if let Ok(proxy) = Proxy::all(&proxy_url) {
                        let proxy_client = Client::builder()
                            .timeout(Duration::from_secs(15))
                            .user_agent(USER_AGENT)
                            .proxy(proxy)
                            .build()
                            .unwrap_or(client.clone());

                        match proxy_client.get(url).send().await {
                            Ok(pr) if pr.status().is_success() => {
                                info!("Proxy bypassed WAF block for {}!", url);
                                return (Some(pr.status()), Some(pr.text().await.unwrap_or_default()), false);
                            }
                            _ => { warn!("Proxy fallback failed to bypass WAF for {}", url); }
                        }
                    }
                }
            }

            return (Some(status), None, true);
        }

        // Rate limited — retry
        if status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 503 {
            warn!("Rate limit {} at {}, retrying...", status, url);
            continue;
        }

        let body = resp.text().await.unwrap_or_default();
        return (Some(status), Some(body), false);
    }

    // All retries exhausted
    (None, None, false)
}

/// Validate state code format (2-letter US states + territories + military codes)
fn is_valid_state_code(code: &str) -> bool {
    matches!(code.to_uppercase().as_str(),
        "AL" | "AK" | "AZ" | "AR" | "CA" | "CO" | "CT" | "DE" |
        "FL" | "GA" | "HI" | "ID" | "IL" | "IN" | "IA" | "KS" |
        "KY" | "LA" | "ME" | "MD" | "MA" | "MI" | "MN" | "MS" |
        "MO" | "MT" | "NE" | "NV" | "NH" | "NJ" | "NM" | "NY" |
        "NC" | "ND" | "OH" | "OK" | "OR" | "PA" | "RI" | "SC" |
        "SD" | "TN" | "TX" | "UT" | "VT" | "VA" | "WA" | "WV" |
        "WI" | "WY" | "DC" | "AS" | "GU" | "MP" | "PR" | "VI" | "AA" | "AE" | "AP"
    )
}

pub async fn run_auditor(state_filter: Option<String>) -> anyhow::Result<()> {
    info!("Starting high-concurrency auditor phase...");
    let conn = init_db()?; // init_db should ideally return a pool in production

    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent(USER_AGENT)
        .build()?;

    // Validate state code if provided
    if let Some(ref state) = state_filter {
        if !is_valid_state_code(state) {
            anyhow::bail!("Invalid state code: {}. Must be a valid 2-letter US state or territory code.", state);
        }
    }

    let mut hospitals_to_audit = Vec::new();
    {
        let mut stmt = if state_filter.is_some() {
            conn.prepare("SELECT ccn, website, cms_hpt_url FROM hospitals WHERE state = ?1")?
        } else {
            conn.prepare("SELECT ccn, website, cms_hpt_url FROM hospitals")?
        };

        let to_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<(String, String, String)> {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        };

        let iter = if let Some(state) = state_filter.as_ref() {
            stmt.query_map(rusqlite::params![state.to_uppercase()], to_row)?
        } else {
            stmt.query_map([], to_row)?
        };
        for row in iter {
            hospitals_to_audit.push(row?);
        }
    }

    info!("Auditing {} hospital nodes concurrently...", hospitals_to_audit.len());

    let mut stream = FuturesUnordered::new();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(32)); // Concurrency throttle

    for (ccn, website, txt_url) in hospitals_to_audit {
        let client_clone = client.clone();
        let sem = semaphore.clone();
        
        stream.push(async move {
            let _permit = sem.acquire().await.unwrap();
            audit_hospital(&client_clone, ccn, website, txt_url).await
        });
    }

    // Process results as they come in
    while let Some(result) = stream.next().await {
        if let Err(e) = result {
            warn!("Audit task failure: {}", e);
        }
    }

    info!("Auditor phase completed.");
    Ok(())
}

async fn audit_hospital(client: &Client, ccn: String, website: String, txt_url: String) -> anyhow::Result<()> {
    if txt_url.is_empty() {
        return Ok(());
    }

    let mut evidence = String::new();
    let mut score: i64 = 0;

    // --- Check 1: cms-hpt.txt ---
    let (hpt_status, _hpt_body, hpt_waf) = fetch_with_backoff(client, &txt_url).await;
    let txt_exists = hpt_status.map(|s| s.is_success()).unwrap_or(false);
    let waf_blocked = hpt_waf;

    if txt_exists {
        score += 50;
        evidence.push_str("✓ cms-hpt.txt found. ");
    } else if waf_blocked {
        score -= 30;
        evidence.push_str(&format!("❌ WAF blocked cms-hpt.txt check ({}). ", hpt_status.map(|s| s.as_u16().to_string()).unwrap_or("no response".into())));
    } else {
        evidence.push_str(&format!("❌ cms-hpt.txt unreachable ({}). ", hpt_status.map(|s| s.as_u16().to_string()).unwrap_or("connection failed".into())));
    }

    // --- Check 2: robots.txt ---
    if !website.is_empty() {
        let robots_url = format!("{}/robots.txt", website);
        let (_, robots_body, _) = fetch_with_backoff(client, &robots_url).await;
        match robots_body {
            Some(text) => {
                if text.contains("Disallow: /cms-hpt.txt") || text.contains("Disallow: /") {
                    evidence.push_str("❌ robots.txt actively blocks MRF access. ");
                } else {
                    score += 10;
                }
            }
            None => {} 
        };
    }

    // In a high-concurrency production system, we'd use a pool of connections (e.g., r2d2)
    // For this single-file audit, we'll re-open to persist results efficiently.
    let conn = init_db()?; 
    conn.execute(
        "INSERT OR REPLACE INTO compliance
            (ccn, score, txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh,
                shoppable_exists, mrf_machine_readable, waf_blocked, last_checked, evidence_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'), ?11)",
        rusqlite::params![
            ccn, score, txt_exists, true,
            txt_exists, 
            false, false, false,
            1,        
            waf_blocked,
            evidence
        ],
    )?;

    Ok(())
}

/// Update compliance record with parser results.
pub fn update_parse_result(
    ccn: &str,
    mrf_machine_readable: bool,
    records_found: usize,
) -> anyhow::Result<()> {
    let conn = init_db()?;

    let parse_penalty: i64 = if mrf_machine_readable { 0 } else { -40 };
    let evidence_suffix = if mrf_machine_readable {
        format!("✓ MRF parsed: {} shoppable records found. ", records_found)
    } else {
        "❌ MRF is machine-unreadable (malformed schema, encoding error, or unsupported format). ".to_string()
    };

    conn.execute(
        "UPDATE compliance SET
            mrf_machine_readable = ?1,
            mrf_valid = ?2,
            score = MAX(0, score + ?3),
            evidence_json = evidence_json || ?4
         WHERE ccn = ?5",
        rusqlite::params![
            mrf_machine_readable,
            mrf_machine_readable,
            parse_penalty,
            evidence_suffix,
            ccn
        ],
    )?;

    Ok(())
}
