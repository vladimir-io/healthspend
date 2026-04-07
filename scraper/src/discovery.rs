use crate::schema::init_db;
use csv::{ByteRecord, ReaderBuilder};
use rayon::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, CONNECTION, UPGRADE_INSECURE_REQUESTS};
use reqwest::Url;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

/// state_filter: a comma-separated list of state abbreviations, or None for all states.
/// When passed from --shard-states, this lets a single runner process TX,OK,AR in one job.

pub struct DiscoveryOptions {
    pub state_filter: Option<String>,
    pub seed_file: String,
    pub nppes_endpoints_file: Option<String>,
    pub ccn_npi_crosswalk_file: Option<String>,
    pub nppes_core_file: Option<String>,
    pub fuzzy_match_nppes: bool,
}

fn normalize_header(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

fn find_col(headers: &csv::StringRecord, candidates: &[&str]) -> Option<usize> {
    let wanted: HashSet<String> = candidates.iter().map(|c| normalize_header(c)).collect();
    headers
        .iter()
        .enumerate()
        .find_map(|(i, h)| if wanted.contains(&normalize_header(h)) { Some(i) } else { None })
}

fn find_col_bytes(headers: &ByteRecord, candidates: &[&str]) -> Option<usize> {
    let wanted: HashSet<String> = candidates.iter().map(|c| normalize_header(c)).collect();
    headers
        .iter()
        .enumerate()
        .find_map(|(i, h)| {
            let field = String::from_utf8_lossy(h);
            if wanted.contains(&normalize_header(&field)) {
                Some(i)
            } else {
                None
            }
        })
}

fn resolve_input_path(seed_file: &str) -> PathBuf {
    let p = Path::new(seed_file);
    if p.is_absolute() {
        return p.to_path_buf();
    }

    if p.exists() {
        return p.to_path_buf();
    }

    let parent = Path::new("..").join(p);
    if parent.exists() {
        return parent;
    }

    p.to_path_buf()
}

fn normalize_npi(raw: &str) -> String {
    raw.chars().filter(|c| c.is_ascii_digit()).collect()
}

fn normalize_ccn(raw: &str) -> String {
    raw.chars().filter(|c| c.is_ascii_digit()).collect()
}

fn normalize_name(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn zip5(raw: &str) -> String {
    raw.chars().filter(|c| c.is_ascii_digit()).take(5).collect()
}

struct NppesCoreIndex {
    conn: Connection,
}

struct SeedRow {
    ccn: String,
    name: String,
    city: String,
    state: String,
    source_website: String,
    source_zip: String,
    source_npi: String,
}

struct DiscoveryRow {
    ccn: String,
    name: String,
    state: String,
    city: String,
    website: String,
    cms_hpt_url: String,
    mrf_url: String,
    attempted_enrichment: usize,
    found_cms_hpt: usize,
    found_mrf: usize,
    endpoint_website_hit: usize,
    fuzzy_npi_hit: usize,
}

struct CcnNpiCrosswalkIndex {
    map: HashMap<String, String>,
}

impl CcnNpiCrosswalkIndex {
    fn build(path: &str) -> anyhow::Result<Self> {
        let resolved = resolve_input_path(path);
        let file = File::open(&resolved)?;
        let mut rdr = ReaderBuilder::new().flexible(true).from_reader(BufReader::new(file));
        let headers = rdr.byte_headers()?.clone();

        let idx_ccn = find_col_bytes(
            &headers,
            &["CCN", "Facility ID", "CAH OR HOSPITAL CCN", "Provider Number"],
        )
        .ok_or_else(|| anyhow::anyhow!("Hospital enrollment crosswalk file missing CCN column"))?;
        let idx_npi = find_col_bytes(&headers, &["NPI", "Provider NPI", "npi_number"])
            .ok_or_else(|| anyhow::anyhow!("Hospital enrollment crosswalk file missing NPI column"))?;

        let mut map = HashMap::new();
        let mut loaded = 0usize;
        for row in rdr.byte_records() {
            let rec = row?;
            let ccn = normalize_ccn(&String::from_utf8_lossy(rec.get(idx_ccn).unwrap_or(b"")));
            let npi = normalize_npi(&String::from_utf8_lossy(rec.get(idx_npi).unwrap_or(b"")));
            if ccn.is_empty() || npi.is_empty() {
                continue;
            }

            map.entry(ccn).or_insert_with(|| {
                loaded += 1;
                npi
            });
        }

        info!("Loaded {} CCN to NPI crosswalk rows", loaded);
        Ok(Self { map })
    }

    fn lookup_npi(&self, ccn: &str) -> Option<String> {
        let ccn = normalize_ccn(ccn);
        if ccn.is_empty() {
            return None;
        }
        self.map.get(&ccn).cloned()
    }
}

impl NppesCoreIndex {
    fn build(path: &str) -> anyhow::Result<Self> {
        let resolved = resolve_input_path(path);
        let file = File::open(&resolved)?;
        let mut rdr = ReaderBuilder::new().flexible(true).from_reader(BufReader::new(file));
        let headers = rdr.headers()?.clone();

        let idx_npi = find_col(&headers, &["NPI", "Provider NPI", "npi_number"])
            .ok_or_else(|| anyhow::anyhow!("NPPES core file missing NPI column"))?;
        let idx_name = find_col(
            &headers,
            &[
                "Provider Organization Name (Legal Business Name)",
                "Provider Organization Name",
                "org_name",
                "Provider Last Name (Legal Name)",
            ],
        )
        .ok_or_else(|| anyhow::anyhow!("NPPES core file missing organization/name column"))?;
        let idx_zip = find_col(
            &headers,
            &[
                "Provider Business Practice Location Address Postal Code",
                "Practice Postal Code",
                "practice_postal_code",
                "Postal Code",
            ],
        )
        .ok_or_else(|| anyhow::anyhow!("NPPES core file missing practice postal code column"))?;

        let mut conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;

            CREATE TABLE nppes_core_lookup (
                npi TEXT NOT NULL,
                zip5 TEXT NOT NULL,
                name_norm TEXT NOT NULL
            );
            CREATE INDEX idx_nppes_zip ON nppes_core_lookup(zip5);
            CREATE INDEX idx_nppes_zip_name ON nppes_core_lookup(zip5, name_norm);
            ",
        )?;

        let tx = conn.transaction()?;
        let mut stmt = tx.prepare(
            "INSERT INTO nppes_core_lookup (npi, zip5, name_norm) VALUES (?1, ?2, ?3)",
        )?;

        let mut inserted = 0usize;
        for row in rdr.records() {
            let rec = row?;
            let npi = normalize_npi(rec.get(idx_npi).unwrap_or(""));
            if npi.is_empty() {
                continue;
            }

            let name_norm = normalize_name(rec.get(idx_name).unwrap_or(""));
            if name_norm.is_empty() {
                continue;
            }

            let zip = zip5(rec.get(idx_zip).unwrap_or(""));
            if zip.is_empty() {
                continue;
            }

            stmt.execute(params![npi, zip, name_norm])?;
            inserted += 1;
        }

        drop(stmt);
        tx.commit()?;
        info!("Indexed {} NPPES core rows for fuzzy Name+ZIP matching", inserted);

        Ok(Self { conn })
    }

    fn lookup_npi(&self, hospital_name: &str, hospital_zip: &str) -> Option<String> {
        let zip = zip5(hospital_zip);
        if zip.is_empty() {
            return None;
        }

        let name_norm = normalize_name(hospital_name);
        if name_norm.is_empty() {
            return None;
        }

        let like = format!("%{}%", name_norm);
        self.conn
            .query_row(
                "
                SELECT npi
                FROM nppes_core_lookup
                WHERE zip5 = ?1
                  AND name_norm LIKE ?2
                ORDER BY LENGTH(name_norm)
                LIMIT 1
                ",
                params![zip, like],
                |row| row.get::<_, String>(0),
            )
            .ok()
    }
}

fn load_seed_rows(
    seed_path: &Path,
    state_set: &Option<Vec<String>>,
) -> anyhow::Result<Vec<SeedRow>> {
    let file = File::open(seed_path)?;
    let mut rdr = ReaderBuilder::new().from_reader(BufReader::new(file));
    let headers = rdr.headers()?.clone();

    let idx_ccn = find_col(&headers, &["Facility ID", "PRVDR_NUM", "Provider Number", "CCN", "ccn"])
        .unwrap_or(0);
    let idx_name = find_col(&headers, &["Facility Name", "FCLTY_NAME", "Hospital Name", "name"])
        .unwrap_or(1);
    let idx_city = find_col(&headers, &["City/Town", "CITY_NAME", "city", "City"])
        .unwrap_or(3);
    let idx_state = find_col(&headers, &["State", "STATE_CD", "state", "State Code"])
        .unwrap_or(4);
    let idx_zip = find_col(&headers, &["ZIP Code", "ZIP_CD", "Postal Code", "Zip", "zip"]);
    let idx_website = find_col(&headers, &["Website", "Hospital Website", "Facility Website", "URL", "web_site"]);
    let idx_npi = find_col(&headers, &["NPI", "Provider NPI", "Attestation NPI", "npi"]);

    let mut rows = Vec::new();
    let mut seen_ccn = HashSet::new();

    for result in rdr.records() {
        let record = result?;
        let ccn = record.get(idx_ccn).unwrap_or("").trim().to_string();
        let name = record.get(idx_name).unwrap_or("").trim().to_string();
        let city = record.get(idx_city).unwrap_or("").trim().to_string();
        let state = record.get(idx_state).unwrap_or("").trim().to_string();
        let source_website = idx_website
            .and_then(|i| record.get(i))
            .unwrap_or("")
            .trim()
            .to_string();
        let source_zip = idx_zip
            .and_then(|i| record.get(i))
            .unwrap_or("")
            .trim()
            .to_string();
        let source_npi = idx_npi
            .and_then(|i| record.get(i))
            .map(normalize_npi)
            .unwrap_or_default();

        if ccn.is_empty() || name.is_empty() || state.is_empty() {
            continue;
        }

        if !seen_ccn.insert(ccn.clone()) {
            continue;
        }

        if let Some(states) = state_set {
            if !states.contains(&state.to_uppercase()) {
                continue;
            }
        }

        rows.push(SeedRow {
            ccn,
            name,
            city,
            state,
            source_website,
            source_zip,
            source_npi,
        });
    }

    Ok(rows)
}

fn load_existing_hospital_state(conn: &Connection) -> anyhow::Result<HashMap<String, (String, String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT ccn, COALESCE(website, ''), COALESCE(cms_hpt_url, ''), COALESCE(mrf_url, '') FROM hospitals",
    )?;

    let mut map = HashMap::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let ccn: String = row.get(0)?;
        let website: String = row.get(1)?;
        let cms_hpt_url: String = row.get(2)?;
        let mrf_url: String = row.get(3)?;
        map.insert(ccn, (website, cms_hpt_url, mrf_url));
    }

    Ok(map)
}

fn normalize_website(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.trim_end_matches('/').to_string();
    }

    format!("https://{}", trimmed.trim_end_matches('/'))
}

fn website_from_endpoint(raw: &str) -> String {
    let url = raw.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return String::new();
    }

    let Ok(parsed) = Url::parse(url) else {
        return String::new();
    };

    if let Some(host) = parsed.host_str() {
        format!("{}://{}", parsed.scheme(), host)
    } else {
        String::new()
    }
}

fn load_nppes_endpoint_map(path: &str) -> anyhow::Result<HashMap<String, String>> {
    let resolved = resolve_input_path(path);
    let file = File::open(&resolved)?;
    let mut rdr = ReaderBuilder::new().flexible(true).from_reader(BufReader::new(file));
    let headers = rdr.headers()?.clone();

    let idx_npi = find_col(&headers, &["NPI", "Provider NPI", "npi_number"]) 
        .ok_or_else(|| anyhow::anyhow!("NPPES endpoint file missing NPI column"))?;
    let idx_endpoint = find_col(
        &headers,
        &[
            "Endpoint",
            "Endpoint URL",
            "Endpoint Location",
            "Endpoint Reference",
            "endpoint",
        ],
    )
    .ok_or_else(|| anyhow::anyhow!("NPPES endpoint file missing endpoint URL column"))?;

    let mut map = HashMap::new();
    for row in rdr.records() {
        let rec = row?;
        let npi = normalize_npi(rec.get(idx_npi).unwrap_or(""));
        if npi.is_empty() {
            continue;
        }
        let website = website_from_endpoint(rec.get(idx_endpoint).unwrap_or(""));
        if website.is_empty() {
            continue;
        }
        map.entry(npi).or_insert(website);
    }

    Ok(map)
}

fn candidate_cms_hpt_urls(website: &str) -> Vec<String> {
    if website.is_empty() {
        return vec![];
    }

    let root = website.trim_end_matches('/');
    vec![
        format!("{root}/cms-hpt.txt"),
        format!("{root}/.well-known/cms-hpt.txt"),
        format!("{root}/price-transparency/cms-hpt.txt"),
        format!("{root}/transparency/cms-hpt.txt"),
        format!("{root}/standard-charges/cms-hpt.txt"),
    ]
}

fn extract_candidate_urls(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in text.split(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '<' || c == '>') {
        let t = token.trim_matches(|c: char| c == ',' || c == ';' || c == ')' || c == '(' || c == '.');
        if t.starts_with("http://") || t.starts_with("https://") {
            out.push(t.to_string());
        }
    }
    out
}

fn choose_mrf_url(text: &str, base_url: &str) -> Option<String> {
    for url in extract_candidate_urls(text) {
        let lower = url.to_ascii_lowercase();
        if lower.contains(".json") || lower.contains(".csv") || lower.contains(".json.gz") || lower.contains(".csv.gz") || lower.contains(".txt") || lower.contains(".zip") {
            return Some(url);
        }
    }

    // Relative-path fallback for cms-hpt lines like "/transparency/standardcharges.json"
    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        let lower = l.to_ascii_lowercase();
        if lower.contains(".json") || lower.contains(".csv") || lower.contains(".json.gz") || lower.contains(".csv.gz") || lower.contains(".txt") || lower.contains(".zip") {
            if (l.starts_with('/') || !l.contains("://")) && !base_url.is_empty() {
                if let Ok(base) = Url::parse(base_url) {
                    if let Ok(joined) = base.join(l) {
                        return Some(joined.to_string());
                    }
                }
            }
        }
    }
    None
}

fn discover_from_homepage(client: &Client, website: &str) -> (String, String) {
    if website.is_empty() {
        return (String::new(), String::new());
    }

    let Ok(resp) = client.get(website).send() else {
        return (String::new(), String::new());
    };
    if !resp.status().is_success() {
        return (String::new(), String::new());
    }

    let Ok(body) = resp.text() else {
        return (String::new(), String::new());
    };

    let mut landing_links = Vec::new();
    for token in body.split('"') {
        let low = token.to_ascii_lowercase();
        if low.contains("transparency") || low.contains("standard-charges") || low.contains("machine-readable") || low.contains("price") {
            landing_links.push(token.trim().to_string());
        }
    }

    let base = Url::parse(website).ok();
    for link in landing_links.into_iter().take(20) {
        let target = if link.starts_with("http://") || link.starts_with("https://") {
            link
        } else if let Some(ref b) = base {
            if let Ok(j) = b.join(&link) {
                j.to_string()
            } else {
                continue;
            }
        } else {
            continue;
        };

        if target.to_ascii_lowercase().contains("cms-hpt.txt") {
            if let Ok(r) = client.get(&target).send() {
                if r.status().is_success() {
                    if let Ok(text) = r.text() {
                        let mrf = choose_mrf_url(&text, &target).unwrap_or_default();
                        return (target, mrf);
                    }
                }
            }
        }

        if let Ok(r) = client.get(&target).send() {
            if !r.status().is_success() {
                continue;
            }
            if let Ok(text) = r.text() {
                if let Some(mrf) = choose_mrf_url(&text, &target) {
                    return (String::new(), mrf);
                }
            }
        }
    }

    (String::new(), String::new())
}

fn enrich_urls(client: &Client, website: &str, existing_cms_hpt: &str, existing_mrf: &str) -> (String, String) {
    if !existing_cms_hpt.is_empty() && !existing_mrf.is_empty() {
        return (existing_cms_hpt.to_string(), existing_mrf.to_string());
    }

    let mut cms_hpt_url = existing_cms_hpt.to_string();
    let mut mrf_url = existing_mrf.to_string();

    if !cms_hpt_url.is_empty() && mrf_url.is_empty() {
        if let Ok(resp) = client.get(&cms_hpt_url).send() {
            if resp.status().is_success() {
                if let Ok(text) = resp.text() {
                    if let Some(found) = choose_mrf_url(&text, &cms_hpt_url) {
                        mrf_url = found;
                    }
                }
            }
        }
    }

    if cms_hpt_url.is_empty() {
        for candidate in candidate_cms_hpt_urls(website) {
            if let Ok(resp) = client.get(&candidate).send() {
                if !resp.status().is_success() {
                    continue;
                }
                cms_hpt_url = candidate;
                if let Ok(text) = resp.text() {
                    if let Some(found) = choose_mrf_url(&text, &cms_hpt_url) {
                        mrf_url = found;
                    }
                }
                break;
            }
        }
    }

    if mrf_url.is_empty() {
        let (home_cms, home_mrf) = discover_from_homepage(client, website);
        if cms_hpt_url.is_empty() && !home_cms.is_empty() {
            cms_hpt_url = home_cms;
        }
        if !home_mrf.is_empty() {
            mrf_url = home_mrf;
        }
    }

    (cms_hpt_url, mrf_url)
}

pub fn run_discovery(state_filter: Option<String>) -> anyhow::Result<()> {
    run_discovery_with_options(DiscoveryOptions {
        state_filter,
        seed_file: "hospitals.csv".to_string(),
        nppes_endpoints_file: None,
        ccn_npi_crosswalk_file: None,
        nppes_core_file: None,
        fuzzy_match_nppes: false,
    })
}

pub fn run_discovery_with_options(options: DiscoveryOptions) -> anyhow::Result<()> {
    info!("Starting hospital discovery phase...");
    let conn = init_db()?;
    conn.busy_timeout(Duration::from_secs(60))?;

    let mut default_headers = HeaderMap::new();
    default_headers.insert(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"),
    );
    default_headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    default_headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    default_headers.insert(UPGRADE_INSECURE_REQUESTS, HeaderValue::from_static("1"));

    let client = Arc::new(
        Client::builder()
            .timeout(Duration::from_secs(12))
            .default_headers(default_headers)
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            .build()?,
    );

    let endpoint_map = if let Some(path) = options.nppes_endpoints_file.as_deref() {
        let m = load_nppes_endpoint_map(path)?;
        info!("Loaded {} NPPES endpoint website mappings", m.len());
        m
    } else {
        HashMap::new()
    };

    let ccn_npi_crosswalk = if let Some(path) = options.ccn_npi_crosswalk_file.as_deref() {
        Some(CcnNpiCrosswalkIndex::build(path)?)
    } else {
        let default_crosswalk = resolve_input_path("data/master2026/hospital_enrollments_2026.csv");
        if default_crosswalk.exists() {
            Some(CcnNpiCrosswalkIndex::build(default_crosswalk.to_string_lossy().as_ref())?)
        } else {
            None
        }
    };

    let nppes_core_index = if options.fuzzy_match_nppes {
        if let Some(path) = options.nppes_core_file.as_deref() {
            Some(Arc::new(Mutex::new(NppesCoreIndex::build(path)?)))
        } else {
            None
        }
    } else {
        None
    };

    // Support comma-separated state lists (from --shard-states)
    let state_set: Option<Vec<String>> = options.state_filter.map(|s| {
        s.split(',').map(|st| st.trim().to_uppercase()).collect()
    });

    let seed_path = resolve_input_path(&options.seed_file);
    let seed_rows = load_seed_rows(&seed_path, &state_set)?;

    info!("Discovery seed: {}", seed_path.display());

    let existing_hospitals = Arc::new(load_existing_hospital_state(&conn)?);
    let endpoint_map = Arc::new(endpoint_map);
    let ccn_npi_crosswalk = ccn_npi_crosswalk.map(Arc::new);

    let discovered_rows: Vec<DiscoveryRow> = seed_rows
        .par_iter()
        .map(|row| {
            let mut website = normalize_website(existing_hospitals.get(&row.ccn).map(|v| v.0.as_str()).unwrap_or(""));
            let (existing_cms_hpt, existing_mrf) = existing_hospitals
                .get(&row.ccn)
                .map(|v| (v.1.clone(), v.2.clone()))
                .unwrap_or_else(|| (String::new(), String::new()));

            if website.is_empty() {
                website = normalize_website(&row.source_website);
            }

            let mut effective_npi = row.source_npi.clone();
            if effective_npi.is_empty() {
                if let Some(index) = ccn_npi_crosswalk.as_ref() {
                    if let Some(npi) = index.lookup_npi(&row.ccn) {
                        effective_npi = npi;
                    }
                }
            }

            let mut fuzzy_npi_hit = 0usize;
            if effective_npi.is_empty() && options.fuzzy_match_nppes {
                if let Some(index) = nppes_core_index.as_ref() {
                    if let Ok(guard) = index.lock() {
                        if let Some(npi) = guard.lookup_npi(&row.name, &row.source_zip) {
                            effective_npi = npi;
                            fuzzy_npi_hit = 1;
                        }
                    }
                }
            }

            let mut endpoint_website_hit = 0usize;
            if website.is_empty() && !effective_npi.is_empty() {
                if let Some(endpoint_website) = endpoint_map.get(&effective_npi) {
                    website = endpoint_website.clone();
                    endpoint_website_hit = 1;
                }
            }

            let (cms_hpt_url, mrf_url, attempted_enrichment) = if !website.is_empty() {
                let (cms_hpt_url, mrf_url) = enrich_urls(client.as_ref(), &website, &existing_cms_hpt, &existing_mrf);
                (cms_hpt_url, mrf_url, 1usize)
            } else {
                (existing_cms_hpt, existing_mrf, 0usize)
            };

            DiscoveryRow {
                ccn: row.ccn.clone(),
                name: row.name.clone(),
                state: row.state.clone(),
                city: row.city.clone(),
                website,
                cms_hpt_url: cms_hpt_url.clone(),
                mrf_url: mrf_url.clone(),
                attempted_enrichment,
                found_cms_hpt: usize::from(!cms_hpt_url.is_empty()),
                found_mrf: usize::from(!mrf_url.is_empty()),
                endpoint_website_hit,
                fuzzy_npi_hit,
            }
        })
        .collect();

    let count = discovered_rows.len();
    let attempted_enrichment = discovered_rows.iter().map(|row| row.attempted_enrichment).sum::<usize>();
    let found_cms_hpt = discovered_rows.iter().map(|row| row.found_cms_hpt).sum::<usize>();
    let found_mrf = discovered_rows.iter().map(|row| row.found_mrf).sum::<usize>();
    let endpoint_website_hits = discovered_rows.iter().map(|row| row.endpoint_website_hit).sum::<usize>();
    let fuzzy_npi_hits = discovered_rows.iter().map(|row| row.fuzzy_npi_hit).sum::<usize>();

    for row in discovered_rows {
        conn.execute(
            "INSERT INTO hospitals (ccn, name, state, city, website, cms_hpt_url, mrf_url, last_audited)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(ccn) DO UPDATE SET
                 name = excluded.name,
                 state = excluded.state,
                 city = excluded.city,
                 website = CASE
                     WHEN excluded.website IS NOT NULL AND excluded.website <> '' THEN excluded.website
                     ELSE hospitals.website
                 END,
                 cms_hpt_url = CASE
                     WHEN excluded.cms_hpt_url IS NOT NULL AND excluded.cms_hpt_url <> '' THEN excluded.cms_hpt_url
                     ELSE hospitals.cms_hpt_url
                 END,
                 mrf_url = CASE
                     WHEN excluded.mrf_url IS NOT NULL AND excluded.mrf_url <> '' THEN excluded.mrf_url
                     ELSE hospitals.mrf_url
                 END,
                 last_audited = CASE
                     WHEN excluded.last_audited IS NOT NULL AND excluded.last_audited <> '' THEN excluded.last_audited
                     ELSE hospitals.last_audited
                 END",
            rusqlite::params![row.ccn, row.name, row.state, row.city, row.website, row.cms_hpt_url, row.mrf_url, ""],
        )?;
    }

    info!(
        "Completed discovery: {} hospitals, enrichment_attempts={}, fuzzy_npi_hits={}, endpoint_website_hits={}, cms_hpt_found={}, mrf_found={}",
        count,
        attempted_enrichment,
        fuzzy_npi_hits,
        endpoint_website_hits,
        found_cms_hpt,
        found_mrf
    );
    Ok(())
}
