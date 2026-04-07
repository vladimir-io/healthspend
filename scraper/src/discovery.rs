use crate::schema::init_db;
use csv::ReaderBuilder;
use std::fs::File;
use std::io::BufReader;
use tracing::info;

/// state_filter: a comma-separated list of state abbreviations, or None for all states.
/// When passed from --shard-states, this lets a single runner process TX,OK,AR in one job.

pub fn run_discovery(state_filter: Option<String>) -> anyhow::Result<()> {
    info!("Starting hospital discovery phase...");
    let mut conn = init_db()?;

    // Support comma-separated state lists (from --shard-states)
    let state_set: Option<Vec<String>> = state_filter.map(|s| {
        s.split(',').map(|st| st.trim().to_uppercase()).collect()
    });

    let file = File::open("data/pos.csv")?;
    let mut rdr = ReaderBuilder::new().from_reader(BufReader::new(file));

    let mut count = 0;

    // Begin transaction for speed
    let tx = conn.transaction()?;

    for result in rdr.records() {
        let record = result?;
        // Columns per mock schema: PRVDR_CTGRY_SBTYP_CD, PRVDR_CTGRY_CD, CHOW_CNT, CHOW_DT, CITY_NAME, GNL_CNTL_TYPE_CD, ZIP_CD, FCLTY_NAME, PRVDR_NUM, STATE_CD, CBSA_URBN_RRL_IND
        let city = &record[4];
        let name = &record[7];
        let ccn = &record[8];
        let state = &record[9];

        if let Some(ref states) = state_set {
            if !states.contains(&state.to_uppercase()) {
                continue;
            }
        }

        // In production, we extract the official hospital website from the CMS record.
        // If missing, the audit phase will attempt to resolve it via the hospital's public transparency portal.
        let website = "".to_string(); 
        let cms_hpt_url = "".to_string(); 

        tx.execute(
            "INSERT OR REPLACE INTO hospitals (ccn, name, state, city, website, cms_hpt_url, mrf_url, last_audited) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![ccn, name, state, city, website, cms_hpt_url, "", ""],
        )?;

        count += 1;
        if count % 100 == 0 {
            info!("Discovered {} hospitals in state filters...", count);
        }
    }
    tx.commit()?;

    info!("Completed discovery: Total {} hospitals added.", count);
    Ok(())
}
