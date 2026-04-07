#[path = "../error_logger.rs"]
mod error_logger;
#[path = "../fact_writer.rs"]
mod fact_writer;
#[path = "../npi/mod.rs"]
mod npi;
#[path = "../parser_csv.rs"]
mod parser_csv;
#[path = "../parser_json.rs"]
mod parser_json;
#[path = "../shoppable.rs"]
mod shoppable;

use clap::Parser;
use rusqlite::Connection;
use std::path::Path;

#[derive(Parser, Debug)]
#[command(author, version, about = "Ingest a single MRF file into SQLite", long_about = None)]
struct Args {
    #[arg(long)]
    file: String,

    #[arg(long)]
    ccn: String,

    #[arg(long, default_value = "prices.db")]
    prices_db: String,

    #[arg(long, default_value = "compliance.db")]
    compliance_db: String,
}

fn update_parse_result(
    compliance_db_path: &str,
    ccn: &str,
    mrf_machine_readable: bool,
    records_found: usize,
) -> anyhow::Result<()> {
    let conn = Connection::open(compliance_db_path)?;

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
            evidence_json = COALESCE(evidence_json, '') || ?4
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !Path::new(&args.file).exists() {
        anyhow::bail!("File not found: {}", args.file);
    }

    let ext = Path::new(&args.file)
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    let (records_inserted, mrf_machine_readable) = match ext.as_str() {
        "json" => {
            let result = parser_json::parse_json_streaming_with_dbs(
                &args.file,
                &args.ccn,
                &args.prices_db,
                &args.compliance_db,
            );
            (result.records_inserted, result.mrf_machine_readable)
        }
        "csv" | "txt" => {
            let result = parser_csv::parse_csv_tall_with_dbs(
                &args.file,
                &args.ccn,
                &args.prices_db,
                &args.compliance_db,
            );
            (result.records_inserted, result.mrf_machine_readable)
        }
        _ => {
            anyhow::bail!(
                "Unsupported MRF extension for {}. Supported: .json, .csv, .txt",
                args.file
            );
        }
    };

    update_parse_result(
        &args.compliance_db,
        &args.ccn,
        mrf_machine_readable,
        records_inserted,
    )?;

    println!(
        "ingested ccn={} file={} records_inserted={} machine_readable={}",
        args.ccn,
        args.file,
        records_inserted,
        mrf_machine_readable
    );

    Ok(())
}
