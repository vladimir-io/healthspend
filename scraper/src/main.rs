pub mod auditor;
pub mod discovery;
pub mod error_logger;
pub mod parser_csv;
pub mod parser_json;
pub mod schema;
pub mod shoppable;

use clap::Parser;
use tracing::Level;

#[derive(Parser, Debug)]
#[command(author, version, about = "healthspend.lol Data Smuggler", long_about = None)]
struct Args {
    #[arg(long, help = "Run discovery phase only")]
    discover_only: bool,

    #[arg(long, help = "Run audit phase only")]
    audit_only: bool,

    #[arg(long, help = "Run parsing phase only on local test files")]
    parse_only: bool,

    /// Single state filter (e.g. --state TX). Used in non-matrix runs.
    #[arg(long)]
    state: Option<String>,

    /// Comma-separated state list for matrix sharding (e.g. --shard-states TX,OK,AR).
    /// When set, overrides --state and runs all listed states.
    #[arg(long)]
    shard_states: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = Args::parse();
    tracing::info!("healthspend.lol — Data Smuggler v1.0 initialized.");

    // Resolve state filter: shard-states takes priority over state
    let state_filter: Option<String> = args.shard_states.or(args.state);

    let mut ran_something = false;

    if args.discover_only {
        discovery::run_discovery(state_filter.clone())?;
        ran_something = true;
    }

    if args.audit_only {
        auditor::run_auditor(state_filter.clone()).await?;
        ran_something = true;
    }

    if args.parse_only {
        // Test files — these get swapped for real MRF paths in the matrix job
        let csv_result = parser_csv::parse_csv_tall("data/test_mrf_tall.csv", "450056");
        auditor::update_parse_result("450056", csv_result.mrf_machine_readable, csv_result.records_inserted)?;

        let json_result = parser_json::parse_json_streaming("data/test_mrf.json", "450358");
        auditor::update_parse_result("450358", json_result.mrf_machine_readable, json_result.records_inserted)?;

        ran_something = true;
    }

    if !ran_something {
        // Full pipeline (no flags)
        discovery::run_discovery(state_filter.clone())?;
        auditor::run_auditor(state_filter.clone()).await?;
    }

    Ok(())
}
