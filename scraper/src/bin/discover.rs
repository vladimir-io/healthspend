#[path = "../schema.rs"]
mod schema;
#[path = "../discovery.rs"]
mod discovery;

use clap::Parser;
use tracing::Level;

#[derive(Parser, Debug)]
#[command(author, version, about = "Run hospital discovery only", long_about = None)]
struct Args {
    /// Optional single state or comma-separated list (e.g. TX or TX,OK,AR)
    #[arg(long)]
    state: Option<String>,

    /// Alternate explicit shard list (takes priority over --state)
    #[arg(long)]
    shard_states: Option<String>,

    /// Discovery seed CSV (CMS hospital file / PDC export)
    #[arg(long, default_value = "hospitals.csv")]
    seed_file: String,

    /// Optional NPPES Endpoint_Reference_File.csv for website enrichment by NPI
    #[arg(long)]
    nppes_endpoints_file: Option<String>,

    /// Optional CMS hospital enrollments crosswalk file for CCN/Facility ID to NPI joins
    #[arg(long)]
    ccn_npi_crosswalk_file: Option<String>,

    /// Optional NPPES core file (npidata_pfile_*.csv) for name+zip to NPI reconciliation
    #[arg(long)]
    nppes_core_file: Option<String>,

    /// Enable Name+ZIP fuzzy reconciliation against NPPES core when seed NPI is missing
    #[arg(long)]
    fuzzy_match_nppes: bool,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = Args::parse();
    let state_filter = args.shard_states.or(args.state);
    discovery::run_discovery_with_options(discovery::DiscoveryOptions {
        state_filter,
        seed_file: args.seed_file,
        nppes_endpoints_file: args.nppes_endpoints_file,
        ccn_npi_crosswalk_file: args.ccn_npi_crosswalk_file,
        nppes_core_file: args.nppes_core_file,
        fuzzy_match_nppes: args.fuzzy_match_nppes,
    })
}
