use chrono::Utc;
use rusqlite::Connection;
use tracing::warn;

/// Represents a parse error that needs to be logged
#[derive(Debug, Clone)]
pub struct ParseError {
    pub ccn: String,
    pub file_path: String,
    pub error_type: String,
    pub error_detail: String,
    pub file_size_bytes: u64,
}

impl ParseError {
    pub fn new(
        ccn: String,
        file_path: String,
        error_type: String,
        error_detail: String,
        file_size_bytes: u64,
    ) -> Self {
        Self {
            ccn,
            file_path,
            error_type,
            error_detail,
            file_size_bytes,
        }
    }
}

/// Log a parse error to the database for tracking and alerts
pub fn log_parse_error_to_database(error: ParseError, conn: &Connection) -> anyhow::Result<()> {
    let timestamp = Utc::now().to_rfc3339();
    
    conn.execute(
        "INSERT INTO parse_errors 
         (ccn, file_path, error_type, error_detail, file_size_bytes, timestamp, resolved)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
        rusqlite::params![
            error.ccn,
            error.file_path,
            error.error_type,
            error.error_detail,
            error.file_size_bytes,
            timestamp,
        ],
    )?;

    warn!(
        "Parse error logged: CCN={}, Type={}, File={}, Size={}bytes",
        error.ccn, error.error_type, error.file_path, error.file_size_bytes
    );

    Ok(())
}

/// Get parse error statistics for alerting
pub fn get_parse_error_stats(
    conn: &Connection,
    hours: i32,
) -> anyhow::Result<(usize, f64)> {
    // Count unresolved errors in the last N hours
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM parse_errors 
         WHERE resolved = 0 AND timestamp > datetime('now', '-' || ?1 || ' hours')"
    )?;
    let error_count: usize = stmt.query_row(rusqlite::params![hours], |row| row.get(0))?;

    // Calculate error rate by comparing to total hospitals
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM hospitals")?;
    let total_hospitals: usize = stmt.query_row([], |row| row.get(0))?;

    let error_rate = if total_hospitals > 0 {
        (error_count as f64) / (total_hospitals as f64)
    } else {
        0.0
    };

    Ok((error_count, error_rate))
}
