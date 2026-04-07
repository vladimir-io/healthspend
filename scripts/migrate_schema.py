#!/usr/bin/env python3
"""
Healthspend Database Schema Migration
Consolidates fragmented hospital data and adds audit trail support
"""
import sqlite3
from datetime import datetime
import sys
from pathlib import Path

def migrate_database(db_path: str, backup: bool = True) -> bool:
    """
    Apply schema migrations to consolidate hospital data and add temporal tracking.
    """
    try:
        if backup:
            backup_path = f"{db_path}.backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            print(f"Creating backup: {backup_path}")
            import shutil
            shutil.copy2(db_path, backup_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"\n{'Database Schema Migration':^60}")
        print("=" * 60)
        
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Migration 1: Unified hospital master table
        print("\n[1/5] Creating unified hospital master table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hospital_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ccn TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                city TEXT,
                zip_code TEXT,
                address TEXT,
                phone TEXT,
                website TEXT,
                cms_hpt_url TEXT,
                mrf_url TEXT,
                hospital_type TEXT,
                ownership TEXT,
                effective_date TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                retired_date TEXT,
                data_source TEXT NOT NULL DEFAULT 'cms'
            )
        """)
        conn.commit()
        print("✓ hospital_master table created")
        
        # Migration 2: Price history table (temporal data)
        print("[2/5] Creating price history table for trends...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hospital_id INTEGER NOT NULL,
                ccn TEXT NOT NULL,
                cpt_code TEXT NOT NULL,
                description TEXT,
                gross_charge REAL,
                cash_price REAL,
                min_negotiated REAL,
                max_negotiated REAL,
                payer TEXT,
                plan TEXT,
                effective_date TEXT NOT NULL,
                retired_date TEXT,
                run_id INTEGER,
                FOREIGN KEY(hospital_id) REFERENCES hospital_master(id),
                UNIQUE(hospital_id, cpt_code, payer, plan, effective_date)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_price_history_ccn ON price_history(ccn)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(effective_date)
        """)
        conn.commit()
        print("✓ price_history table created")
        
        # Migration 3: Compliance history (audit trail)
        print("[3/5] Creating compliance audit trail...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS compliance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hospital_id INTEGER NOT NULL,
                ccn TEXT NOT NULL,
                compliance_date TEXT NOT NULL,
                score INTEGER,
                mrf_machine_readable INTEGER,
                waf_blocked INTEGER,
                txt_exists INTEGER,
                robots_ok INTEGER,
                mrf_reachable INTEGER,
                mrf_valid INTEGER,
                mrf_fresh INTEGER,
                shoppable_exists INTEGER,
                status TEXT,
                reason_code TEXT,
                evidence_json TEXT,
                run_id INTEGER,
                FOREIGN KEY(hospital_id) REFERENCES hospital_master(id),
                UNIQUE(hospital_id, compliance_date)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_compliance_history_date ON compliance_history(compliance_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_compliance_history_status ON compliance_history(status)
        """)
        conn.commit()
        print("✓ compliance_history table created")
        
        # Migration 4: Data quality audit log
        print("[4/5] Creating data quality tracking tables...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT NOT NULL,
                hospital_id INTEGER,
                ccn TEXT,
                severity TEXT,
                message TEXT,
                details_json TEXT,
                resolved INTEGER DEFAULT 0,
                resolution_notes TEXT
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quality_log_timestamp ON data_quality_log(timestamp)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quality_log_resolved ON data_quality_log(resolved)
        """)
        conn.commit()
        print("✓ data_quality_log table created")
        
        # Migration 5: Pipeline metadata
        print("[5/5] Creating pipeline metadata tables...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT UNIQUE NOT NULL,
                run_timestamp TEXT NOT NULL,
                phase TEXT,
                state_filter TEXT,
                status TEXT DEFAULT 'in_progress',
                hospitals_discovered INTEGER DEFAULT 0,
                hospitals_audited INTEGER DEFAULT 0,
                mrfs_parsed INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                duration_seconds INTEGER,
                notes TEXT
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON pipeline_runs(run_date)
        """)
        conn.commit()
        print("✓ pipeline_runs table created")
        
        # Create views for convenience
        print("\nCreating helper views...")
        
        # Current compliance view
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS current_compliance AS
            SELECT 
                hm.ccn,
                hm.name,
                hm.state,
                ch.score,
                ch.compliance_date,
                ch.status,
                ch.reason_code,
                ch.mrf_machine_readable,
                ch.waf_blocked
            FROM hospital_master hm
            LEFT JOIN compliance_history ch ON hm.id = ch.hospital_id
            WHERE ch.compliance_date = (
                SELECT MAX(compliance_date) FROM compliance_history 
                WHERE hospital_id = hm.id
            )
        """)
        print("✓ current_compliance view created")
        
        # Price trends view
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS price_trends AS
            SELECT 
                ccn,
                cpt_code,
                payer,
                plan,
                effective_date,
                cash_price,
                LAG(cash_price) OVER (
                    PARTITION BY ccn, cpt_code, payer, plan 
                    ORDER BY effective_date
                ) as previous_price,
                cash_price - LAG(cash_price) OVER (
                    PARTITION BY ccn, cpt_code, payer, plan 
                    ORDER BY effective_date
                ) as price_change
            FROM price_history
            WHERE retired_date IS NULL
        """)
        print("✓ price_trends view created")
        
        conn.commit()
        
        print("\n" + "=" * 60)
        print("✓ Migration complete!")
        print("=" * 60)
        print("\nNew tables created:")
        print("  • hospital_master - Unified hospital records")
        print("  • price_history - Temporal price tracking")
        print("  • compliance_history - Audit trail")
        print("  • data_quality_log - Quality events")
        print("  • pipeline_runs - Pipeline metadata")
        print("\nViews created:")
        print("  • current_compliance - Latest compliance status")
        print("  • price_trends - Price changes over time")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "scraper/compliance.db"
    
    if not Path(db_path).exists():
        print(f"✗ Database not found: {db_path}")
        sys.exit(1)
    
    success = migrate_database(db_path, backup=True)
    sys.exit(0 if success else 1)
