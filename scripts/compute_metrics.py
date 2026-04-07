#!/usr/bin/env python3
"""
Healthspend Data Quality Metrics Computation
Generates aggregated metrics from pipeline runs for dashboarding and alerting
"""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys

def compute_metrics(prices_db: str, compliance_db: str, output_db: str) -> bool:
    """Compute data quality metrics across the platform."""
    
    try:
        prices_conn = sqlite3.connect(prices_db)
        compliance_conn = sqlite3.connect(compliance_db)
        output_conn = sqlite3.connect(output_db)
        
        # Enable foreign keys
        compliance_conn.execute("PRAGMA foreign_keys = ON")
        output_conn.execute("PRAGMA foreign_keys = ON")
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        print(f"\n{'Computing Metrics':^60}")
        print("=" * 60)
        
        # Create metrics table if it doesn't exist
        output_conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                state TEXT,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                UNIQUE(date, state, metric_name),
                FOREIGN KEY(date) REFERENCES runs(date)
            )
        """)
        
        output_conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT DEFAULT 'success'
            )
        """)
        
        # Record this run
        output_conn.execute(
            "INSERT OR REPLACE INTO runs (date, timestamp, status) VALUES (?, ?, ?)",
            (today, datetime.now().isoformat(), 'computing_metrics')
        )
        
        # 1. Parse success rate by state
        print("\n[1/6] Computing parse success rates...")
        try:
            compliance_cursor = compliance_conn.cursor()
            compliance_cursor.execute("""
                SELECT 
                    h.state,
                    SUM(CASE WHEN c.mrf_machine_readable = 1 THEN 1 ELSE 0 END) as successful,
                    COUNT(*) as total
                FROM hospitals h
                LEFT JOIN compliance c ON h.ccn = c.ccn
                WHERE h.state IS NOT NULL AND h.state != ''
                GROUP BY h.state
            """)
            
            for state, successful, total in compliance_cursor.fetchall():
                if total > 0:
                    rate = (successful / total * 100) if total > 0 else 0
                else:
                    rate = 0
                    
                output_conn.execute(
                    "INSERT OR REPLACE INTO metrics (date, state, metric_name, value, unit) VALUES (?, ?, ?, ?, ?)",
                    (today, state, 'parse_success_rate_pct', rate, 'percent')
                )
            print("✓ Parse success rates computed")
        except Exception as e:
            print(f"! Parse success rate computation failed: {e}")
        
        # 2. Hospitals with price data by state
        print("[2/6] Computing hospital coverage...")
        try:
            prices_cursor = prices_conn.cursor()
            prices_cursor.execute("""
                SELECT h.state, COUNT(DISTINCT p.ccn) as hospital_count
                FROM prices p
                LEFT JOIN hospitals h ON p.ccn = h.ccn
                WHERE h.state IS NOT NULL AND h.state != ''
                GROUP BY h.state
            """)
            
            for state, count in prices_cursor.fetchall():
                output_conn.execute(
                    "INSERT OR REPLACE INTO metrics (date, state, metric_name, value, unit) VALUES (?, ?, ?, ?, ?)",
                    (today, state, 'hospitals_with_prices', count, 'count')
                )
            print("✓ Hospital coverage computed")
        except Exception as e:
            print(f"! Hospital coverage computation failed: {e}")
        
        # 3. Average prices per hospital by state
        print("[3/6] Computing average prices per hospital...")
        try:
            prices_cursor = prices_conn.cursor()
            prices_cursor.execute("""
                SELECT h.state, AVG(price_count) as avg_prices
                FROM (
                    SELECT h.state, p.ccn, COUNT(*) as price_count
                    FROM prices p
                    LEFT JOIN hospitals h ON p.ccn = h.ccn
                    WHERE h.state IS NOT NULL AND h.state != ''
                    GROUP BY p.ccn
                ) subq
                GROUP BY h.state
            """)
            
            for state, avg_records in prices_cursor.fetchall():
                output_conn.execute(
                    "INSERT OR REPLACE INTO metrics (date, state, metric_name, value, unit) VALUES (?, ?, ?, ?, ?)",
                    (today, state, 'avg_prices_per_hospital', avg_records, 'count')
                )
            print("✓ Average prices per hospital computed")
        except Exception as e:
            print(f"! Average prices computation failed: {e}")
        
        # 4. WAF block rate by state
        print("[4/6] Computing WAF block rates...")
        try:
            compliance_cursor = compliance_conn.cursor()
            compliance_cursor.execute("""
                SELECT h.state,
                    SUM(CASE WHEN c.waf_blocked = 1 THEN 1 ELSE 0 END) as blocked,
                    COUNT(*) as total
                FROM compliance c
                LEFT JOIN hospitals h ON c.ccn = h.ccn
                WHERE h.state IS NOT NULL AND h.state != ''
                GROUP BY h.state
            """)
            
            for state, blocked, total in compliance_cursor.fetchall():
                if total > 0:
                    rate = (blocked / total * 100)
                else:
                    rate = 0
                    
                output_conn.execute(
                    "INSERT OR REPLACE INTO metrics (date, state, metric_name, value, unit) VALUES (?, ?, ?, ?, ?)",
                    (today, state, 'waf_block_rate_pct', rate, 'percent')
                )
            print("✓ WAF block rates computed")
        except Exception as e:
            print(f"! WAF block rate computation failed: {e}")
        
        # 5. Parse error statistics
        print("[5/6] Computing parse error statistics...")
        try:
            compliance_cursor = compliance_conn.cursor()
            
            # Unresolved errors by state
            compliance_cursor.execute("""
                SELECT state, COUNT(*) as error_count
                FROM parse_errors
                WHERE resolved = 0
                GROUP BY state
            """)
            
            for state, error_count in compliance_cursor.fetchall() or []:
                output_conn.execute(
                    "INSERT OR REPLACE INTO metrics (date, state, metric_name, value, unit) VALUES (?, ?, ?, ?, ?)",
                    (today, state, 'unresolved_parse_errors', error_count, 'count')
                )
            
            # Overall error rate
            compliance_cursor.execute("SELECT COUNT(DISTINCT state) FROM hospitals")
            total_states = compliance_cursor.fetchone()[0] or 1
            
            compliance_cursor.execute("SELECT COUNT(*) FROM parse_errors WHERE resolved = 0")
            total_unresolved = compliance_cursor.fetchone()[0] or 0
            
            print("✓ Parse error statistics computed")
        except Exception as e:
            print(f"! Parse error statistics computation failed: {e}")
        
        # 6. Compliance score distribution
        print("[6/6] Computing compliance statistics...")
        try:
            compliance_cursor = compliance_conn.cursor()
            
            # Average compliance score by state
            compliance_cursor.execute("""
                SELECT h.state, AVG(COALESCE(c.score, 30)) as avg_score
                FROM hospitals h
                LEFT JOIN compliance c ON h.ccn = c.ccn
                WHERE h.state IS NOT NULL AND h.state != ''
                GROUP BY h.state
            """)
            
            for state, avg_score in compliance_cursor.fetchall():
                output_conn.execute(
                    "INSERT OR REPLACE INTO metrics (date, state, metric_name, value, unit) VALUES (?, ?, ?, ?, ?)",
                    (today, state, 'avg_compliance_score', avg_score, 'points')
                )
            print("✓ Compliance statistics computed")
        except Exception as e:
            print(f"! Compliance statistics computation failed: {e}")
        
        # Commit all changes
        output_conn.commit()
        output_conn.execute("UPDATE runs SET status = 'success' WHERE date = ?", (today,))
        output_conn.commit()
        
        # Generate summary report
        print("\n" + "=" * 60)
        print("Metrics Summary")
        print("=" * 60)
        
        output_cursor = output_conn.cursor()
        output_cursor.execute("SELECT COUNT(*) FROM metrics WHERE date = ?", (today,))
        total_metrics = output_cursor.fetchone()[0]
        print(f"✓ {total_metrics:,} metrics computed for {today}")
        
        # Get top 3 states by parse success
        output_cursor.execute("""
            SELECT state, value FROM metrics 
            WHERE date = ? AND metric_name = 'parse_success_rate_pct'
            ORDER BY value DESC LIMIT 3
        """, (today,))
        
        print("\nTop 3 States (Parse Success):")
        for state, rate in output_cursor.fetchall():
            print(f"  • {state}: {rate:.1f}%")
        
        # Get states with WAF issues
        output_cursor.execute("""
            SELECT state, value FROM metrics 
            WHERE date = ? AND metric_name = 'waf_block_rate_pct' AND value > 5
            ORDER BY value DESC LIMIT 5
        """, (today,))
        
        waf_issues = output_cursor.fetchall()
        if waf_issues:
            print("\nStates with WAF Blocking Issues (>5%):")
            for state, rate in waf_issues:
                print(f"  ⚠ {state}: {rate:.1f}% WAF blocks")
        
        output_conn.close()
        prices_conn.close()
        compliance_conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n✗ Metrics computation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    prices_db = sys.argv[1] if len(sys.argv) > 1 else "scraper/prices.db"
    compliance_db = sys.argv[2] if len(sys.argv) > 2 else "scraper/compliance.db"
    output_db = sys.argv[3] if len(sys.argv) > 3 else "web/public/metrics.db"
    
    # Check if input databases exist
    if not Path(prices_db).exists():
        print(f"✗ prices.db not found at {prices_db}")
        sys.exit(1)
    
    if not Path(compliance_db).exists():
        print(f"✗ compliance.db not found at {compliance_db}")
        sys.exit(1)
    
    success = compute_metrics(prices_db, compliance_db, output_db)
    sys.exit(0 if success else 1)
