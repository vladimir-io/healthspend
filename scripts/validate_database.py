#!/usr/bin/env python3
"""
Database validation script for Healthspend data pipeline.
Checks database integrity before deployment.
"""
import sqlite3
import sys
from pathlib import Path

def validate_database(db_path: str) -> bool:
    """Check database integrity before deployment."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        checks = []
        
        # 1. Table exists check
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        if 'prices' not in tables:
            checks.append(("FAIL", "prices table missing"))
        else:
            checks.append(("PASS", "prices table exists"))
        
        # 2. Row count check
        try:
            cursor.execute("SELECT COUNT(*) FROM prices")
            row_count = cursor.fetchone()[0]
            if row_count < 100000:
                checks.append(("WARN", f"Only {row_count:,} price records (expected >100k)"))
            else:
                checks.append(("PASS", f"{row_count:,} price records"))
        except sqlite3.OperationalError:
            checks.append(("WARN", "Could not query prices table"))
        
        # 3. Null check
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM prices WHERE ccn IS NULL OR cpt_code IS NULL OR cash_price IS NULL"
            )
            nulls = cursor.fetchone()[0]
            if nulls > 0:
                checks.append(("WARN", f"{nulls:,} records with NULL critical fields"))
            else:
                checks.append(("PASS", "No NULL values in critical fields"))
        except sqlite3.OperationalError:
            checks.append(("WARN", "Could not check for NULLs"))
        
        # 4. Hospital coverage
        try:
            cursor.execute("SELECT COUNT(DISTINCT ccn) FROM prices")
            hospitals_with_prices = cursor.fetchone()[0]
            if hospitals_with_prices < 1000:
                checks.append(("WARN", f"Only {hospitals_with_prices:,} hospitals with prices (expected >1k)"))
            else:
                checks.append(("PASS", f"{hospitals_with_prices:,} unique hospitals with prices"))
        except sqlite3.OperationalError:
            checks.append(("WARN", "Could not count unique hospitals"))
        
        # 5. Parse error check
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM parse_errors WHERE resolved = 0"
            )
            unresolved_errors = cursor.fetchone()[0]
            if unresolved_errors > 0:
                checks.append(("WARN", f"{unresolved_errors:,} unresolved parse errors"))
            else:
                checks.append(("PASS", "No unresolved parse errors"))
        except sqlite3.OperationalError:
            checks.append(("SKIP", "parse_errors table not present"))
        
        conn.close()
        
        # Report
        print(f"\n{'Database Validation Results':^60}")
        print("=" * 60)
        
        failed = False
        for status, message in checks:
            if status == "PASS":
                symbol = "✓"
            elif status == "WARN":
                symbol = "!"
            elif status == "FAIL":
                symbol = "✗"
            else:
                symbol = "-"
            
            print(f"{symbol} [{status:4}] {message}")
            if status == "FAIL":
                failed = True
        
        print("=" * 60)
        return not failed
        
    except Exception as e:
        print(f"✗ [ERROR] Failed to validate database: {e}")
        return False

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "prices.db"
    
    if not Path(db_path).exists():
        print(f"✗ [FAIL] Database file not found: {db_path}")
        sys.exit(1)
    
    success = validate_database(db_path)
    sys.exit(0 if success else 1)
