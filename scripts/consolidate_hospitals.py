#!/usr/bin/env python3
"""
Hospital Data Consolidation Script
Merges data from fragmented sources into unified hospital_master table
"""
import sqlite3
from datetime import datetime
import sys
from pathlib import Path
from typing import Dict, List, Tuple

def consolidate_hospitals(
    source_dbs: List[str],
    target_db: str,
    cms_csv: str = None
) -> bool:
    """
    Consolidate hospital records from multiple sources into unified schema.
    
    Args:
        source_dbs: List of database paths to consolidate from
        target_db: Target database path
        cms_csv: Optional CMS data CSV file
    """
    try:
        target_conn = sqlite3.connect(target_db)
        target_conn.row_factory = sqlite3.Row
        
        print(f"\n{'Hospital Data Consolidation':^60}")
        print("=" * 60)
        
        # Enable foreign keys
        target_conn.execute("PRAGMA foreign_keys = ON")
        
        # Track consolidation statistics
        stats = {
            'total_read': 0,
            'merged': 0,
            'new': 0,
            'conflicts': 0,
            'updated': 0,
        }
        
        # Step 1: Read from source databases
        print("\n[1/3] Reading from source databases...")
        
        hospital_records: Dict[str, dict] = {}
        
        for source_db in source_dbs:
            if not Path(source_db).exists():
                print(f"⚠ Source database not found: {source_db}, skipping")
                continue
            
            try:
                conn = sqlite3.connect(source_db)
                cursor = conn.cursor()
                
                # Try to read from both old and new schema
                try:
                    cursor.execute("SELECT ccn, name, state, city, website FROM hospitals")
                    for row in cursor.fetchall():
                        ccn = row[0]
                        if ccn not in hospital_records:
                            hospital_records[ccn] = {
                                'ccn': ccn,
                                'name': row[1],
                                'state': row[2],
                                'city': row[3],
                                'website': row[4],
                                'sources': [source_db]
                            }
                            stats['new'] += 1
                        else:
                            stats['merged'] += 1
                        stats['total_read'] += 1
                except sqlite3.OperationalError:
                    print(f"⚠ Could not read hospitals table from {source_db}")
                
                conn.close()
            except Exception as e:
                print(f"✗ Error reading {source_db}: {e}")
        
        print(f"✓ Read {stats['total_read']:,} records from source databases")
        print(f"  • New records: {stats['new']:,}")
        print(f"  • Merged: {stats['merged']:,}")
        
        # Step 2: Load CMS data if provided
        print("\n[2/3] Loading CMS authoritative data...")
        if cms_csv and Path(cms_csv).exists():
            try:
                import csv
                with open(cms_csv, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ccn = row.get('Facility CCN') or row.get('ccn')
                        if not ccn:
                            continue
                        
                        if ccn in hospital_records:
                            # Merge/update with CMS data
                            hospital_records[ccn]['cms_name'] = row.get('Facility Name')
                            hospital_records[ccn]['cms_state'] = row.get('State')
                            hospital_records[ccn]['cms_city'] = row.get('City')
                            hospital_records[ccn]['address'] = row.get('Address')
                            hospital_records[ccn]['phone'] = row.get('Phone Number')
                            stats['updated'] += 1
                        else:
                            # New from CMS
                            hospital_records[ccn] = {
                                'ccn': ccn,
                                'name': row.get('Facility Name'),
                                'state': row.get('State'),
                                'city': row.get('City'),
                                'address': row.get('Address'),
                                'phone': row.get('Phone Number'),
                                'sources': ['cms']
                            }
                            stats['new'] += 1
                
                print(f"✓ Loaded CMS data, updated {stats['updated']:,} records")
            except Exception as e:
                print(f"⚠ Could not load CMS data: {e}")
        
        # Step 3: Insert into unified table
        print("\n[3/3] Inserting into unified hospital_master table...")
        
        inserted = 0
        for ccn, record in hospital_records.items():
            try:
                target_conn.execute("""
                    INSERT OR REPLACE INTO hospital_master 
                    (ccn, name, state, city, address, phone, website, data_source, effective_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'consolidation', ?)
                """, (
                    ccn,
                    record.get('name') or record.get('cms_name') or 'Unknown',
                    record.get('state') or record.get('cms_state'),
                    record.get('city') or record.get('cms_city'),
                    record.get('address'),
                    record.get('phone'),
                    record.get('website'),
                    datetime.now().isoformat(),
                ))
                inserted += 1
                
                if inserted % 100 == 0:
                    target_conn.commit()
            except Exception as e:
                print(f"✗ Error inserting {ccn}: {e}")
                stats['conflicts'] += 1
        
        target_conn.commit()
        
        print(f"✓ Inserted {inserted:,} records into hospital_master")
        print("\n" + "=" * 60)
        print("Consolidation Summary")
        print("=" * 60)
        print(f"Total records processed: {stats['total_read']:,}")
        print(f"New records created: {stats['new']:,}")
        print(f"Records merged: {stats['merged']:,}")
        print(f"Records updated: {stats['updated']:,}")
        print(f"Conflicts: {stats['conflicts']:,}")
        
        # Final statistics
        cursor = target_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hospital_master")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT state) FROM hospital_master")
        states = cursor.fetchone()[0]
        
        print(f"\nFinal state:")
        print(f"  • Total hospitals: {total:,}")
        print(f"  • States: {states}")
        
        target_conn.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Consolidation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    target_db = "scraper/compliance.db"
    source_dbs = [
        "scraper/compliance.db",
        "web/public/audit_data.db",
    ]
    cms_csv = "hospitals.csv"
    
    # Check that target database exists (migrations should have run first)
    if not Path(target_db).exists():
        print(f"✗ Target database not found: {target_db}")
        print("Run migrate_schema.py first")
        sys.exit(1)
    
    success = consolidate_hospitals(source_dbs, target_db, cms_csv)
    sys.exit(0 if success else 1)
