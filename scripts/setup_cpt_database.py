#!/usr/bin/env python3
"""
CPT Mapping Database Setup
Externalizes CPT code mappings to database for dynamic updates
"""
import sqlite3
import json
from datetime import datetime
import sys
from pathlib import Path

# Hand-curated semantic mappings
BASE_MAPPINGS = {
    'knee replacement': '27447',
    'hip replacement': '27130',
    'mri': '70551',
    'brain mri': '70551',
    'mri brain': '70551',
    'ct scan': '74177',
    'cat scan': '74177',
    'xray': '71045',
    'x-ray': '71045',
    'chest x-ray': '71045',
    'blood work': '80053',
    'metabolic panel': '80053',
    'cmp': '80053',
    'colonoscopy': '45378',
    'emergency': '99283',
    'er': '99283',
    'er visit': '99283',
    'emergency room': '99283',
    'childbirth': '59400',
    'birth': '59400',
    'labor': '59400',
    'stitches': '12001',
    'wound': '12001',
    'ultrasound': '76700',
    'labs': '80053',
    'lab work': '80053',
    'physical': '99213',
    'physical exam': '99213',
    'checkup': '99213',
    'teeth cleaning': '99000',
}

# Category to fallback CPT code mapping
CATEGORY_FALLBACK = {
    'Emergency': '99283',
    'Imaging': '70551',
    'Lab Work': '80053',
    'Surgery': '27447',
    'Maternity': '59400',
    'Cardiology': '99285',
    'Mental Health': '99213',
    'Physical Therapy': '12001',
    'Preventive': '99213',
    'Sleep': '70551',
    'Orthopedic': '27447',
    'General': '12001',
}

def setup_cpt_database(output_db: str) -> bool:
    """Create CPT mapping database for search optimization."""
    try:
        conn = sqlite3.connect(output_db)
        cursor = conn.cursor()
        
        print(f"\n{'CPT Mapping Database Setup':^60}")
        print("=" * 60)
        
        # Create CPT code catalog table
        print("\n[1/3] Creating CPT catalog table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cpt_catalog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL,
                audit_node INTEGER DEFAULT 0,
                is_shoppable INTEGER DEFAULT 1,
                added_date TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cpt_code ON cpt_catalog(code)
        """)
        conn.commit()
        print("✓ CPT catalog table created")
        
        # Create semantic mapping table
        print("[2/3] Creating semantic mapping table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cpt_semantic_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plain_text TEXT UNIQUE NOT NULL,
                cpt_code TEXT NOT NULL,
                category TEXT,
                confidence REAL DEFAULT 1.0,
                source TEXT DEFAULT 'manual',
                added_date TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(cpt_code) REFERENCES cpt_catalog(code)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_mapping_plain ON cpt_semantic_mappings(plain_text)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_mapping_cpt ON cpt_semantic_mappings(cpt_code)
        """)
        conn.commit()
        print("✓ Semantic mapping table created")
        
        # Insert base mappings
        print("[3/3] Inserting base semantic mappings...")
        for plain_text, cpt_code in BASE_MAPPINGS.items():
            try:
                # Get category for this code
                category = None
                for cat, fallback_code in CATEGORY_FALLBACK.items():
                    if fallback_code == cpt_code:
                        category = cat
                        break
                
                cursor.execute("""
                    INSERT OR REPLACE INTO cpt_semantic_mappings
                    (plain_text, cpt_code, category, confidence, source)
                    VALUES (?, ?, ?, 1.0, 'manual')
                """, (plain_text, cpt_code, category or 'General'))
            except sqlite3.IntegrityError:
                pass
        
        conn.commit()
        
        print(f"✓ Inserted {len(BASE_MAPPINGS)} semantic mappings")
        
        # Create search analytics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                search_query TEXT NOT NULL,
                cpt_code_matched TEXT,
                fallback_used INTEGER DEFAULT 0,
                results_count INTEGER,
                user_feedback TEXT
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_search_timestamp ON search_analytics(timestamp)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_search_fallback ON search_analytics(fallback_used)
        """)
        conn.commit()
        print("✓ Search analytics table created")
        
        # Create FTS5 virtual table for full-text search
        print("\nCreating FTS5 full-text search index...")
        try:
            cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS cpt_search USING fts5(
                    plain_text,
                    cpt_code,
                    category,
                    content=cpt_semantic_mappings,
                    content_rowid=id
                )
            """)
            
            # Populate FTS table
            cursor.execute("""
                INSERT INTO cpt_search (rowid, plain_text, cpt_code, category)
                SELECT id, plain_text, cpt_code, category FROM cpt_semantic_mappings
            """)
            conn.commit()
            print("✓ FTS5 index created")
        except Exception as e:
            print(f"⚠ FTS5 setup failed (may not be available): {e}")
        
        conn.commit()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✓ CPT Database Setup Complete")
        print("=" * 60)
        print(f"\nTables created:")
        print("  • cpt_catalog - CPT code definitions")
        print("  • cpt_semantic_mappings - Plain text to code mappings")
        print("  • cpt_search - FTS5 full-text search index")
        print("  • search_analytics - Search query tracking")
        print(f"\nInitial mappings: {len(BASE_MAPPINGS)}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ CPT database setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    output_db = sys.argv[1] if len(sys.argv) > 1 else "web/public/cpt_mappings.db"
    
    success = setup_cpt_database(output_db)
    sys.exit(0 if success else 1)
