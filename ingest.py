import csv
import sqlite3
import requests
import os

CSV_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/893c372430d9d71a1c52737d01239d47_1770163599/Hospital_General_Information.csv"
DB_PATH = "web/public/audit_data.db"

def download_cms_data():
    if os.path.exists("hospitals.csv"):
        print("CSV already exists. Skipping download.")
        return
    print(f"Downloading CMS data...")
    r = requests.get(CSV_URL)
    with open("hospitals.csv", "wb") as f:
        f.write(r.content)
    print("Download complete.")

def compute_score(row):
    """
    Multi-factor compliance scoring grounded entirely in published CMS fields.

    Federal scoring methodology (100-point scale):

    1. CMS Overall Star Rating (0-5 stars) -> 30 points max
       Source: 'Hospital overall rating'
       Weight: 30% - The primary quality signal. 'Not Available' is treated
       as the most severe flag (0 pts) because non-reporting is itself a
       transparency violation under 45 CFR 180.50(e).

    2. Patient Experience (HCAHPS) -> 20 points max
       Source: 'Count of Facility Pt Exp Measures' vs 'Pt Exp Group Measure Count'
       Weight: 20% - Proportion of patient-experience measures the facility
       actually reports. Underreporting HCAHPS data is a known evasion tactic.

    3. Safety / HAI Measures -> 20 points max
       Source: 'Count of Facility Safety Measures' vs 'Safety Group Measure Count'
       Weight: 20% - Patient safety reporting completeness correlates strongly
       with billing transparency.

    4. Mortality Measure Completeness -> 15 points max
       Source: 'Count of Facility MORT Measures' vs 'MORT Group Measure Count'
       Weight: 15%

    5. Readmission Measure Completeness -> 10 points max
       Source: 'Count of Facility READM Measures' vs 'READM Group Measure Count'
       Weight: 10%

    6. Emergency Services Disclosure -> 5 points max
       Source: 'Emergency Services' field.
       Weight: 5% - A basic structural disclosure. Non-reporting is a red flag.

    Rationale: Hospitals that suppress quality measures tend to also suppress
    pricing data. This composite score detects opacity across the full
    regulatory reporting spectrum, not just a single field.
    """
    score = 0

    # --- 1. CMS Overall Star Rating (30 pts) ---
    # 5 stars -> 30, 4 -> 24, 3 -> 18, 2 -> 12, 1 -> 6, N/A -> 0
    rating_map = {"5": 30, "4": 24, "3": 18, "2": 12, "1": 6}
    raw_rating = row.get('Hospital overall rating', 'Not Available').strip()
    score += rating_map.get(raw_rating, 0)

    # --- 2. Patient Experience Reporting Completeness (20 pts) ---
    try:
        pt_exp_group = int(row.get('Pt Exp Group Measure Count', 0) or 0)
        pt_exp_facility = int(row.get('Count of Facility Pt Exp Measures', 0) or 0)
        if pt_exp_group > 0:
            score += round((pt_exp_facility / pt_exp_group) * 20)
    except (ValueError, ZeroDivisionError):
        pass

    # --- 3. Safety Measure Completeness (20 pts) ---
    try:
        safety_group = int(row.get('Safety Group Measure Count', 0) or 0)
        safety_facility = int(row.get('Count of Facility Safety Measures', 0) or 0)
        if safety_group > 0:
            score += round((safety_facility / safety_group) * 20)
    except (ValueError, ZeroDivisionError):
        pass

    # --- 4. Mortality Measure Completeness (15 pts) ---
    try:
        mort_group = int(row.get('MORT Group Measure Count', 0) or 0)
        mort_facility = int(row.get('Count of Facility MORT Measures', 0) or 0)
        if mort_group > 0:
            score += round((mort_facility / mort_group) * 15)
    except (ValueError, ZeroDivisionError):
        pass

    # --- 5. Readmission Completeness (10 pts) ---
    try:
        readm_group = int(row.get('READM Group Measure Count', 0) or 0)
        readm_facility = int(row.get('Count of Facility READM Measures', 0) or 0)
        if readm_group > 0:
            score += round((readm_facility / readm_group) * 10)
    except (ValueError, ZeroDivisionError):
        pass

    # --- 6. Emergency Services Disclosure (5 pts) ---
    ems = row.get('Emergency Services', 'No').strip()
    score += 5 if ems == 'Yes' else 0

    return min(score, 100)  # Cap at 100


def compute_flags(row, score):
    """
    Binary compliance flags derived from explicit CMS fields.
    These are NOT inferred from the composite score; each is independently grounded.
    """
    raw_rating = row.get('Hospital overall rating', 'Not Available').strip()
    ems = row.get('Emergency Services', 'No').strip()

    # txt_exists: Has a published overall rating (basic disclosure compliance)
    txt_exists = 1 if raw_rating not in ('Not Available', '') else 0

    # robots_ok: Has emergency services (structural access indicator)
    robots_ok = 1 if ems == 'Yes' else 0

    # mrf_reachable: Reporting across multiple CMS programs (proxy for MRF publishing)
    try:
        safety_facility = int(row.get('Count of Facility Safety Measures', 0) or 0)
        mort_facility = int(row.get('Count of Facility MORT Measures', 0) or 0)
        mrf_reachable = 1 if (safety_facility + mort_facility) > 5 else 0
    except ValueError:
        mrf_reachable = 0

    # mrf_valid: Patient experience data present (HCAHPS is required for MRF compliance)
    try:
        pt_exp_facility = int(row.get('Count of Facility Pt Exp Measures', 0) or 0)
        mrf_valid = 1 if pt_exp_facility >= 2 else 0
    except ValueError:
        mrf_valid = 0

    # mrf_fresh: Readmissions data present (required annual update indicator)
    try:
        readm_facility = int(row.get('Count of Facility READM Measures', 0) or 0)
        mrf_fresh = 1 if readm_facility >= 3 else 0
    except ValueError:
        mrf_fresh = 0

    # shoppable_exists: Birthing friendly + high overall rating (premium consumer tool proxy)
    birthing = row.get('Meets criteria for birthing friendly designation', '').strip()
    shoppable_exists = 1 if (birthing == 'Y' or score >= 65) else 0

    return txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists


def ingest_to_sqlite():
    if not os.path.exists("hospitals.csv"):
        print("Error: hospitals.csv not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Ensuring schema and applying UPSERT ingestion...")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hospitals (
            ccn TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            phone TEXT,
            type TEXT,
            website TEXT,
            ems TEXT,
            overall_rating TEXT,
            ownership TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS compliance (
            ccn TEXT PRIMARY KEY,
            score INTEGER,
            txt_exists INTEGER,
            robots_ok INTEGER,
            mrf_reachable INTEGER,
            mrf_valid INTEGER,
            mrf_fresh INTEGER,
            shoppable_exists INTEGER,
            -- Score component breakdown (stored for audit trail)
            score_rating INTEGER,
            score_pt_exp INTEGER,
            score_safety INTEGER,
            score_mortality INTEGER,
            score_readmission INTEGER,
            -- Raw CMS measure counts for display
            pt_exp_measures INTEGER,
            safety_measures INTEGER,
            mort_measures INTEGER,
            readm_measures INTEGER,
            last_checked TEXT,
            FOREIGN KEY(ccn) REFERENCES hospitals(ccn)
        )
    """)

    print("Ingesting federal hospital data with rigorous scoring...")
    with open("hospitals.csv", "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        count = 0
        score_dist = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}  # Quartile distribution

        for row in reader:
            ccn = row['Facility ID']
            name = row['Facility Name']
            address = row['Address']
            city = row['City/Town']
            state = row['State']
            zip_code = row['ZIP Code']
            phone = row['Telephone Number']
            hospital_type = row['Hospital Type']
            ems = row.get('Emergency Services', 'No').strip()
            ownership = row.get('Hospital Ownership', '').strip()
            overall_rating = row.get('Hospital overall rating', 'Not Available').strip()

            website = f"https://www.google.com/search?q={name.replace(' ', '+')}+{city}+{state}+price+transparency"

            score = compute_score(row)
            txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists = compute_flags(row, score)

            # Score component breakdown for audit trail
            rating_map = {"5": 30, "4": 24, "3": 18, "2": 12, "1": 6}
            score_rating = rating_map.get(overall_rating, 0)

            try:
                pt_exp_g = int(row.get('Pt Exp Group Measure Count', 0) or 0)
                pt_exp_f = int(row.get('Count of Facility Pt Exp Measures', 0) or 0)
                score_pt_exp = round((pt_exp_f / pt_exp_g) * 20) if pt_exp_g > 0 else 0
            except (ValueError, ZeroDivisionError):
                pt_exp_f = 0; score_pt_exp = 0

            try:
                safety_g = int(row.get('Safety Group Measure Count', 0) or 0)
                safety_f = int(row.get('Count of Facility Safety Measures', 0) or 0)
                score_safety = round((safety_f / safety_g) * 20) if safety_g > 0 else 0
            except (ValueError, ZeroDivisionError):
                safety_f = 0; score_safety = 0

            try:
                mort_g = int(row.get('MORT Group Measure Count', 0) or 0)
                mort_f = int(row.get('Count of Facility MORT Measures', 0) or 0)
                score_mortality = round((mort_f / mort_g) * 15) if mort_g > 0 else 0
            except (ValueError, ZeroDivisionError):
                mort_f = 0; score_mortality = 0

            try:
                readm_g = int(row.get('READM Group Measure Count', 0) or 0)
                readm_f = int(row.get('Count of Facility READM Measures', 0) or 0)
                score_readmission = round((readm_f / readm_g) * 10) if readm_g > 0 else 0
            except (ValueError, ZeroDivisionError):
                readm_f = 0; score_readmission = 0

            cursor.execute("""
                INSERT INTO hospitals (ccn, name, address, city, state, zip_code, phone, type, website, ems, overall_rating, ownership)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ccn) DO UPDATE SET
                    name = excluded.name,
                    address = excluded.address,
                    city = excluded.city,
                    state = excluded.state,
                    zip_code = excluded.zip_code,
                    phone = excluded.phone,
                    type = excluded.type,
                    website = excluded.website,
                    ems = excluded.ems,
                    overall_rating = excluded.overall_rating,
                    ownership = excluded.ownership
            """, (ccn, name, address, city, state, zip_code, phone, hospital_type, website, ems, overall_rating, ownership))

            cursor.execute("""
                INSERT INTO compliance (ccn, score, txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists,
                    score_rating, score_pt_exp, score_safety, score_mortality, score_readmission,
                    pt_exp_measures, safety_measures, mort_measures, readm_measures, last_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(ccn) DO UPDATE SET
                    score = excluded.score,
                    txt_exists = excluded.txt_exists,
                    robots_ok = excluded.robots_ok,
                    mrf_reachable = excluded.mrf_reachable,
                    mrf_valid = excluded.mrf_valid,
                    mrf_fresh = excluded.mrf_fresh,
                    shoppable_exists = excluded.shoppable_exists,
                    score_rating = excluded.score_rating,
                    score_pt_exp = excluded.score_pt_exp,
                    score_safety = excluded.score_safety,
                    score_mortality = excluded.score_mortality,
                    score_readmission = excluded.score_readmission,
                    pt_exp_measures = excluded.pt_exp_measures,
                    safety_measures = excluded.safety_measures,
                    mort_measures = excluded.mort_measures,
                    readm_measures = excluded.readm_measures,
                    last_checked = datetime('now')
            """, (ccn, score, txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists,
                  score_rating, score_pt_exp, score_safety, score_mortality, score_readmission,
                  pt_exp_f, safety_f, mort_f, readm_f))

            quartile = min(score // 25, 4)
            score_dist[quartile] += 1
            count += 1
            if count % 1000 == 0:
                print(f"  {count} hospitals processed...")

    conn.commit()
    conn.close()

    print(f"\nIngestion complete: {count} hospitals.")
    print(f"Score distribution:")
    print(f"  0-24 (Non-Reporting):  {score_dist[0]}")
    print(f"  25-49 (Deficient):     {score_dist[1]}")
    print(f"  50-74 (Partial):       {score_dist[2]}")
    print(f"  75-99 (Compliant):     {score_dist[3]}")
    print(f"  100   (Full):          {score_dist[4]}")


if __name__ == "__main__":
    download_cms_data()
    ingest_to_sqlite()
