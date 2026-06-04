import csv
import sqlite3
import requests
import os

HOSPITALS_CSV_URL = (
    "https://data.cms.gov/provider-data/sites/default/files/resources/"
    "893c372430d9d71a1c52737d01239d47_1770163599/Hospital_General_Information.csv"
)
QUALITY_API_URL = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"
    "?limit=10000&offset=0&results=true&keys=true"
)
DB_PATH = "web/public/audit_data.db"


def download_hospitals_csv():
    if os.path.exists("hospitals.csv"):
        return
    print("Downloading CMS hospital list...")
    r = requests.get(HOSPITALS_CSV_URL, timeout=60)
    r.raise_for_status()
    with open("hospitals.csv", "wb") as f:
        f.write(r.content)
    print("Download complete.")


def fetch_quality_index() -> dict:
    """
    Fetches the CMS hospital quality dataset (xubh-q36u) using pagination.
    Returns a dict keyed by facility_id (CCN).
    """
    print("Fetching CMS quality index (xubh-q36u) via paginated API...")
    base_url = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"
    limit = 1000
    offset = 0
    full_index = {}

    while True:
        url = f"{base_url}?limit={limit}&offset={offset}&results=true"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        
        if not results:
            break
            
        for row in results:
            fid = row.get("facility_id")
            if fid:
                full_index[fid] = row
        
        print(f"  Loaded {len(full_index)} quality records...")
        if len(results) < limit:
            break
        offset += limit

    return full_index


def compute_score(base_row: dict, quality: dict) -> int:
    """
    Facility Audit Index — CMS Baseline (100-point scale).
    
    Hospitals can reach 100 pts based on federal quality reporting completeness.
    The MRF Active Audit (scraper) then adjusts this score in merge_pipeline_data.py.
    """
    score = 0

    raw_rating = base_row.get("Hospital overall rating", "Not Available").strip()
    score += {"5": 30, "4": 24, "3": 18, "2": 12, "1": 6}.get(raw_rating, 0)

    ems = base_row.get("Emergency Services", "No").strip()
    score += 5 if ems == "Yes" else 0

    if quality:
        def ratio_pts(group_key, facility_key, max_pts):
            try:
                g = int(quality.get(group_key) or 0)
                f = int(quality.get(facility_key) or 0)
                return round((f / g) * max_pts) if g > 0 else 0
            except (ValueError, ZeroDivisionError):
                return 0

        score += ratio_pts("pt_exp_group_measure_count",   "count_of_facility_pt_exp_measures", 20)
        score += ratio_pts("safety_group_measure_count",   "count_of_facility_safety_measures",  20)
        score += ratio_pts("mort_group_measure_count",     "count_of_facility_mort_measures",    15)
        score += ratio_pts("readm_group_measure_count",    "count_of_facility_readm_measures",   10)

    return min(score, 100)


def compute_score_breakdown(base_row: dict, quality: dict) -> dict:
    raw_rating = base_row.get("Hospital overall rating", "Not Available").strip()
    score_rating = {"5": 30, "4": 24, "3": 18, "2": 12, "1": 6}.get(raw_rating, 0)

    def ratio_pts(group_key, facility_key, max_pts):
        try:
            g = int((quality or {}).get(group_key) or 0)
            f = int((quality or {}).get(facility_key) or 0)
            p = round((f / g) * max_pts) if g > 0 else 0
            return p, f
        except (ValueError, ZeroDivisionError):
            return 0, 0

    s_pt, f_pt = ratio_pts("pt_exp_group_measure_count", "count_of_facility_pt_exp_measures", 20)
    s_sa, f_sa = ratio_pts("safety_group_measure_count", "count_of_facility_safety_measures",  20)
    s_mo, f_mo = ratio_pts("mort_group_measure_count",   "count_of_facility_mort_measures",    15)
    s_re, f_re = ratio_pts("readm_group_measure_count",  "count_of_facility_readm_measures",   10)

    return dict(
        score_rating=score_rating,
        score_pt_exp=s_pt,
        score_safety=s_sa,
        score_mortality=s_mo,
        score_readmission=s_re,
        pt_exp_measures=f_pt,
        safety_measures=f_sa,
        mort_measures=f_mo,
        readm_measures=f_re,
    )


def compute_flags(base_row: dict, quality: dict, score: int) -> tuple:
    raw_rating = base_row.get("Hospital overall rating", "Not Available").strip()
    ems        = base_row.get("Emergency Services", "No").strip()
    birthing   = base_row.get("Meets criteria for birthing friendly designation", "").strip()

    txt_exists       = 1 if raw_rating not in ("Not Available", "") else 0
    robots_ok        = 1 if ems == "Yes" else 0
    mrf_reachable    = 0  # populated by active MRF audit in merge_pipeline_data.py
    mrf_valid        = 0
    mrf_fresh        = 0
    shoppable_exists = 1 if birthing == "Y" else 0

    return txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists


def ingest_to_sqlite(quality_index: dict):
    if not os.path.exists("hospitals.csv"):
        print("Error: hospitals.csv not found.")
        return

    conn   = sqlite3.connect(DB_PATH)
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
            score_rating INTEGER,
            score_pt_exp INTEGER,
            score_safety INTEGER,
            score_mortality INTEGER,
            score_readmission INTEGER,
            pt_exp_measures INTEGER,
            safety_measures INTEGER,
            mort_measures INTEGER,
            readm_measures INTEGER,
            last_checked TEXT,
            FOREIGN KEY(ccn) REFERENCES hospitals(ccn)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_ein ON prices(ein)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_attribution_confidence ON prices(attribution_confidence)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hospitals_state ON hospitals(state)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hospitals_zip_code ON hospitals(zip_code)")

    print("Ingesting federal hospital data with full scoring...")
    score_dist = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
    count = 0

    with open("hospitals.csv", "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ccn           = row["Facility ID"]
            name          = row["Facility Name"]
            address       = row["Address"]
            city          = row["City/Town"]
            state         = row["State"]
            zip_code      = row["ZIP Code"]
            phone         = row["Telephone Number"]
            hospital_type = row["Hospital Type"]
            ems           = row.get("Emergency Services", "No").strip()
            ownership     = row.get("Hospital Ownership", "").strip()
            overall_rating = row.get("Hospital overall rating", "Not Available").strip()
            website = (
                "https://www.google.com/search?q="
                + name.replace(" ", "+") + "+" + city + "+" + state + "+price+transparency"
            )

            quality = quality_index.get(ccn)
            score   = compute_score(row, quality)
            bd      = compute_score_breakdown(row, quality)
            flags   = compute_flags(row, quality, score)
            txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists = flags

            cursor.execute("""
                INSERT INTO hospitals (ccn, name, address, city, state, zip_code, phone, type, website, ems, overall_rating, ownership)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ccn) DO UPDATE SET
                    name=excluded.name, address=excluded.address, city=excluded.city,
                    state=excluded.state, zip_code=excluded.zip_code, phone=excluded.phone,
                    type=excluded.type, website=excluded.website, ems=excluded.ems,
                    overall_rating=excluded.overall_rating, ownership=excluded.ownership
            """, (ccn, name, address, city, state, zip_code, phone, hospital_type,
                  website, ems, overall_rating, ownership))

            cursor.execute("""
                INSERT INTO compliance (
                    ccn, score, txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists,
                    score_rating, score_pt_exp, score_safety, score_mortality, score_readmission,
                    pt_exp_measures, safety_measures, mort_measures, readm_measures, last_checked
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(ccn) DO UPDATE SET
                    score=excluded.score, txt_exists=excluded.txt_exists,
                    robots_ok=excluded.robots_ok, mrf_reachable=excluded.mrf_reachable,
                    mrf_valid=excluded.mrf_valid, mrf_fresh=excluded.mrf_fresh,
                    shoppable_exists=excluded.shoppable_exists,
                    score_rating=excluded.score_rating, score_pt_exp=excluded.score_pt_exp,
                    score_safety=excluded.score_safety, score_mortality=excluded.score_mortality,
                    score_readmission=excluded.score_readmission,
                    pt_exp_measures=excluded.pt_exp_measures,
                    safety_measures=excluded.safety_measures,
                    mort_measures=excluded.mort_measures,
                    readm_measures=excluded.readm_measures,
                    last_checked=datetime('now')
            """, (
                ccn, score, txt_exists, robots_ok, mrf_reachable, mrf_valid, mrf_fresh, shoppable_exists,
                bd["score_rating"], bd["score_pt_exp"], bd["score_safety"],
                bd["score_mortality"], bd["score_readmission"],
                bd["pt_exp_measures"], bd["safety_measures"],
                bd["mort_measures"], bd["readm_measures"],
            ))

            score_dist[min(score // 25, 4)] += 1
            count += 1
            if count % 1000 == 0:
                print(f"  {count} hospitals processed...")

    conn.commit()
    conn.close()

    print(f"\nIngestion complete: {count} hospitals.")
    print(f"Score distribution:")
    print(f"  0-24  (Non-Reporting):  {score_dist[0]}")
    print(f"  25-49 (Deficient):      {score_dist[1]}")
    print(f"  50-74 (Partial):        {score_dist[2]}")
    print(f"  75-99 (Compliant):      {score_dist[3]}")
    print(f"  100   (Full):           {score_dist[4]}")


if __name__ == "__main__":
    download_hospitals_csv()
    quality_index = fetch_quality_index()
    ingest_to_sqlite(quality_index)
