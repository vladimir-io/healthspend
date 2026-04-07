#!/usr/bin/env python3
"""
Generate and publish the 2026 State of Non-Compliance report.

Outputs an HTML report with:
- Top 10 hospitals using deactivated NPIs
- Top 10 license proxy offenders
"""

import argparse
import datetime as dt
import html
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple


def norm_name(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", (value or "").lower())).strip()


def names_match(a: str, b: str) -> bool:
    an = norm_name(a)
    bn = norm_name(b)
    if not an or not bn:
        return False
    return an == bn or an in bn or bn in an


def fetch_zombie_top10(cur: sqlite3.Cursor) -> List[Tuple[str, str, int, int, int]]:
    rows = cur.execute(
        """
        SELECT
          p.ccn,
          COALESCE(h.name, 'Unknown Facility') AS hospital_name,
          COUNT(*) AS affected_rows,
          SUM(CASE WHEN d.reason_code = '4' THEN 1 ELSE 0 END) AS code4_rows,
          COUNT(DISTINCT p.provider_npi) AS distinct_npis
        FROM prices p
        JOIN nppes_deactivations d ON d.npi = p.provider_npi
        LEFT JOIN dim_hospital h ON h.ccn = p.ccn
        WHERE p.provider_npi IS NOT NULL
          AND TRIM(p.provider_npi) <> ''
          AND d.reason_code IS NOT NULL
        GROUP BY p.ccn, COALESCE(h.name, 'Unknown Facility')
        ORDER BY affected_rows DESC, distinct_npis DESC, p.ccn
        LIMIT 10
        """
    ).fetchall()
    return [(str(r[0]), str(r[1]), int(r[2]), int(r[3] or 0), int(r[4])) for r in rows]


def fetch_license_proxy_top10(cur: sqlite3.Cursor) -> List[Tuple[str, str, int, int]]:
    candidates = cur.execute(
        """
        SELECT
          h.ccn,
          h.name,
          p.provider_npi,
          d.org_name,
          COUNT(*) as affected_rows
        FROM prices p
        JOIN dim_hospital h ON h.ccn = p.ccn
        LEFT JOIN dim_provider_npi d ON d.npi = p.provider_npi
        WHERE p.provider_npi IS NOT NULL
          AND TRIM(p.provider_npi) <> ''
          AND d.org_name IS NOT NULL
          AND TRIM(d.org_name) <> ''
        GROUP BY h.ccn, h.name, p.provider_npi, d.org_name
        """
    ).fetchall()

    offenders: Dict[str, Dict[str, object]] = {}
    for ccn, hospital_name, npi, org_name, affected_rows in candidates:
        if not hospital_name or not org_name:
            continue
        if names_match(hospital_name, org_name):
            continue

        alias_rows = cur.execute(
            "SELECT other_name FROM nppes_other_names WHERE npi = ?",
            (npi,),
        ).fetchall()
        if any(names_match(hospital_name, alias[0]) for alias in alias_rows if alias and alias[0]):
            continue

        key = str(ccn)
        if key not in offenders:
            offenders[key] = {
                "hospital_name": str(hospital_name),
                "affected_rows": 0,
                "npis": set(),
            }

        offenders[key]["affected_rows"] = int(offenders[key]["affected_rows"]) + int(affected_rows)
        offenders[key]["npis"].add(str(npi))

    ranked: List[Tuple[str, str, int, int]] = []
    for ccn, data in offenders.items():
        ranked.append(
            (
                ccn,
                str(data["hospital_name"]),
                int(data["affected_rows"]),
                len(data["npis"]),
            )
        )

    ranked.sort(key=lambda x: (-x[2], -x[3], x[0]))
    return ranked[:10]


def build_html_report(year: int, zombie_rows, proxy_rows) -> str:
    generated = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M UTC")

    def tr(cols: List[str]) -> str:
        return "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in cols) + "</tr>"

    zombie_body = "\n".join(
        tr([
            str(i + 1),
            row[0],
            row[1],
            f"{row[2]:,}",
            f"{row[3]:,}",
            f"{row[4]:,}",
        ])
        for i, row in enumerate(zombie_rows)
    ) or "<tr><td colspan=\"6\">No deactivated NPI findings in current dataset.</td></tr>"

    proxy_body = "\n".join(
        tr([
            str(i + 1),
            row[0],
            row[1],
            f"{row[2]:,}",
            f"{row[3]:,}",
        ])
        for i, row in enumerate(proxy_rows)
    ) or "<tr><td colspan=\"5\">No license proxy findings in current dataset.</td></tr>"

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>State of Non-Compliance {year}</title>
  <style>
    :root {{
      --bg: #0e1116;
      --panel: #151a22;
      --line: #2a3140;
      --text: #e7ebf3;
      --muted: #a5b1c6;
      --warn: #ffb454;
      --accent: #6cc3ff;
    }}
    body {{
      margin: 0;
      background: radial-gradient(1200px 400px at 10% -10%, #202a3a 0%, transparent 70%), var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, -apple-system, Segoe UI, Helvetica, Arial, sans-serif;
      line-height: 1.45;
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 40px 22px 64px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 2rem;
      letter-spacing: -0.02em;
    }}
    .sub {{
      color: var(--muted);
      margin-bottom: 28px;
      font-size: 0.95rem;
    }}
    section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 18px;
      margin-bottom: 18px;
      box-shadow: 0 12px 40px rgba(0,0,0,0.25);
    }}
    h2 {{
      margin: 0 0 10px;
      font-size: 1.05rem;
      color: var(--warn);
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 9px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--accent);
      font-size: 0.76rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      font-weight: 700;
    }}
    .small {{ color: var(--muted); font-size: 0.84rem; }}
    a {{ color: var(--accent); }}
  </style>
</head>
<body>
  <main>
    <h1>State of Non-Compliance {year}</h1>
    <p class=\"sub\">Generated {generated}. This report surfaces the highest-volume compliance risks in current filing data.</p>

    <section>
      <h2>Top 10 Hospitals Using Deactivated NPIs</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>CCN</th>
            <th>Hospital</th>
            <th>Affected Rows</th>
            <th>Reason Code 4 Rows</th>
            <th>Distinct Deactivated NPIs</th>
          </tr>
        </thead>
        <tbody>
          {zombie_body}
        </tbody>
      </table>
    </section>

    <section>
      <h2>Top 10 License Proxy Offenders</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>CCN</th>
            <th>Hospital</th>
            <th>Affected Rows</th>
            <th>Distinct Proxy NPIs</th>
          </tr>
        </thead>
        <tbody>
          {proxy_body}
        </tbody>
      </table>
      <p class=\"small\">Method: hospital legal entity name mismatch against NPI organization name and known aliases.</p>
    </section>
  </main>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish State of Non-Compliance report")
    parser.add_argument("--db", default="scraper/prices.db", help="SQLite database path")
    parser.add_argument("--year", type=int, default=2026, help="Report year")
    parser.add_argument(
        "--out",
        default="web/public/state-of-non-compliance-2026.html",
        help="Output HTML path",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        zombie_rows = fetch_zombie_top10(cur)
        proxy_rows = fetch_license_proxy_top10(cur)
    finally:
        conn.close()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(build_html_report(args.year, zombie_rows, proxy_rows), encoding="utf-8")
    print(f"Published report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
