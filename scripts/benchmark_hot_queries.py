#!/usr/bin/env python3
"""
Benchmark hot query latency and persist p50/p95 history.

Tracks before/after by label:
python3 scripts/benchmark_hot_queries.py --db scraper/prices.db --label before-r2
python3 scripts/benchmark_hot_queries.py --db scraper/prices.db --label after-r2
"""

import argparse
import json
import sqlite3
import statistics
import time
from pathlib import Path
from typing import List, Tuple


Query = Tuple[str, str, Tuple[object, ...]]


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    vals = sorted(values)
    k = (len(vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(vals) - 1)
    if f == c:
        return vals[f]
    d = k - f
    return vals[f] * (1.0 - d) + vals[c] * d


def run_query(cur: sqlite3.Cursor, sql: str, params: Tuple[object, ...]) -> float:
    t0 = time.perf_counter()
    cur.execute(sql, params).fetchall()
    return (time.perf_counter() - t0) * 1000.0


def benchmark(cur: sqlite3.Cursor, query: Query, warmup: int, iterations: int) -> dict:
    name, sql, params = query
    for _ in range(warmup):
        run_query(cur, sql, params)

    samples = [run_query(cur, sql, params) for _ in range(iterations)]
    return {
        "name": name,
        "samples_ms": samples,
        "p50_ms": round(percentile(samples, 0.50), 2),
        "p95_ms": round(percentile(samples, 0.95), 2),
        "avg_ms": round(statistics.fmean(samples), 2),
        "min_ms": round(min(samples), 2),
        "max_ms": round(max(samples), 2),
    }


def default_queries() -> List[Query]:
    return [
        (
            "top_cash_by_code",
            """
            SELECT ccn, hospital_name, code, payer_name, cash_price, negotiated_rate, zombie_status
            FROM hot_price_compare
            WHERE code = ? AND attribution_confidence >= ?
            ORDER BY cash_price ASC
            LIMIT 50
            """,
            ("70450", 0.90),
        ),
        (
            "payer_delta_by_code",
            """
            SELECT ccn, payer_name, plan_name, delta_abs, delta_pct, license_proxy_suspected
            FROM hot_price_compare
            WHERE code = ?
            ORDER BY delta_abs DESC
            LIMIT 50
            """,
            ("70450",),
        ),
        (
            "zombie_filtered_lookup",
            """
            SELECT ccn, hospital_name, zombie_status, zombie_reason_code, accessibility
            FROM hot_price_compare
            WHERE zombie_status = 'deactivated'
            ORDER BY ccn
            LIMIT 50
            """,
            (),
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark hot SQLite query latency")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to SQLite database")
    parser.add_argument("--label", default="ad-hoc", help="Benchmark label (before/after)")
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--history", default="scripts/benchmarks/hot_query_history.jsonl")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    report = {
        "label": args.label,
        "db": str(db_path),
        "warmup": args.warmup,
        "iterations": args.iterations,
        "results": [benchmark(cur, q, args.warmup, args.iterations) for q in default_queries()],
    }

    overall_p50 = percentile([r["p50_ms"] for r in report["results"]], 0.5)
    overall_p95 = percentile([r["p95_ms"] for r in report["results"]], 0.95)
    report["overall"] = {
        "p50_ms": round(overall_p50, 2),
        "p95_ms": round(overall_p95, 2),
    }

    print(json.dumps(report, indent=2))

    history_path = Path(args.history)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(report) + "\n")

    print(f"Appended benchmark history: {history_path}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
