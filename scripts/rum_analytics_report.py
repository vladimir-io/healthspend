#!/usr/bin/env python3
"""Summarize HealthSpend RUM rollups from Cloudflare KV."""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / "web" / ".env"
KV_NAMESPACE_ID = "3a225c32ca9f48d18d7132ff0160ba5d"
DEFAULT_DAYS = 7


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_FILE.is_file():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    for key in ("CLOUDFLARE_API_TOKEN_READ_ONLY", "CLOUDFLARE_ACCOUNT_ID"):
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


def kv_list(token: str, account_id: str, prefix: str) -> list[str]:
    keys: list[str] = []
    cursor: str | None = None
    while True:
        params: dict[str, str] = {"prefix": prefix, "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        qs = urllib.parse.urlencode(params)
        url = (
            f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
            f"/storage/kv/namespaces/{KV_NAMESPACE_ID}/keys?{qs}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode())
        if not payload.get("success"):
            raise RuntimeError(payload.get("errors"))
        result = payload["result"]
        keys.extend(item["name"] for item in result)
        cursor = result[-1].get("cursor") if result else None
        if not cursor or len(result) < 1000:
            break
    return keys


def kv_get_many(token: str, account_id: str, keys: list[str]) -> dict[str, int]:
    if not keys:
        return {}
    out: dict[str, int] = {}
    for key in keys:
        encoded = urllib.parse.quote(key, safe="")
        url = (
            f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
            f"/storage/kv/namespaces/{KV_NAMESPACE_ID}/values/{encoded}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode()
        except urllib.error.HTTPError as err:
            if err.code == 404:
                continue
            raise
        try:
            out[key] = int(float(raw))
        except (TypeError, ValueError):
            out[key] = 0
    return out


def day_range(days: int) -> list[str]:
    today = date.today()
    return [(today - timedelta(days=i)).isoformat() for i in range(days)]


def print_rows(title: str, rows: list[tuple], headers: tuple[str, ...]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(no data yet — deploy RUM ingest and wait for traffic)")
        return
    widths = [max(len(h), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print("-" * (sum(widths) + 2 * (len(headers) - 1)))
    for row in rows:
        print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))))


def main() -> int:
    days = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DAYS
    env = load_env()
    token = env.get("CLOUDFLARE_API_TOKEN_READ_ONLY") or env.get("CLOUDFLARE_API_TOKEN")
    account_id = env.get("CLOUDFLARE_ACCOUNT_ID")
    if not token or not account_id:
        print("Set CLOUDFLARE_API_TOKEN_READ_ONLY and CLOUDFLARE_ACCOUNT_ID in web/.env", file=sys.stderr)
        return 1

    print(f"HealthSpend RUM report — last {days} day(s) — KV namespace `{KV_NAMESPACE_ID}`")

    try:
        all_keys: list[str] = []
        for day in day_range(days):
            all_keys.extend(kv_list(token, account_id, f"d:{day}:"))
        values = kv_get_many(token, account_id, all_keys)
    except urllib.error.HTTPError as err:
        print(f"KV query failed ({err.code}): {err.read().decode(errors='replace')}", file=sys.stderr)
        return 1
    except RuntimeError as err:
        print(f"KV query failed: {err}", file=sys.stderr)
        return 1

    if not values:
        print("\n(no data yet — deploy RUM ingest and wait for traffic)")
        print("\nRun: python3 scripts/rum_analytics_report.py [days]")
        return 0

    events: dict[str, int] = defaultdict(int)
    paths: dict[str, int] = defaultdict(int)
    routes: dict[str, int] = defaultdict(int)
    sources: dict[str, int] = defaultdict(int)
    referrers: dict[str, int] = defaultdict(int)
    daily: dict[str, int] = defaultdict(int)
    search_n = 0
    search_ms = 0.0
    db_warm: dict[str, tuple[int, float]] = defaultdict(lambda: (0, 0.0))
    funnel: dict[str, int] = defaultdict(int)
    dispute_channels: dict[str, int] = defaultdict(int)

    event_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):event:(.+)$")
    path_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):path:(.+)$")
    route_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):route:(.+)$")
    src_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):src:(.+)$")
    ref_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):ref:(.+)$")
    funnel_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):funnel:(.+)$")
    dispute_send_re = re.compile(r"^d:(\d{4}-\d{2}-\d{2}):dispute_send:(.+)$")

    for key, count in values.items():
        if m := event_re.match(key):
            daily[m.group(1)] += count
            events[m.group(2)] += count
            continue
        if m := path_re.match(key):
            paths[m.group(2)] += count
            continue
        if m := route_re.match(key):
            routes[m.group(2)] += count
            continue
        if m := src_re.match(key):
            sources[m.group(2)] += count
            continue
        if m := ref_re.match(key):
            referrers[m.group(2)] += count
            continue
        if m := funnel_re.match(key):
            funnel[m.group(2)] += count
            continue
        if m := dispute_send_re.match(key):
            dispute_channels[m.group(2)] += count
            continue
        if key.endswith(":search:n"):
            search_n += count
            continue
        if key.endswith(":search:ms_sum"):
            search_ms += count
            continue
        if ":db_warm:" in key and key.endswith(":ms_sum"):
            tier = key.split(":db_warm:", 1)[1].rsplit(":", 1)[0]
            n, ms = db_warm[tier]
            db_warm[tier] = (n, ms + count)
            continue
        if ":db_warm:" in key and not key.endswith(":ms_sum"):
            tier = key.split(":db_warm:", 1)[1]
            n, ms = db_warm[tier]
            db_warm[tier] = (n + count, ms)

    print_rows(
        "Events",
        sorted(((k, v) for k, v in events.items()), key=lambda x: -x[1]),
        ("event", "count"),
    )
    print_rows(
        "App routes (page_view meta)",
        sorted(((k, v) for k, v in routes.items()), key=lambda x: -x[1])[:15],
        ("route", "views"),
    )
    print_rows(
        "Hash paths (all events)",
        sorted(((k, v) for k, v in paths.items()), key=lambda x: -x[1])[:15],
        ("path", "hits"),
    )

    median = round(search_ms / search_n) if search_n else 0
    print_rows("Search performance", [(search_n, median)], ("searches", "avg_ms"))

    searches = events.get("search", 0)
    conversions = funnel.get("search_to_dispute", 0)
    rate = f"{(100 * conversions / searches):.1f}%" if searches else "—"
    print_rows(
        "Search → dispute funnel",
        [
            (searches, funnel.get("dispute_open", 0), conversions, funnel.get("dispute_send", 0), rate),
        ],
        ("searches", "dispute_open", "search_to_dispute", "dispute_send", "conv_rate"),
    )
    print_rows(
        "Dispute send channel",
        sorted(((k, v) for k, v in dispute_channels.items()), key=lambda x: -x[1]),
        ("channel", "sends"),
    )

    warm_rows = []
    for tier, (n, ms) in sorted(db_warm.items(), key=lambda x: -x[1][0]):
        avg = round(ms / n) if n else 0
        warm_rows.append((tier, n, avg))
    print_rows("DB warm by tier", warm_rows, ("tier", "warms", "avg_ms"))

    print_rows(
        "UTM sources",
        sorted(((k, v) for k, v in sources.items()), key=lambda x: -x[1])[:15],
        ("utm_source", "hits"),
    )
    print_rows(
        "External referrers",
        sorted(((k, v) for k, v in referrers.items()), key=lambda x: -x[1])[:15],
        ("referrer", "hits"),
    )
    print_rows(
        "Daily volume",
        sorted(((k, v) for k, v in daily.items()), key=lambda x: x[0], reverse=True),
        ("day", "events"),
    )

    print("\nRun: python3 scripts/rum_analytics_report.py [days]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
