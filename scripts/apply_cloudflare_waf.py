#!/usr/bin/env python3
"""Apply declarative Cloudflare WAF custom rules from infra/cloudflare/*.json."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / "web" / ".env"
RULES_DIR = ROOT / "infra" / "cloudflare"
RULE_TAG = "healthspend-scanner-block"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_FILE.is_file():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    for key in ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_ZONE_ID"):
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


def api(token: str, method: str, url: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode())
    if not payload.get("success"):
        raise RuntimeError(payload.get("errors"))
    return payload


def resolve_zone_id(token: str, zone_name: str, zone_id: str | None) -> str:
    if zone_id:
        return zone_id
    q = urllib.parse.quote(zone_name)
    payload = api(token, "GET", f"https://api.cloudflare.com/client/v4/zones?name={q}&status=active")
    zones = payload.get("result") or []
    if not zones:
        raise RuntimeError(f"No active zone found for {zone_name}")
    return zones[0]["id"]


def load_rule(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    required = ("description", "action", "expression")
    missing = [k for k in required if k not in data]
    if missing:
        raise RuntimeError(f"{path.name} missing keys: {missing}")
    return data


def upsert_custom_rule(token: str, zone_id: str, rule: dict) -> None:
    import urllib.parse

    entry_url = (
        f"https://api.cloudflare.com/client/v4/zones/{zone_id}"
        "/rulesets/phases/http_request_firewall_custom/entrypoint"
    )
    payload = api(token, "GET", entry_url)
    ruleset = payload["result"]
    rules = list(ruleset.get("rules") or [])
    description = rule["description"]

    new_rule = {
        "action": rule["action"],
        "expression": rule["expression"],
        "description": description,
        "enabled": True,
    }

    replaced = False
    for i, existing in enumerate(rules):
        if existing.get("description") == description:
            rules[i] = {**existing, **new_rule}
            replaced = True
            break
    if not replaced:
        rules.append(new_rule)

    api(
        token,
        "PUT",
        entry_url,
        {
            "rules": rules,
        },
    )
    verb = "Updated" if replaced else "Created"
    print(f"✓ {verb} WAF custom rule: {description}")


def main() -> int:
    env = load_env()
    token = env.get("CLOUDFLARE_API_TOKEN")
    if not token:
        print("Set CLOUDFLARE_API_TOKEN (Zone Firewall Edit) in web/.env", file=sys.stderr)
        return 1

    rule_files = sorted(RULES_DIR.glob("waf-*.json"))
    if not rule_files:
        print(f"No rules in {RULES_DIR}", file=sys.stderr)
        return 1

    zone_name = json.loads(rule_files[0].read_text()).get("zone_name", "healthspend.lol")
    try:
        zone_id = resolve_zone_id(token, zone_name, env.get("CLOUDFLARE_ZONE_ID"))
        print(f"Zone: {zone_name} ({zone_id})")
        for path in rule_files:
            upsert_custom_rule(token, zone_id, load_rule(path))
    except urllib.error.HTTPError as err:
        print(f"Cloudflare API error ({err.code}): {err.read().decode(errors='replace')}", file=sys.stderr)
        return 1
    except RuntimeError as err:
        print(f"WAF apply failed: {err}", file=sys.stderr)
        return 1

    print(f"\nRules source: {RULES_DIR} (tag: {RULE_TAG})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
