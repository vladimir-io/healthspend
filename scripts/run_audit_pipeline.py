#!/usr/bin/env python3
"""
Audit orchestration for HealthSpend data refresh.
Runs schema migration, optional NPPES/CMS loads, parser smoke, and audits.
"""

import argparse
import shutil
import shlex
import subprocess
from pathlib import Path


def run(cmd: str, cwd: Path) -> None:
    print(f"\n$ {cmd}")
    subprocess.run(shlex.split(cmd), cwd=str(cwd), check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run HealthSpend audit pipeline")
    parser.add_argument("--db", default="scraper/prices.db", help="Path to prices db")
    parser.add_argument("--nppes-core")
    parser.add_argument("--nppes-practice")
    parser.add_argument("--nppes-other-names")
    parser.add_argument("--nppes-deactivations")
    parser.add_argument("--nppes-endpoints")
    parser.add_argument("--cms-attesters")
    parser.add_argument("--benchmark-label", default="", help="Run p50/p95 benchmark with this label")
    parser.add_argument("--publish-bucket", default="", help="If set, publish hot artifacts to this R2 bucket")
    parser.add_argument("--publish-prefix", default="hot", help="R2 key prefix for publish step")
    parser.add_argument("--publish-files", nargs="*", default=["web/public/audit_data.db"], help="Artifact files for R2 publish")
    parser.add_argument("--dry-run-publish", action="store_true", help="Print R2 publish commands without executing")
    parser.add_argument("--mrf-dir", default="", help="Optional directory containing downloaded MRF files")
    parser.add_argument("--cleanup", action="store_true", help="Alias for --cleanup-mrf-dir")
    parser.add_argument("--cleanup-mrf-dir", action="store_true", help="Delete --mrf-dir after the pipeline completes successfully")
    parser.add_argument("--state", default="", help="Optional state filter for MRF download/ingest stage")
    parser.add_argument("--threads", type=int, default=4, help="Worker hint for MRF streaming stage")
    parser.add_argument("--mrf-limit", type=int, default=0, help="Optional cap on number of hospitals to download/ingest")
    parser.add_argument("--skip-existing-mrfs", action="store_true", help="Reuse files already present in --mrf-dir")
    parser.add_argument("--skip-parse-smoke", action="store_true")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]

    run(f"python3 scripts/migrate_schema.py {args.db}", repo)

    if any([args.nppes_core, args.nppes_practice, args.nppes_other_names, args.nppes_deactivations, args.nppes_endpoints]):
        cmd = [
            "python3", "scripts/load_nppes_v2.py", "--db", args.db,
        ]
        if args.nppes_core:
            cmd += ["--core", args.nppes_core]
        if args.nppes_practice:
            cmd += ["--practice", args.nppes_practice]
        if args.nppes_other_names:
            cmd += ["--other-names", args.nppes_other_names]
        if args.nppes_deactivations:
            cmd += ["--deactivations", args.nppes_deactivations]
        if args.nppes_endpoints:
            cmd += ["--endpoints", args.nppes_endpoints]
        run(" ".join(shlex.quote(x) for x in cmd), repo)

    if args.cms_attesters:
        run(
            " ".join(
                [
                    "python3",
                    "scripts/load_cms_attesters.py",
                    "--db",
                    shlex.quote(args.db),
                    "--file",
                    shlex.quote(args.cms_attesters),
                ]
            ),
            repo,
        )

    if not args.skip_parse_smoke:
        run("cargo run --bin scraper -- --parse-only", repo / "scraper")

    if args.mrf_dir:
        mrf_cmd = [
            "python3",
            "scripts/mrf_streamer.py",
            "--prices-db",
            args.db,
            "--mrf-dir",
            args.mrf_dir,
        ]
        compliance_db = str(Path(args.db).with_name("compliance.db"))
        mrf_cmd += ["--compliance-db", compliance_db]
        if args.state:
            mrf_cmd += ["--state", args.state]
        if args.threads > 0:
            mrf_cmd += ["--threads", str(args.threads)]
        if args.mrf_limit > 0:
            mrf_cmd += ["--limit", str(args.mrf_limit)]
        if args.skip_existing_mrfs:
            mrf_cmd.append("--skip-existing")
        if args.cleanup or args.cleanup_mrf_dir:
            mrf_cmd.append("--cleanup")

        run(" ".join(shlex.quote(x) for x in mrf_cmd), repo)

    run(f"python3 scripts/audit_zombie_npi.py --db {args.db}", repo)
    run(f"python3 scripts/audit_license_proxy.py --db {args.db}", repo)
    run(f"python3 scripts/audit_attester_validity.py --db {args.db}", repo)

    if args.nppes_endpoints:
        run(f"python3 scripts/ping_nppes_endpoints.py --db {args.db} --limit 100", repo)

    if args.benchmark_label:
        run(
            f"python3 scripts/benchmark_hot_queries.py --db {args.db} --label {shlex.quote(args.benchmark_label)}",
            repo,
        )

    if args.publish_bucket:
        cmd = [
            "python3",
            "scripts/deploy_artifacts.py",
            "--bucket",
            args.publish_bucket,
            "--prefix",
            args.publish_prefix,
            "--files",
            *args.publish_files,
        ]
        if args.dry_run_publish:
            cmd.append("--dry-run")
        run(" ".join(shlex.quote(x) for x in cmd), repo)

    if args.cleanup or args.cleanup_mrf_dir:
        if not args.mrf_dir:
            raise SystemExit("--cleanup or --cleanup-mrf-dir requires --mrf-dir")

        mrf_dir = Path(args.mrf_dir).expanduser().resolve()
        if not mrf_dir.exists():
            print(f"MRF directory not found, skipping cleanup: {mrf_dir}")
        elif mrf_dir.is_dir():
            print(f"Removing downloaded MRF directory: {mrf_dir}")
            shutil.rmtree(mrf_dir)
        else:
            raise SystemExit(f"--mrf-dir must point to a directory, got: {mrf_dir}")

    print("\nPipeline finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
