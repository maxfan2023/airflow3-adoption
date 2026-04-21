#!/usr/bin/env python3
"""Prewarm a Nexus-backed DAG bundle into the local cache on an Airflow node."""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cli_support import ScriptOutputSession, StepReporter, resolve_runtime_logging_settings
from common import (
    build_latest_manifest_path,
    build_version_record_path,
    normalize_environment,
    parse_bool,
    resolve_credentials_file,
    load_properties,
)
from my_company.airflow_bundles.nexus import NexusArtifactClient, prewarm_bundle_cache


DEFAULT_NEXUS_REPOSITORY_URL = "https://nexus302.systems.uk.hsbc:8081/nexus/repository/raw-alm-uat_n3p"
DEFAULT_CACHE_ROOT = "/FCR_APP/abinitio/airflow/v3/dag_bundle_cache"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Prewarm a DAG bundle cache from Nexus without changing Airflow configuration.",
    )
    parser.add_argument("--environment", choices=("dev", "uat", "prod"), default="dev")
    parser.add_argument("--bundle-name", required=True)
    parser.add_argument("--version", help="Optional bundle version. Defaults to latest.json.")
    parser.add_argument("--manifest-path", help="Override the latest manifest path in Nexus.")
    parser.add_argument("--credentials-file", help="Override the environment-specific Nexus credentials file.")
    parser.add_argument("--repository-url", help="Override the Nexus repository root URL.")
    parser.add_argument("--cache-root", default=DEFAULT_CACHE_ROOT)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args(argv)


def debug_print(enabled, message):
    if enabled:
        print("[DEBUG] {0}".format(message))


def _run_with_args(args, reporter=None):
    reporter = reporter or StepReporter(enabled=False)
    environment = normalize_environment(args.environment)
    reporter.section("🧭", "Resolve Bundle Prewarm Inputs")
    credentials_file = resolve_credentials_file(args.credentials_file, environment=environment)
    properties = load_properties(credentials_file)
    repository_url = str(
        args.repository_url
        or properties.get("NEXUS_REPOSITORY_URL")
        or DEFAULT_NEXUS_REPOSITORY_URL
    ).rstrip("/")
    username = properties.get("NEXUS_USERNAME")
    password = properties.get("NEXUS_PASSWORD")
    if not username or not password:
        raise ValueError("NEXUS_USERNAME and NEXUS_PASSWORD are required for bundle prewarm.")
    verify_tls = not parse_bool(properties.get("NEXUS_INSECURE"), default=False)
    timeout_seconds = int(properties.get("NEXUS_TIMEOUT_SECONDS", 60))
    manifest_path = args.manifest_path or build_latest_manifest_path(environment, args.bundle_name)
    cache_root = Path(args.cache_root).expanduser().resolve()
    reporter.value("🌍", "Environment", environment)
    reporter.value("📦", "Bundle name", args.bundle_name)
    reporter.value("🔐", "Credentials file", credentials_file)
    reporter.value("📍", "Latest manifest path", manifest_path)
    reporter.value("🗄️", "Cache root", cache_root)

    reporter.section("📥", "Read Bundle Metadata")
    client = NexusArtifactClient(
        repository_url=repository_url,
        username=username,
        password=password,
        verify_tls=verify_tls,
        timeout_seconds=timeout_seconds,
    )
    if args.version:
        selected_manifest = client.read_manifest(
            build_version_record_path(environment, args.bundle_name, args.version)
        )
    else:
        selected_manifest = client.read_manifest(manifest_path)
    reporter.value("🏷️", "Selected version", selected_manifest.version)
    reporter.value("🔗", "Artifact path", selected_manifest.artifact_path)
    reporter.value("🧮", "Expected SHA256", selected_manifest.sha256)
    debug_print(args.debug, "Selected manifest payload: {0}".format(selected_manifest.to_dict()))

    reporter.section("🔥", "Prewarm Bundle Cache")
    metadata = prewarm_bundle_cache(
        bundle_name=args.bundle_name,
        repository_url=repository_url,
        cache_root=cache_root,
        username=username,
        password=password,
        verify_tls=verify_tls,
        timeout_seconds=timeout_seconds,
        manifest_path=manifest_path,
        version=args.version or "",
    )
    reporter.value("📁", "Cached bundle path", metadata["package_root"])
    reporter.value("🏷️", "Cached version", metadata["version"])
    return {
        "environment": environment,
        "bundle_name": args.bundle_name,
        "version": metadata["version"],
        "package_root": metadata["package_root"],
        "manifest_path": manifest_path,
    }


def main(argv=None):
    args = parse_args(argv)
    logging_settings = resolve_runtime_logging_settings(environment=args.environment)
    session = ScriptOutputSession(
        script_name="prewarm_dag_bundle_cache",
        log_directory=logging_settings.directory,
        retention_days=logging_settings.retention_days,
    )
    exit_code = 0
    reporter = StepReporter(enabled=True)

    try:
        with session:
            result = _run_with_args(args, reporter=reporter)
            reporter.section("📋", "Bundle Prewarm Summary")
            reporter.value("🌍", "Environment", result["environment"])
            reporter.value("📦", "Bundle name", result["bundle_name"])
            reporter.value("🏷️", "Version", result["version"])
            reporter.value("📁", "Cached bundle path", result["package_root"])
            reporter.value("📍", "Latest manifest path", result["manifest_path"])
            reporter.message("✅", "Bundle cache prewarm completed successfully.")
    except Exception as exc:  # noqa: BLE001 - CLI should report all failures clearly.
        print("❌ Error: {0}".format(exc), file=sys.stderr)
        exit_code = 1
    finally:
        if session.stdout_log_path and session.stderr_log_path:
            reporter.section("🗂️", "Execution Logs")
            reporter.value("📄", "STDOUT log", session.stdout_log_path)
            reporter.value("🚨", "STDERR log", session.stderr_log_path)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
