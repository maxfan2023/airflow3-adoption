#!/usr/bin/env python3
"""Deploy a DAG archive from Nexus or a local file into Airflow dags."""

import argparse
import os
import sys
from pathlib import Path

from common import normalize_environment, parse_bool, resolve_credentials_file, load_properties
from deploy_steps import (
    ArchiveExtractor,
    ArtifactFetcher,
    ChecksumValidator,
    DeploymentError,
    DeploymentPublisher,
    ImportChecker,
    PythonCheckOrchestrator,
    RuleChecker,
    SyntaxChecker,
    TagProcessor,
    load_pipeline_config,
    strip_archive_suffix,
)


def parse_args(argv=None):
    """Build the command line interface for DAG deployment."""
    parser = argparse.ArgumentParser(
        description="Deploy a DAG package from Nexus or a local archive into Airflow dags.",
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--artifact-url",
        help="Full URL of the DAG archive in Nexus or another reachable location.",
    )
    source_group.add_argument(
        "--artifact-path",
        help="Path inside the configured Nexus repository, for example com/example/dag.zip.",
    )
    source_group.add_argument(
        "--archive-file",
        help="Local DAG archive file to validate and deploy.",
    )
    parser.add_argument(
        "--config",
        help="Path to the deployment pipeline JSON config file.",
    )
    parser.add_argument(
        "--environment",
        choices=("dev", "uat", "prod"),
        default="dev",
        help="Deployment environment used to resolve default config and credentials files.",
    )
    parser.add_argument(
        "--credentials-file",
        help="Path to the Nexus credentials env file.",
    )
    parser.add_argument(
        "--working-root",
        help="Override the working root path from the deployment config.",
    )
    parser.add_argument(
        "--expected-sha256",
        help="Expected SHA256 value when checksum.mode is cli_value.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full validation flow without writing to landing or dags.",
    )
    return parser.parse_args(argv)


def build_release_name(archive_name, allowed_suffixes):
    """Create a unique release directory name from the archive file name."""
    base_name = strip_archive_suffix(archive_name, allowed_suffixes)
    session_suffix = "{0}-{1}".format(os.getpid(), _utc_timestamp())
    return "{0}-{1}".format(base_name, session_suffix)


def run(argv=None):
    """Execute the deployment pipeline and return a summary dictionary."""
    args = parse_args(argv)
    environment = normalize_environment(args.environment)
    config = load_pipeline_config(args.config, args.working_root, environment=environment)
    release_name = None

    credentials = {}
    timeout_seconds = config.nexus.timeout_seconds
    verify_tls = config.nexus.verify_tls
    if args.artifact_url or args.artifact_path:
        credentials_file = resolve_credentials_file(
            args.credentials_file,
            environment=environment,
        )
        credentials = load_properties(credentials_file)
        username = credentials.get("NEXUS_USERNAME")
        password = credentials.get("NEXUS_PASSWORD")
        if not username or not password:
            raise DeploymentError(
                "NEXUS_USERNAME and NEXUS_PASSWORD are required for remote artifact download."
            )
        timeout_seconds = int(credentials.get("NEXUS_TIMEOUT_SECONDS", timeout_seconds))
        if parse_bool(credentials.get("NEXUS_INSECURE"), default=False):
            verify_tls = False
    else:
        username = None
        password = None

    fetcher = ArtifactFetcher(
        repository_url=config.nexus.repository_url,
        timeout_seconds=timeout_seconds,
        verify_tls=verify_tls,
        username=username,
        password=password,
    )

    session_root = config.paths.working_root / "{0}-{1}".format(os.getpid(), _utc_timestamp())
    download_dir = session_root / "downloads"
    extract_dir = session_root / "extracted"

    artifact = fetcher.fetch(
        artifact_url=args.artifact_url,
        artifact_path=args.artifact_path,
        archive_file=args.archive_file,
        download_dir=download_dir,
        checksum_mode=config.checksum.mode,
        sidecar_suffix=config.checksum.sidecar_suffix,
    )

    release_name = build_release_name(
        artifact.local_archive_path.name,
        config.archive.allowed_suffixes,
    )
    checksum = ChecksumValidator(
        mode=config.checksum.mode,
        sidecar_suffix=config.checksum.sidecar_suffix,
    ).validate(
        artifact.local_archive_path,
        expected_sha256=args.expected_sha256,
        sidecar_content=artifact.sidecar_content,
    )

    extractor = ArchiveExtractor(
        allowed_suffixes=config.archive.allowed_suffixes,
        require_single_top_level_dir=config.archive.require_single_top_level_dir,
    )
    extracted_root = extractor.extract(artifact.local_archive_path, extract_dir)

    python_checks = PythonCheckOrchestrator(
        [
            SyntaxChecker(),
            ImportChecker(
                extra_pythonpath=config.imports.extra_pythonpath,
                activation_command=config.imports.activation_command,
                shell_executable=config.imports.shell_executable,
                python_executable=config.imports.python_executable,
                timeout_seconds=config.imports.timeout_seconds,
            ),
        ]
    )
    python_checks.run(extracted_root)

    RuleChecker(
        name_rules=config.rules.name_rules,
        queue_rules=config.rules.queue_rules,
    ).validate(extracted_root)

    TagProcessor(
        source_variable_name=config.tagging.source_variable_name,
        managed_tags=config.tagging.managed_tags,
        us_sources=config.tagging.us_sources,
        us_tag=config.tagging.us_tag,
        global_tag=config.tagging.global_tag,
    ).process_package(extracted_root)

    publish_result = DeploymentPublisher(
        landing_root=config.paths.landing_root,
        dags_root=config.paths.dags_root,
        backup_root=config.paths.backup_root,
    ).publish(
        package_root=extracted_root,
        release_name=release_name,
        dry_run=args.dry_run,
    )

    return {
        "artifact": artifact,
        "checksum": checksum,
        "release_name": release_name,
        "publish_result": publish_result,
        "working_root": session_root,
        "environment": environment,
        "config_path": config.config_path,
    }


def main(argv=None):
    """CLI entry point."""
    try:
        result = run(argv)
        artifact = result["artifact"]
        checksum = result["checksum"]
        publish_result = result["publish_result"]

        print("Artifact staged: {0}".format(artifact.local_archive_path))
        print("Environment: {0}".format(result["environment"]))
        print("Config file: {0}".format(result["config_path"]))
        if artifact.resolved_url:
            print("Artifact URL: {0}".format(artifact.resolved_url))
        print("SHA256: {0}".format(checksum.actual_sha256))
        if checksum.expected_sha256:
            print("Expected SHA256: {0}".format(checksum.expected_sha256))
        print("Release name: {0}".format(result["release_name"]))
        print("Landing target: {0}".format(publish_result.landing_release_dir))
        print("Live target: {0}".format(publish_result.live_target_dir))
        if publish_result.dry_run:
            print("Dry run enabled. Landing and live publish steps were skipped.")
        else:
            print("Deployment completed successfully.")
        return 0
    except (DeploymentError, ValueError) as exc:
        print("Error: {0}".format(exc), file=sys.stderr)
        return 1


def _utc_timestamp():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


if __name__ == "__main__":
    raise SystemExit(main())
