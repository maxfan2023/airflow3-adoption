#!/usr/bin/env python3
"""Package DAG files into a zip archive and upload the archive to Nexus.

Usage examples:

1. Package one DAG directory and upload it with the company default Nexus path:
   python3 scripts/dag_publish/package_and_upload_dag.py \
     dags/customer_sync \
     --artifact-id DAG_ID_RELEASE \
     --version 0001.4972.user_name

2. Package multiple sources into one zip:
   python3 scripts/dag_publish/package_and_upload_dag.py \
     dags/customer_sync.py dags/common \
     --artifact-id DAG_BUNDLE_RELEASE \
     --version 0001.4972.user_name

3. Validate the generated zip name and upload URL without sending anything:
   python3 scripts/dag_publish/package_and_upload_dag.py \
     dags/customer_sync \
     --artifact-id DAG_ID_RELEASE \
     --version 0001.4972.user_name \
     --dry-run

The script reads Nexus credentials from the first existing predefined location:
1. configs/dag_publish/nexus_credentials.<environment>.env relative to the repo root
2. configs/dag_publish/nexus_credentials.env relative to the repo root
3. configs/dag_publish/nexus_credentials.<environment>.env relative to the script directory
4. configs/dag_publish/nexus_credentials.env relative to the script directory
5. nexus_credentials.<environment>.env in the same directory as this script
6. nexus_credentials.env in the same directory as this script
"""

import argparse
import base64
import hashlib
import json
import os
import shlex
import shutil
import ssl
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib import error, parse, request
import zipfile

from cli_support import ScriptOutputSession, StepReporter, resolve_runtime_logging_settings
from common import (
    build_default_credentials_candidates,
    normalize_environment,
)
from deploy_steps import DeploymentError, SyntaxChecker, load_pipeline_config


SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_NEXUS_REPOSITORY_URL = (
    "https://nexus302.systems.uk.hsbc:8081/nexus/repository/raw-alm-uat_n3p"
)
DEFAULT_PATH_PREFIX = "com/hsbc/gdt/et/fctm/1646753/CHG123456"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_ARCHIVE_SEPARATOR = "."
SKIP_NAMES = {"__pycache__", ".DS_Store", ".git", ".idea"}
SKIP_SUFFIXES = {".pyc", ".pyo"}
SURROUNDING_QUOTE_CHARS = {'"', "'", "“", "”", "‘", "’"}


def parse_args(argv=None) -> argparse.Namespace:
    """Define the command line interface developers use to publish a DAG package."""
    parser = argparse.ArgumentParser(
        description="Package DAG files into a zip archive and upload it to Nexus.",
    )
    parser.add_argument(
        "sources",
        nargs="+",
        help="DAG files or directories to package.",
    )
    parser.add_argument(
        "--artifact-id",
        help="Artifact id used in the zip filename and default Nexus object name.",
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Artifact version used in the zip filename and default Nexus object name.",
    )
    parser.add_argument(
        "--environment",
        choices=("dev", "uat", "prod"),
        default="dev",
        help="Target environment used to resolve the default credentials file.",
    )
    parser.add_argument(
        "--credentials-file",
        help="Override the default environment-specific Nexus credentials file.",
    )
    parser.add_argument(
        "--config",
        help="Override the deployment pipeline config used for Airflow DAG validation.",
    )
    parser.add_argument(
        "--path-prefix",
        help="Override the default path prefix inside the Nexus Raw repository.",
    )
    parser.add_argument(
        "--repository-url",
        help="Override the full Nexus repository URL.",
    )
    parser.add_argument(
        "--base-url",
        help="Override the Nexus base URL, for example https://nexus.",
    )
    parser.add_argument(
        "--repository",
        help="Override the Nexus repository name, for example airflow-dags.",
    )
    parser.add_argument(
        "--upload-path",
        help="Custom path inside the Nexus repository. Defaults to <path-prefix>/<artifact-id>.<version>.zip.",
    )
    parser.add_argument(
        "--output-dir",
        default="build/dag_packages",
        help="Directory used to store the generated archive locally.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="HTTP timeout in seconds. Defaults to NEXUS_TIMEOUT_SECONDS or 60.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS certificate verification during upload.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Create the archive and print the upload target without sending it to Nexus.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print step-by-step debug details, including executed shell commands.",
    )
    return parser.parse_args(argv)


def load_properties(path):
    """Load a simple .env style file.

    The file is intentionally parsed with stdlib only so the script can run on
    a minimal RHEL8 host without extra Python packages.
    """
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"Credentials file does not exist: {path}")

    properties = {}  # type: Dict[str, str]
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # Allow both plain KEY=value and shell-friendly "export KEY=value".
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise ValueError(f"Invalid line {line_number} in credentials file: {raw_line}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        # Be forgiving when credentials files are edited in rich-text tools or
        # chat clients that auto-replace ASCII quotes with curly quotes.
        if len(value) >= 2 and value[0] in SURROUNDING_QUOTE_CHARS and value[-1] in SURROUNDING_QUOTE_CHARS:
            value = value[1:-1]
        properties[key] = value
    return properties


def resolve_credentials_file(explicit_path=None, environment=None):
    """Find the predefined credentials file from deployment-safe locations."""
    environment = normalize_environment(environment)
    candidates = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    candidates.extend(build_default_credentials_candidates(environment))

    for candidate in candidates:
        candidate_path = Path(candidate).expanduser().resolve()
        if candidate_path.is_file():
            return candidate_path

    checked_locations = "\n".join(
        "  - {0}".format(Path(candidate).expanduser().resolve())
        for candidate in candidates
    )
    raise ValueError(
        "Credentials file does not exist in any predefined location. Checked:\n{0}".format(
            checked_locations
        )
    )


def parse_bool(value, default=False):
    """Normalize boolean values coming from the credentials file."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def normalize_sources(raw_sources):
    """Convert input paths into absolute existing paths before packaging starts."""
    sources = []  # type: List[Path]
    for raw_source in raw_sources:
        source = Path(raw_source).expanduser().resolve()
        if not source.exists():
            raise ValueError(f"Source path does not exist: {raw_source}")
        sources.append(source)
    return sources


def derive_artifact_id(sources, explicit_artifact_id):
    """Pick a stable artifact name used in the zip filename and default Nexus path."""
    if explicit_artifact_id:
        return explicit_artifact_id
    if len(sources) != 1:
        raise ValueError("Please provide --artifact-id when packaging multiple sources.")
    source = sources[0]
    return source.stem if source.is_file() else source.name


def should_skip(path):
    """Skip editor cache files and Python bytecode from the package."""
    if any(part in SKIP_NAMES for part in path.parts):
        return True
    if path.name in SKIP_NAMES:
        return True
    if path.suffix.lower() in SKIP_SUFFIXES:
        return True
    return False


def iter_archive_entries(sources):
    """Build the final file list stored in the zip archive.

    Directory inputs keep their top-level folder name so a DAG directory like
    "dags/customer_sync" becomes "customer_sync/..."" inside the zip.
    """
    entries = []  # type: List[Tuple[Path, str]]
    seen_names = set()  # type: Set[str]
    for source in sources:
        base_dir = source.parent
        if source.is_file():
            archive_name = source.relative_to(base_dir).as_posix()
            if archive_name in seen_names:
                raise ValueError(f"Duplicate archive entry detected: {archive_name}")
            entries.append((source, archive_name))
            seen_names.add(archive_name)
            continue

        for candidate in sorted(source.rglob("*")):
            if not candidate.is_file() or should_skip(candidate):
                continue
            # Use POSIX-style paths in the zip so the archive layout is
            # consistent across Linux and macOS.
            archive_name = candidate.relative_to(base_dir).as_posix()
            if archive_name in seen_names:
                raise ValueError(f"Duplicate archive entry detected: {archive_name}")
            entries.append((candidate, archive_name))
            seen_names.add(archive_name)
    if not entries:
        raise ValueError("No files were found to package.")
    return entries


def validate_python_sources(sources):
    """Validate packaged Python files before building the archive."""
    python_files = [
        source_path
        for source_path, _archive_entry in iter_archive_entries(sources)
        if source_path.suffix.lower() == ".py"
    ]
    if not python_files:
        return
    SyntaxChecker().run(REPO_ROOT, python_files)


def debug_print(enabled, message):
    """Print a debug line when --debug is enabled."""
    if enabled:
        print("[DEBUG] {0}".format(message))


def format_command(command):
    """Format a command for debug output."""
    if isinstance(command, str):
        return command
    return " ".join(shlex.quote(str(part)) for part in command)


def stage_sources_for_airflow_check(sources, staging_root, debug=False):
    """Copy packaged sources into a temporary directory for Airflow DAG validation."""
    staging_root = Path(staging_root).expanduser().resolve()
    staged_files = 0
    for source_path, archive_entry in iter_archive_entries(sources):
        destination = Path(staging_root) / archive_entry
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source_path), str(destination))
        staged_files += 1
        debug_print(debug, "Staged file for Airflow validation: {0} -> {1}".format(source_path, destination))
    debug_print(debug, "Prepared Airflow validation staging directory with {0} files.".format(staged_files))
    return Path(staging_root)


def render_airflow_cli_env(raw_env, session_root):
    """Render per-run Airflow CLI environment overrides."""
    session_root_text = Path(session_root).expanduser().resolve().as_posix()
    rendered = {}
    for key, value in (raw_env or {}).items():
        if isinstance(value, bool):
            rendered[str(key)] = "True" if value else "False"
        elif value is None:
            rendered[str(key)] = ""
        else:
            rendered[str(key)] = str(value).replace("{session_root}", session_root_text)
    return rendered


def resolve_airflow_staging_root(dags_folder_value, session_root):
    """Resolve the effective staged DAGs folder for Airflow CLI validation."""
    staging_root = Path(str(dags_folder_value)).expanduser()
    if not staging_root.is_absolute():
        staging_root = (Path(session_root).expanduser().resolve() / staging_root).resolve()
    return staging_root.resolve()


def run_airflow_dag_validation(sources, environment, config_path=None, debug=False, input_fn=None):
    """Run advisory Airflow DAG validation against the staged package contents."""
    warning_message = ""
    debug_print(debug, "Loading deployment config for Airflow validation.")
    try:
        config = load_pipeline_config(explicit_path=config_path, environment=environment)
    except DeploymentError as exc:
        warning_message = (
            "Airflow DAG validation could not start because the deployment config "
            "could not be loaded.\n{0}".format(exc)
        )
    else:
        debug_print(debug, "Using deployment config: {0}".format(config.config_path))
        config.airflow_cli.temp_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="dag_package_airflow_check_",
            dir=str(config.airflow_cli.temp_root),
        ) as temp_dir:
            session_root = Path(temp_dir).resolve()
            environment_overrides = render_airflow_cli_env(
                config.airflow_cli.env,
                session_root=session_root,
            )
            staging_root = resolve_airflow_staging_root(
                environment_overrides["AIRFLOW__CORE__DAGS_FOLDER"],
                session_root=session_root,
            )
            stage_sources_for_airflow_check(
                sources,
                staging_root,
                debug=debug,
            )
            command = build_airflow_validation_command(config)
            debug_print(debug, "Airflow CLI temp root: {0}".format(config.airflow_cli.temp_root))
            debug_print(debug, "Airflow CLI session root: {0}".format(session_root))
            debug_print(debug, "Airflow validation command: {0}".format(format_command(command)))
            debug_print(debug, "Airflow validation environment overrides: {0}".format(environment_overrides))

            try:
                completed = execute_airflow_validation(
                    python_executable=config.imports.python_executable,
                    validation_command=command,
                    shell_executable=config.imports.shell_executable,
                    activation_command=config.imports.activation_command,
                    timeout_seconds=config.imports.timeout_seconds,
                    environment_overrides=environment_overrides,
                    debug=debug,
                )
            except DeploymentError as exc:
                warning_message = (
                    "Airflow DAG validation could not be completed.\n{0}".format(exc)
                )
            else:
                warning_message = interpret_airflow_validation_result(completed)

    if warning_message:
        confirm_continue_after_validation_issue(
            heading="Airflow DAG validation reported issues.",
            message=warning_message,
            input_fn=input_fn,
            debug=debug,
        )
    else:
        debug_print(debug, "Airflow DAG validation completed without import errors.")


def build_airflow_validation_command(config):
    """Build the Airflow CLI command used for package-time DAG validation."""
    return [
        config.imports.python_executable,
        "-m",
        "airflow",
        "dags",
        "list-import-errors",
        "-l",
        "-o",
        "json",
    ]


def build_airflow_db_migrate_command(python_executable):
    """Build the Airflow CLI command used to initialize the temporary metadata DB."""
    return [
        python_executable,
        "-m",
        "airflow",
        "db",
        "migrate",
    ]


def execute_airflow_validation(
    python_executable,
    validation_command,
    shell_executable,
    activation_command,
    timeout_seconds,
    environment_overrides,
    debug=False,
):
    """Execute Airflow DB init plus DAG validation inside the configured Python environment."""
    migrate_command = build_airflow_db_migrate_command(python_executable)
    debug_print(debug, "Airflow DB migrate command: {0}".format(format_command(migrate_command)))
    migrate_result = execute_airflow_command(
        command=migrate_command,
        shell_executable=shell_executable,
        activation_command=activation_command,
        timeout_seconds=timeout_seconds,
        environment_overrides=environment_overrides,
        debug=debug,
    )
    if migrate_result.returncode != 0:
        raise DeploymentError(build_airflow_command_failure_message(migrate_result, "Airflow DB migrate command"))

    return execute_airflow_command(
        command=validation_command,
        shell_executable=shell_executable,
        activation_command=activation_command,
        timeout_seconds=timeout_seconds,
        environment_overrides=environment_overrides,
        debug=debug,
    )


def execute_airflow_command(
    command,
    shell_executable,
    activation_command,
    timeout_seconds,
    environment_overrides,
    debug=False,
):
    """Execute one Airflow-related command inside the configured Python environment."""
    env = os.environ.copy()
    env.update(environment_overrides)

    try:
        if activation_command:
            shell_command = "{0} && {1}".format(
                activation_command,
                format_command(command),
            )
            debug_print(debug, "Executing shell command: {0}".format(shell_command))
            return subprocess.run(
                [shell_executable, "-lc", shell_command],
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout_seconds,
                check=False,
            )
        debug_print(debug, "Executing command: {0}".format(format_command(command)))
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise DeploymentError(
            "Airflow DAG validation timed out after {0} seconds.".format(timeout_seconds)
        ) from exc
    except OSError as exc:
        raise DeploymentError(
            "Airflow DAG validation could not start the configured Python environment: {0}".format(
                exc
            )
        ) from exc


def interpret_airflow_validation_result(completed):
    """Interpret the Airflow CLI result and return a warning message when issues are found."""
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()

    if not stdout:
        if completed.returncode != 0:
            return build_airflow_command_failure_message(
                completed,
                "Airflow DAG validation command",
            )
        return ""

    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        if completed.returncode != 0:
            return build_airflow_command_failure_message(completed, "Airflow DAG validation command")
        message = "Airflow DAG validation returned non-JSON output."
        if stdout:
            message = "{0}\nSTDOUT:\n{1}".format(message, stdout)
        if stderr:
            message = "{0}\nSTDERR:\n{1}".format(message, stderr)
        return message

    if isinstance(parsed, dict):
        parsed = parsed.get("import_errors") or parsed.get("errors") or [parsed]

    if not parsed:
        if completed.returncode != 0:
            return build_airflow_command_failure_message(
                completed,
                "Airflow DAG validation command",
            )
        return ""

    lines = ["Airflow reported DAG import errors:"]
    for index, item in enumerate(parsed, start=1):
        if isinstance(item, dict):
            filepath = (
                item.get("filepath")
                or item.get("file_path")
                or item.get("filename")
                or "<unknown file>"
            )
            error_detail = (
                item.get("error")
                or item.get("import_error")
                or item.get("stacktrace")
                or json.dumps(item, ensure_ascii=False)
            )
            lines.append("{0}. {1}".format(index, filepath))
            lines.append("   {0}".format(str(error_detail).replace("\n", "\n   ")))
        else:
            lines.append("{0}. {1}".format(index, item))
    if stderr:
        lines.append("STDERR:")
        lines.append(stderr)
    return "\n".join(lines)


def build_airflow_command_failure_message(completed, label):
    """Build a readable warning when an Airflow CLI command itself fails."""
    lines = [
        "{0} failed with exit code {1}.".format(
            label,
            completed.returncode
        )
    ]
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if stdout:
        lines.append("STDOUT:")
        lines.append(stdout)
    if stderr:
        lines.append("STDERR:")
        lines.append(stderr)
    return "\n".join(lines)


def prompt_user_to_continue(input_fn=None):
    """Prompt the user to continue after an advisory validation warning."""
    if input_fn is None:
        input_fn = input
    try:
        return input_fn("⚠️ Type 'go' to ignore these issues and continue packaging: ").strip().lower()
    except EOFError as exc:
        raise DeploymentError(
            "Validation issues were reported and interactive confirmation was not available."
        ) from exc


def confirm_continue_after_validation_issue(heading, message, input_fn=None, debug=False):
    """Show a validation issue and continue only when the user types 'go'."""
    print("⚠️ {0}".format(heading), file=sys.stderr)
    print(message, file=sys.stderr)
    response = prompt_user_to_continue(input_fn=input_fn)
    debug_print(debug, "User confirmation response after validation warning: {0}".format(response))
    if response != "go":
        raise DeploymentError(
            "Packaging aborted by user after validation issues were reported."
        )


def build_archive_name(artifact_id, version):
    """Build the final zip filename.

    The company convention in Nexus uses dots between the logical artifact name
    and the release version, for example:
    DAG_ID_RELEASE.0001.4972.user_name.zip
    """
    return artifact_id + DEFAULT_ARCHIVE_SEPARATOR + version + ".zip"


def create_archive(
    sources,
    output_dir,
    archive_name,
):
    """Create the local zip file and calculate its SHA256 for traceability."""
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / archive_name
    sha256 = hashlib.sha256()

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source_path, archive_entry in iter_archive_entries(sources):
            archive.write(source_path, archive_entry)

    with archive_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha256.update(chunk)

    return archive_path, sha256.hexdigest()


def normalize_path_prefix(path_prefix):
    """Accept either slash paths or legacy dot notation and normalize to a path."""
    if "/" in path_prefix:
        parts = path_prefix.split("/")
    else:
        parts = path_prefix.replace(".", "/").split("/")
    return "/".join(part for part in parts if part)


def build_default_upload_path(path_prefix, archive_name):
    """Create the default object path in the Nexus Raw repository.

    Example:
    com/hsbc/gdt/et/fctm/1646753/CHG123456 + DAG_ID_RELEASE.0001.4972.user_name.zip
    -> com/hsbc/gdt/et/fctm/1646753/CHG123456/DAG_ID_RELEASE.0001.4972.user_name.zip
    """
    normalized_prefix = normalize_path_prefix(path_prefix)
    parts = [part for part in (normalized_prefix, archive_name) if part]
    return "/".join(parts)


def resolve_repository_url(args, properties):
    """Resolve the target Nexus repository URL from CLI first, then config file."""
    if args.repository_url:
        return args.repository_url.rstrip("/")
    if properties.get("NEXUS_REPOSITORY_URL"):
        return properties["NEXUS_REPOSITORY_URL"].rstrip("/")

    base_url = args.base_url or properties.get("NEXUS_BASE_URL")
    repository = args.repository or properties.get("NEXUS_REPOSITORY")
    if not base_url or not repository:
        return DEFAULT_NEXUS_REPOSITORY_URL
    return f"{base_url.rstrip('/')}/repository/{repository.strip('/')}"


def build_upload_url(repository_url, upload_path):
    """Join the repository root and the target object path safely."""
    return f"{repository_url.rstrip('/')}/{parse.quote(upload_path.lstrip('/'), safe='/')}"


def upload_archive(
    upload_url,
    archive_path,
    username,
    password,
    timeout,
    insecure,
):
    """Upload the generated zip with a simple HTTP PUT request.

    This matches the common upload pattern for Nexus Raw repositories.
    """
    auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    payload = archive_path.read_bytes()
    http_request = request.Request(
        upload_url,
        data=payload,
        method="PUT",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/zip",
            "Content-Length": str(len(payload)),
        },
    )

    # Some internal Nexus environments use a private CA. Keep strict TLS by
    # default, and only allow skipping verification when the operator asks for it.
    ssl_context = ssl._create_unverified_context() if insecure else None
    try:
        with request.urlopen(http_request, timeout=timeout, context=ssl_context) as response:
            if response.status >= 400:
                raise RuntimeError(f"Nexus upload failed with HTTP {response.status}")
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace").strip()
        message = f"Nexus upload failed with HTTP {exc.code}"
        if error_body:
            message = f"{message}: {error_body}"
        raise RuntimeError(message) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Unable to reach Nexus: {exc.reason}") from exc


def _run_with_args(args, reporter=None):
    """Main flow: read config, package sources, print target, then upload."""
    reporter = reporter or StepReporter(enabled=False)

    debug_print(args.debug, "Starting package_and_upload_dag.")
    environment = normalize_environment(args.environment)
    debug_print(args.debug, "Selected environment: {0}".format(environment))

    reporter.section("🧭", "Initialize Package Upload")
    credentials_file = resolve_credentials_file(
        explicit_path=args.credentials_file,
        environment=environment,
    )
    debug_print(args.debug, "Resolved credentials file: {0}".format(credentials_file))
    properties = load_properties(credentials_file)
    sources = normalize_sources(args.sources)
    debug_print(args.debug, "Normalized {0} source path(s).".format(len(sources)))
    reporter.message("✅", "Environment and credentials are ready.")

    reporter.section("🔍", "Run Python Syntax Validation")
    debug_print(args.debug, "Running Python syntax validation for package sources.")
    syntax_issue_detected = False
    try:
        validate_python_sources(sources)
    except DeploymentError as exc:
        syntax_issue_detected = True
        confirm_continue_after_validation_issue(
            heading="Python syntax validation reported issues.",
            message=str(exc),
            debug=args.debug,
        )
        reporter.message("⚠️", "Python syntax validation issues were overridden by operator input.")
    else:
        reporter.message("✅", "Python syntax validation passed.")

    reporter.section("🐍", "Run Airflow CLI Validation")
    if syntax_issue_detected:
        reporter.message("⏭️", "Airflow CLI validation was skipped because syntax issues were overridden.")
    else:
        debug_print(args.debug, "Running advisory Airflow DAG validation.")
        run_airflow_dag_validation(
            sources=sources,
            environment=environment,
            config_path=args.config,
            debug=args.debug,
        )
        reporter.message("✅", "Airflow CLI validation finished.")

    artifact_id = derive_artifact_id(sources, args.artifact_id)
    version = args.version.strip()
    if not version:
        raise ValueError("--version cannot be empty.")

    repository_url = resolve_repository_url(args, properties)
    username = properties.get("NEXUS_USERNAME")
    password = properties.get("NEXUS_PASSWORD")
    if not username or not password:
        raise ValueError("NEXUS_USERNAME and NEXUS_PASSWORD are required in the credentials file.")

    path_prefix = (
        args.path_prefix
        or properties.get("NEXUS_PATH_PREFIX")
        or properties.get("NEXUS_GROUP_ID")
        or DEFAULT_PATH_PREFIX
    )
    timeout = args.timeout or int(properties.get("NEXUS_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
    insecure = args.insecure or parse_bool(properties.get("NEXUS_INSECURE"), default=False)

    reporter.section("📦", "Create Archive")
    archive_name = build_archive_name(artifact_id, version)
    output_dir = Path(args.output_dir).expanduser().resolve()
    debug_print(args.debug, "Creating archive {0} in {1}".format(archive_name, output_dir))
    archive_path, archive_sha256 = create_archive(sources, output_dir, archive_name)
    reporter.message("✅", "Archive build completed.")

    reporter.section("☁️", "Prepare Nexus Upload")
    upload_path = args.upload_path or build_default_upload_path(path_prefix, archive_name)
    upload_url = build_upload_url(repository_url, upload_path)
    debug_print(args.debug, "Resolved upload URL: {0}".format(upload_url))

    if args.dry_run:
        debug_print(args.debug, "Dry-run enabled; upload step skipped.")
    else:
        reporter.section("🚀", "Upload Archive To Nexus")
        debug_print(args.debug, "Uploading archive to Nexus.")
        upload_archive(upload_url, archive_path, username, password, timeout, insecure)

    return {
        "archive_path": archive_path,
        "archive_sha256": archive_sha256,
        "environment": environment,
        "credentials_file": credentials_file,
        "upload_url": upload_url,
        "dry_run": args.dry_run,
        "uploaded": not args.dry_run,
    }


def main(argv=None):
    """CLI entry point."""
    args = parse_args(argv)
    logging_settings = resolve_runtime_logging_settings(
        explicit_config_path=args.config,
        environment=args.environment,
    )

    try:
        with ScriptOutputSession(
            script_name="package_and_upload_dag",
            log_directory=logging_settings.directory,
            retention_days=logging_settings.retention_days,
        ):
            reporter = StepReporter(enabled=True)
            result = _run_with_args(args, reporter=reporter)

            reporter.section("📋", "Package Upload Summary")
            reporter.value("📦", "Archive created", result["archive_path"])
            reporter.value("🌍", "Environment", result["environment"])
            reporter.value("🔐", "Credentials file", result["credentials_file"])
            reporter.value("🧮", "SHA256", result["archive_sha256"])
            reporter.value("🔗", "Upload target", result["upload_url"])

            if result["dry_run"]:
                reporter.message("🧪", "Dry run enabled. Upload skipped.")
            else:
                reporter.message("✅", "Upload completed successfully.")
            return 0
    except (DeploymentError, RuntimeError, ValueError, OSError) as exc:
        print(f"❌ Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
