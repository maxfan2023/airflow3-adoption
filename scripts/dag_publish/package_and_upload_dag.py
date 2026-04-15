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

The script always reads Nexus credentials from:
configs/dag_publish/nexus_credentials.env
"""

import argparse
import base64
import hashlib
import ssl
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib import error, parse, request
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CREDENTIALS_FILE = REPO_ROOT / "configs" / "dag_publish" / "nexus_credentials.env"
DEFAULT_NEXUS_REPOSITORY_URL = (
    "https://nexus302.systems.uk.hsbc:8081/nexus/repository/raw-alm-uat_n3p"
)
DEFAULT_PATH_PREFIX = "com/hsbc/gdt/et/fctm/1646753/CHG123456"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_ARCHIVE_SEPARATOR = "."
SKIP_NAMES = {"__pycache__", ".DS_Store", ".git", ".idea"}
SKIP_SUFFIXES = {".pyc", ".pyo"}


def parse_args() -> argparse.Namespace:
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
    return parser.parse_args()


def load_properties(path):
    """Load a simple .env style file.

    The file is intentionally parsed with stdlib only so the script can run on
    a minimal RHEL8 host without extra Python packages.
    """
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
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        properties[key] = value
    return properties


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


def main():
    """Main flow: read config, package sources, print target, then upload."""
    args = parse_args()

    try:
        credentials_file = DEFAULT_CREDENTIALS_FILE
        properties = load_properties(credentials_file)
        sources = normalize_sources(args.sources)
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

        # The archive name is the release artifact developers will see in Nexus.
        archive_name = build_archive_name(artifact_id, version)
        output_dir = Path(args.output_dir).expanduser().resolve()
        archive_path, archive_sha256 = create_archive(sources, output_dir, archive_name)

        # When no custom upload path is given, publish to a predictable path so
        # downstream deployment pipelines can fetch by artifact coordinates.
        upload_path = args.upload_path or build_default_upload_path(path_prefix, archive_name)
        upload_url = build_upload_url(repository_url, upload_path)

        print(f"Archive created: {archive_path}")
        print(f"SHA256: {archive_sha256}")
        print(f"Upload target: {upload_url}")

        if args.dry_run:
            print("Dry run enabled. Upload skipped.")
            return 0

        upload_archive(upload_url, archive_path, username, password, timeout, insecure)
        print("Upload completed successfully.")
        return 0
    except (RuntimeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
