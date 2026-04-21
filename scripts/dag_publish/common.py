#!/usr/bin/env python3
"""Shared helpers for DAG publishing scripts."""

from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
from typing import Dict


SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
REPO_ROOT = SCRIPT_DIR.parents[1]
SUPPORTED_ENVIRONMENTS = ("dev", "uat", "prod")
DEFAULT_ENVIRONMENT = "dev"
SURROUNDING_QUOTE_CHARS = {'"', "'", "“", "”", "‘", "’"}
DEFAULT_BUNDLE_PREFIX = "com/hsbc/gdt/et/fctm/bundles"
CHANGE_TICKET_PATTERN = re.compile(r"(CHG[0-9A-Za-z_-]+)")


def load_properties(path):
    """Load a simple .env style file without extra dependencies."""
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise ValueError("Credentials file does not exist: {0}".format(path))

    properties = {}  # type: Dict[str, str]
    lines = path.read_text(encoding="utf-8").splitlines()
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise ValueError(
                "Invalid line {0} in credentials file: {1}".format(line_number, raw_line)
            )
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] in SURROUNDING_QUOTE_CHARS and value[-1] in SURROUNDING_QUOTE_CHARS:
            value = value[1:-1]
        properties[key] = value
    return properties


def normalize_environment(environment=None):
    """Normalize and validate the deployment environment name."""
    normalized = str(environment or DEFAULT_ENVIRONMENT).strip().lower()
    if normalized not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            "Unsupported environment '{0}'. Expected one of: {1}".format(
                normalized,
                ", ".join(SUPPORTED_ENVIRONMENTS),
            )
        )
    return normalized


def resolve_credentials_file(explicit_path=None, environment=None):
    """Find the credentials file from explicit input or safe default locations."""
    environment = normalize_environment(environment)
    candidates = []  # type: list[Path]
    if explicit_path:
        candidates.append(Path(explicit_path))
    candidates.extend(build_default_credentials_candidates(environment))

    checked = []  # type: list[str]
    for candidate in candidates:
        candidate_path = Path(candidate).expanduser().resolve()
        checked.append(str(candidate_path))
        if candidate_path.is_file():
            return candidate_path

    checked_locations = "\n".join("  - {0}".format(item) for item in checked)
    raise ValueError(
        "Credentials file does not exist in any predefined location. Checked:\n{0}".format(
            checked_locations
        )
    )


def parse_bool(value, default=False):
    """Normalize booleans read from config files."""
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def resolve_path(value, base_dir):
    """Resolve a path relative to the given base directory."""
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path(base_dir).expanduser().resolve() / path).resolve()


def build_repository_object_url(repository_url, artifact_path):
    """Build a repository object URL from the repository root and object path."""
    from urllib import parse

    return "{0}/{1}".format(
        repository_url.rstrip("/"),
        parse.quote(str(artifact_path).lstrip("/"), safe="/"),
    )


def normalize_repo_path(value):
    """Normalize repository object paths to slash-separated form."""
    text = str(value or "").strip()
    if not text:
        return ""
    if "/" in text:
        parts = text.split("/")
    else:
        parts = text.replace(".", "/").split("/")
    return "/".join(part for part in parts if part)


def build_bundle_base_path(environment, bundle_name, root_prefix=DEFAULT_BUNDLE_PREFIX):
    """Build the Nexus repository path prefix for bundle metadata."""
    environment = normalize_environment(environment)
    normalized_root = normalize_repo_path(root_prefix)
    bundle_name = str(bundle_name).strip()
    if not bundle_name:
        raise ValueError("bundle_name cannot be empty.")
    return "/".join(part for part in (normalized_root, environment, bundle_name) if part)


def build_latest_manifest_path(environment, bundle_name, root_prefix=DEFAULT_BUNDLE_PREFIX):
    """Build the latest manifest path for a bundle."""
    return "{0}/latest.json".format(build_bundle_base_path(environment, bundle_name, root_prefix=root_prefix))


def build_release_record_path(environment, bundle_name, released_at, root_prefix=DEFAULT_BUNDLE_PREFIX):
    """Build the immutable release record path for a bundle release."""
    return "{0}/releases/{1}.json".format(
        build_bundle_base_path(environment, bundle_name, root_prefix=root_prefix),
        str(released_at).strip(),
    )


def build_version_record_path(environment, bundle_name, version, root_prefix=DEFAULT_BUNDLE_PREFIX):
    """Build the immutable version lookup path for a bundle release."""
    version = str(version or "").strip()
    if not version:
        raise ValueError("version cannot be empty.")
    return "{0}/versions/{1}.json".format(
        build_bundle_base_path(environment, bundle_name, root_prefix=root_prefix),
        version,
    )


def infer_change_ticket(*values):
    """Extract a change ticket like CHG123456 from one or more candidate strings."""
    for value in values:
        if not value:
            continue
        match = CHANGE_TICKET_PATTERN.search(str(value))
        if match:
            return match.group(1)
    return ""


def utc_now_text():
    """Return the current UTC timestamp in an RFC3339-like format suitable for metadata."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_git_commit(explicit_value=None, cwd=None):
    """Resolve a git commit SHA, preferring explicit input and then the current repository HEAD."""
    if explicit_value:
        return str(explicit_value).strip()
    try:
        process = subprocess.Popen(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd or REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError:
        return ""
    stdout, _stderr = process.communicate()
    if process.returncode != 0:
        return ""
    return str(stdout).strip()


def build_default_credentials_candidates(environment):
    """Build environment-aware credentials search paths."""
    environment = normalize_environment(environment)
    return [
        REPO_ROOT / "configs" / "dag_publish" / "nexus_credentials.{0}.env".format(environment),
        REPO_ROOT / "configs" / "dag_publish" / "nexus_credentials.env",
        SCRIPT_DIR / "configs" / "dag_publish" / "nexus_credentials.{0}.env".format(environment),
        SCRIPT_DIR / "configs" / "dag_publish" / "nexus_credentials.env",
        SCRIPT_DIR / "nexus_credentials.{0}.env".format(environment),
        SCRIPT_DIR / "nexus_credentials.env",
    ]


def build_default_pipeline_config_candidates(environment):
    """Build environment-aware deployment config search paths."""
    environment = normalize_environment(environment)
    return [
        REPO_ROOT / "configs" / "dag_publish" / "deploy_pipeline.{0}.json".format(environment),
        REPO_ROOT / "configs" / "dag_publish" / "deploy_pipeline.json",
        SCRIPT_DIR / "configs" / "dag_publish" / "deploy_pipeline.{0}.json".format(environment),
        SCRIPT_DIR / "configs" / "dag_publish" / "deploy_pipeline.json",
    ]
