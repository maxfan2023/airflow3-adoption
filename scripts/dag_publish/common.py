#!/usr/bin/env python3
"""Shared helpers for DAG publishing scripts."""

from pathlib import Path
from typing import Dict


SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
REPO_ROOT = SCRIPT_DIR.parents[1]
SUPPORTED_ENVIRONMENTS = ("dev", "uat", "prod")
DEFAULT_ENVIRONMENT = "dev"
SURROUNDING_QUOTE_CHARS = {'"', "'", "“", "”", "‘", "’"}


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
