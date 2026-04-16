"""Configuration loading for the DAG deployment pipeline."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from common import build_default_pipeline_config_candidates, normalize_environment, resolve_path
from deploy_steps.exceptions import DeploymentError


@dataclass
class PathSettings:
    working_root: Path
    landing_root: Path
    dags_root: Path
    backup_root: Path


@dataclass
class NexusSettings:
    repository_url: str
    timeout_seconds: int
    verify_tls: bool


@dataclass
class ArchiveSettings:
    allowed_suffixes: List[str]
    require_single_top_level_dir: bool


@dataclass
class ChecksumSettings:
    mode: str
    sidecar_suffix: str


@dataclass
class ImportSettings:
    extra_pythonpath: List[Path] = field(default_factory=list)
    shell_executable: str = "/bin/bash"
    activation_command: str = ""
    python_executable: str = "python"
    timeout_seconds: int = 300


@dataclass
class TaggingSettings:
    source_variable_name: str
    managed_tags: List[str]
    us_sources: List[str]
    us_tag: str
    global_tag: str


@dataclass
class RegexRuleSettings:
    enabled: bool
    allow_patterns: List[str] = field(default_factory=list)
    deny_patterns: List[str] = field(default_factory=list)


@dataclass
class RulesSettings:
    name_rules: RegexRuleSettings
    queue_rules: RegexRuleSettings


@dataclass
class PipelineConfig:
    config_path: Path
    environment: str
    paths: PathSettings
    nexus: NexusSettings
    archive: ArchiveSettings
    checksum: ChecksumSettings
    imports: ImportSettings
    tagging: TaggingSettings
    rules: RulesSettings


def resolve_config_file(explicit_path=None, environment=None):
    """Find the deployment config file."""
    environment = normalize_environment(environment)
    candidates = []  # type: List[Path]
    if explicit_path:
        candidates.append(Path(explicit_path))
    candidates.extend(build_default_pipeline_config_candidates(environment))

    checked = []  # type: List[str]
    for candidate in candidates:
        candidate_path = Path(candidate).expanduser().resolve()
        checked.append(str(candidate_path))
        if candidate_path.is_file():
            return candidate_path

    raise DeploymentError(
        "Deployment config file does not exist. Checked:\n{0}".format(
            "\n".join("  - {0}".format(item) for item in checked)
        )
    )


def load_pipeline_config(explicit_path=None, working_root_override=None, environment=None):
    """Load and normalize pipeline configuration from JSON."""
    environment = normalize_environment(environment)
    config_path = resolve_config_file(explicit_path, environment=environment)
    base_dir = config_path.parent
    raw = json.loads(config_path.read_text(encoding="utf-8"))

    paths_raw = raw.get("paths") or {}
    nexus_raw = raw.get("nexus") or {}
    archive_raw = raw.get("archive") or {}
    checksum_raw = raw.get("checksum") or {}
    imports_raw = raw.get("imports") or {}
    tagging_raw = raw.get("tagging") or {}
    rules_raw = raw.get("rules") or {}

    if working_root_override:
        working_root = resolve_path(working_root_override, base_dir)
    else:
        working_root = resolve_path(_require_key(paths_raw, "working_root"), base_dir)

    config = PipelineConfig(
        config_path=config_path,
        environment=environment,
        paths=PathSettings(
            working_root=working_root,
            landing_root=resolve_path(_require_key(paths_raw, "landing_root"), base_dir),
            dags_root=resolve_path(_require_key(paths_raw, "dags_root"), base_dir),
            backup_root=resolve_path(_require_key(paths_raw, "backup_root"), base_dir),
        ),
        nexus=NexusSettings(
            repository_url=str(_require_key(nexus_raw, "repository_url")).rstrip("/"),
            timeout_seconds=int(_require_key(nexus_raw, "timeout_seconds")),
            verify_tls=bool(_require_key(nexus_raw, "verify_tls")),
        ),
        archive=ArchiveSettings(
            allowed_suffixes=[str(item) for item in _require_key(archive_raw, "allowed_suffixes")],
            require_single_top_level_dir=bool(
                _require_key(archive_raw, "require_single_top_level_dir")
            ),
        ),
        checksum=ChecksumSettings(
            mode=str(_require_key(checksum_raw, "mode")),
            sidecar_suffix=str(checksum_raw.get("sidecar_suffix", ".sha256")),
        ),
        imports=ImportSettings(
            extra_pythonpath=[
                resolve_path(item, base_dir)
                for item in imports_raw.get("extra_pythonpath", [])
            ],
            shell_executable=str(imports_raw.get("shell_executable", "/bin/bash")),
            activation_command=str(imports_raw.get("activation_command", "")).strip(),
            python_executable=str(imports_raw.get("python_executable", "python")).strip() or "python",
            timeout_seconds=int(imports_raw.get("timeout_seconds", 300)),
        ),
        tagging=TaggingSettings(
            source_variable_name=str(
                tagging_raw.get("source_variable_name", "source")
            ),
            managed_tags=[str(item) for item in _require_key(tagging_raw, "managed_tags")],
            us_sources=[str(item) for item in _require_key(tagging_raw, "us_sources")],
            us_tag=str(_require_key(tagging_raw, "us_tag")),
            global_tag=str(_require_key(tagging_raw, "global_tag")),
        ),
        rules=RulesSettings(
            name_rules=_build_rule_settings(rules_raw.get("name_rules") or {}),
            queue_rules=_build_rule_settings(rules_raw.get("queue_rules") or {}),
        ),
    )
    _validate_config(config)
    return config


def _build_rule_settings(raw_rule):
    return RegexRuleSettings(
        enabled=bool(raw_rule.get("enabled", False)),
        allow_patterns=[str(item) for item in raw_rule.get("allow_patterns", [])],
        deny_patterns=[str(item) for item in raw_rule.get("deny_patterns", [])],
    )


def _require_key(mapping, key):
    if key not in mapping:
        raise DeploymentError("Missing required config key: {0}".format(key))
    return mapping[key]


def _validate_config(config):
    valid_modes = {"compute_only", "sidecar_file", "cli_value"}
    if config.checksum.mode not in valid_modes:
        raise DeploymentError(
            "Unsupported checksum mode '{0}'. Expected one of: {1}".format(
                config.checksum.mode,
                ", ".join(sorted(valid_modes)),
            )
        )

    if not config.archive.allowed_suffixes:
        raise DeploymentError("archive.allowed_suffixes cannot be empty.")

    if config.tagging.us_tag not in config.tagging.managed_tags:
        raise DeploymentError("tagging.us_tag must be part of tagging.managed_tags.")
    if config.tagging.global_tag not in config.tagging.managed_tags:
        raise DeploymentError("tagging.global_tag must be part of tagging.managed_tags.")
    if config.imports.timeout_seconds <= 0:
        raise DeploymentError("imports.timeout_seconds must be greater than zero.")
