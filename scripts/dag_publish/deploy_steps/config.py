"""Configuration loading for the DAG deployment pipeline."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from common import (
    DEFAULT_BUNDLE_PREFIX,
    build_default_pipeline_config_candidates,
    normalize_environment,
    parse_bool,
    resolve_path,
)
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
class LoggingSettings:
    directory: Path
    retention_days: int = 14


@dataclass
class BundleSettings:
    metadata_root_prefix: str
    cache_root: Path


@dataclass
class AirflowCliSettings:
    temp_root: Path
    env: Dict[str, object] = field(default_factory=dict)


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
class DagVariableRuleSettings:
    name: str
    required: bool = True
    allowed_values: List[str] = field(default_factory=list)


@dataclass
class RulesSettings:
    name_rules: RegexRuleSettings
    queue_rules: RegexRuleSettings
    dag_variable_rules: List[DagVariableRuleSettings] = field(default_factory=list)


@dataclass
class PipelineConfig:
    config_path: Path
    environment: str
    paths: PathSettings
    nexus: NexusSettings
    archive: ArchiveSettings
    checksum: ChecksumSettings
    logging: LoggingSettings
    bundle: BundleSettings
    airflow_cli: AirflowCliSettings
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
    logging_raw = raw.get("logging") or {}
    bundle_raw = raw.get("bundle") or {}
    airflow_cli_raw = raw.get("airflow_cli") or {}
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
        logging=LoggingSettings(
            directory=resolve_path(
                logging_raw.get("directory", str(working_root.parent / "logs")),
                base_dir,
            ),
            retention_days=int(logging_raw.get("retention_days", 14)),
        ),
        bundle=BundleSettings(
            metadata_root_prefix=str(
                bundle_raw.get("metadata_root_prefix", DEFAULT_BUNDLE_PREFIX)
            ).strip(),
            cache_root=resolve_path(
                bundle_raw.get("cache_root", "/FCR_APP/abinitio/airflow/v3/dag_bundle_cache"),
                base_dir,
            ),
        ),
        airflow_cli=_build_airflow_cli_settings(
            airflow_cli_raw=airflow_cli_raw,
            working_root=working_root,
            base_dir=base_dir,
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
            dag_variable_rules=_build_dag_variable_rules(
                rules_raw.get("dag_variable_rules") or []
            ),
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


def _build_dag_variable_rules(raw_rules):
    rules = []
    for item in raw_rules:
        rules.append(
            DagVariableRuleSettings(
                name=str(_require_key(item, "name")).strip(),
                required=bool(item.get("required", True)),
                allowed_values=[str(value) for value in item.get("allowed_values", [])],
            )
        )
    return rules


def _build_airflow_cli_settings(airflow_cli_raw, working_root, base_dir):
    default_env = {
        "AIRFLOW__CORE__DAGS_FOLDER": "{session_root}/staging",
        "AIRFLOW__CORE__LOAD_EXAMPLES": False,
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:///{session_root}/airflow_metadata.db",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    custom_env = airflow_cli_raw.get("env") or {}
    merged_env = dict(default_env)
    for key, value in custom_env.items():
        merged_env[str(key)] = value

    return AirflowCliSettings(
        temp_root=resolve_path(
            airflow_cli_raw.get("temp_root", str(working_root.parent / "airflow_cli")),
            base_dir,
        ),
        env=merged_env,
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
    if config.logging.retention_days <= 0:
        raise DeploymentError("logging.retention_days must be greater than zero.")
    if not str(config.bundle.metadata_root_prefix or "").strip():
        raise DeploymentError("bundle.metadata_root_prefix cannot be empty.")
    if "AIRFLOW__CORE__DAGS_FOLDER" not in config.airflow_cli.env:
        raise DeploymentError("airflow_cli.env must include AIRFLOW__CORE__DAGS_FOLDER.")
    if "AIRFLOW__CORE__LOAD_EXAMPLES" not in config.airflow_cli.env:
        raise DeploymentError("airflow_cli.env must include AIRFLOW__CORE__LOAD_EXAMPLES.")
    if "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in config.airflow_cli.env:
        raise DeploymentError("airflow_cli.env must include AIRFLOW__DATABASE__SQL_ALCHEMY_CONN.")
    config.airflow_cli.env["AIRFLOW__CORE__LOAD_EXAMPLES"] = _stringify_env_value(
        config.airflow_cli.env["AIRFLOW__CORE__LOAD_EXAMPLES"]
    )
    config.airflow_cli.env["PYTHONDONTWRITEBYTECODE"] = _stringify_env_value(
        config.airflow_cli.env.get("PYTHONDONTWRITEBYTECODE", "1")
    )
    if config.imports.timeout_seconds <= 0:
        raise DeploymentError("imports.timeout_seconds must be greater than zero.")
    seen_rule_names = set()
    for rule in config.rules.dag_variable_rules:
        if not rule.name:
            raise DeploymentError("rules.dag_variable_rules[].name cannot be empty.")
        if rule.name in seen_rule_names:
            raise DeploymentError(
                "rules.dag_variable_rules contains duplicate variable name '{0}'.".format(
                    rule.name
                )
            )
        seen_rule_names.add(rule.name)


def _stringify_env_value(value):
    if isinstance(value, bool):
        return "True" if value else "False"
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"true", "false", "yes", "no", "on", "off", "1", "0"}:
        return "True" if parse_bool(text, default=False) else "False"
    return text
