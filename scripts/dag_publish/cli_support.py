#!/usr/bin/env python3
"""Shared CLI output and logging helpers for DAG publish scripts."""

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from common import REPO_ROOT, normalize_environment


DEFAULT_LOG_RETENTION_DAYS = 14


@dataclass
class RuntimeLoggingSettings:
    directory: Path
    retention_days: int
    config_path: Optional[Path] = None
    used_fallback: bool = False


class TeeStream:
    """Write output to the terminal and a log file at the same time."""

    def __init__(self, primary_stream, log_stream):
        self.primary_stream = primary_stream
        self.log_stream = log_stream

    def write(self, data):
        if not data:
            return 0
        self.primary_stream.write(data)
        self.log_stream.write(data)
        return len(data)

    def flush(self):
        self.primary_stream.flush()
        self.log_stream.flush()

    def __getattr__(self, name):
        return getattr(self.primary_stream, name)


class ScriptOutputSession:
    """Mirror stdout/stderr to per-run log files."""

    def __init__(self, script_name, log_directory, retention_days):
        self.script_name = str(script_name)
        self.log_directory = Path(log_directory).expanduser().resolve()
        self.retention_days = int(retention_days)
        self.stdout_log_path = None
        self.stderr_log_path = None
        self._stdout_handle = None
        self._stderr_handle = None
        self._original_stdout = None
        self._original_stderr = None

    def __enter__(self):
        self.log_directory.mkdir(parents=True, exist_ok=True)
        cleanup_log_directory(self.log_directory, self.retention_days)

        run_suffix = "{0}-{1}".format(_utc_timestamp(), os.getpid())
        self.stdout_log_path = self.log_directory / "{0}-{1}.log".format(
            self.script_name,
            run_suffix,
        )
        self.stderr_log_path = self.log_directory / "{0}-{1}.err.log".format(
            self.script_name,
            run_suffix,
        )

        self._stdout_handle = self.stdout_log_path.open(
            "a",
            encoding="utf-8",
            buffering=1,
        )
        self._stderr_handle = self.stderr_log_path.open(
            "a",
            encoding="utf-8",
            buffering=1,
        )

        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        sys.stdout = TeeStream(self._original_stdout, self._stdout_handle)
        sys.stderr = TeeStream(self._original_stderr, self._stderr_handle)
        return self

    def __exit__(self, exc_type, exc, tb):
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr
        if self._stdout_handle is not None:
            self._stdout_handle.close()
        if self._stderr_handle is not None:
            self._stderr_handle.close()
        return False


class StepReporter:
    """Print high-visibility step banners and result lines."""

    def __init__(self, enabled=True):
        self.enabled = bool(enabled)

    def section(self, emoji, title, detail=None, stream=None):
        if not self.enabled:
            return
        stream = stream or sys.stdout
        border = "=" * 18
        print("", file=stream)
        print("{0} {1} {2} {0}".format(border, emoji, title), file=stream)
        if detail:
            print(detail, file=stream)

    def message(self, emoji, text, stream=None):
        if not self.enabled:
            return
        stream = stream or sys.stdout
        print("{0} {1}".format(emoji, text), file=stream)

    def value(self, emoji, label, value, stream=None):
        if not self.enabled:
            return
        stream = stream or sys.stdout
        print("{0} {1}: {2}".format(emoji, label, value), file=stream)

    def items(self, emoji, label, values, stream=None):
        if not self.enabled:
            return
        stream = stream or sys.stdout
        values = list(values or [])
        if not values:
            print("{0} {1}: none".format(emoji, label), file=stream)
            return
        print("{0} {1}:".format(emoji, label), file=stream)
        for value in values:
            print("   - {0}".format(value), file=stream)


def cleanup_log_directory(log_directory, retention_days, now=None):
    """Delete log files older than the retention window."""
    log_directory = Path(log_directory).expanduser().resolve()
    retention_days = int(retention_days)
    if retention_days <= 0:
        raise ValueError("retention_days must be greater than zero.")
    if not log_directory.exists():
        return

    current_time = now or datetime.now(timezone.utc)
    cutoff = current_time - timedelta(days=retention_days)
    cutoff_epoch = cutoff.timestamp()

    for candidate in log_directory.rglob("*.log"):
        if not candidate.is_file():
            continue
        if candidate.stat().st_mtime < cutoff_epoch:
            candidate.unlink()


def resolve_runtime_logging_settings(explicit_config_path=None, environment=None, working_root_override=None):
    """Resolve logging settings from the deployment config, or fall back safely."""
    from deploy_steps import DeploymentError, load_pipeline_config

    normalized_environment = normalize_environment(environment)
    try:
        config = load_pipeline_config(
            explicit_path=explicit_config_path,
            working_root_override=working_root_override,
            environment=normalized_environment,
        )
    except (DeploymentError, OSError, ValueError, json.JSONDecodeError):
        return RuntimeLoggingSettings(
            directory=(REPO_ROOT / "build" / "dag_deploy" / normalized_environment / "logs").resolve(),
            retention_days=DEFAULT_LOG_RETENTION_DAYS,
            config_path=None,
            used_fallback=True,
        )

    return RuntimeLoggingSettings(
        directory=config.logging.directory,
        retention_days=config.logging.retention_days,
        config_path=config.config_path,
        used_fallback=False,
    )


def _utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
