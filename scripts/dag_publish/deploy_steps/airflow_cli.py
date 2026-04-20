"""Airflow CLI validation for extracted DAG packages."""

import json
import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

from deploy_steps.exceptions import DeploymentError
from deploy_steps.python_checks import discover_python_files
from deploy_steps.rules import discover_airflow_dag_files


def validate_package_with_airflow_cli(package_root, airflow_cli_settings, import_settings, reporter=None, debug=False):
    """Run Airflow CLI validation against an extracted DAG package."""
    package_root = Path(package_root).expanduser().resolve()
    airflow_cli_settings.temp_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="dag_deploy_airflow_check_",
        dir=str(airflow_cli_settings.temp_root),
    ) as temp_dir:
        session_root = Path(temp_dir).resolve()
        environment_overrides = render_airflow_cli_env(
            airflow_cli_settings.env,
            session_root=session_root,
        )
        staging_root = resolve_airflow_staging_root(
            environment_overrides["AIRFLOW__CORE__DAGS_FOLDER"],
            session_root=session_root,
        )
        stage_package_root_for_airflow_check(package_root, staging_root, debug=debug)
        staged_python_files = discover_python_files(staging_root)
        detected_dag_files = discover_airflow_dag_files(python_files=staged_python_files)

        if reporter:
            reporter.message(
                "🧪",
                "Checks: initialize a temporary Airflow metadata DB, then run DAG import validation.",
            )
            reporter.items("📄", "Python files staged for Airflow CLI validation", staged_python_files)
            reporter.items("🌬️", "Airflow DAG files detected", detected_dag_files)

        command = build_airflow_validation_command(import_settings.python_executable)
        debug_print(debug, "Airflow CLI temp root: {0}".format(airflow_cli_settings.temp_root))
        debug_print(debug, "Airflow CLI session root: {0}".format(session_root))
        debug_print(debug, "Airflow validation command: {0}".format(format_command(command)))
        debug_print(debug, "Airflow validation environment overrides: {0}".format(environment_overrides))

        completed = execute_airflow_validation(
            python_executable=import_settings.python_executable,
            validation_command=command,
            shell_executable=import_settings.shell_executable,
            activation_command=import_settings.activation_command,
            timeout_seconds=import_settings.timeout_seconds,
            environment_overrides=environment_overrides,
            debug=debug,
        )
        warning_message = interpret_airflow_validation_result(completed)
        if warning_message:
            raise DeploymentError(warning_message)


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


def stage_package_root_for_airflow_check(package_root, staging_root, debug=False):
    """Copy the extracted package root into the Airflow CLI staging directory."""
    package_root = Path(package_root).expanduser().resolve()
    staging_root = Path(staging_root).expanduser().resolve()
    target_root = staging_root / package_root.name

    if staging_root.exists():
        shutil.rmtree(str(staging_root))
    staging_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(str(package_root), str(target_root))
    debug_print(debug, "Staged extracted package for Airflow CLI validation: {0} -> {1}".format(package_root, target_root))
    return target_root


def build_airflow_validation_command(python_executable):
    """Build the Airflow CLI command used for DAG validation."""
    return [
        python_executable,
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
    """Interpret the Airflow CLI result and return a readable error when issues are found."""
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
            completed.returncode,
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


def format_command(command):
    """Format a command for debug output."""
    if isinstance(command, str):
        return command
    return " ".join(shlex.quote(str(part)) for part in command)


def debug_print(enabled, message):
    """Print a debug line when debug output is enabled."""
    if enabled:
        print("[DEBUG] {0}".format(message))
