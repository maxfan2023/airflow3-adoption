"""Python validation checks used by the DAG deployment pipeline."""

import ast
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from deploy_steps.exceptions import DeploymentError


IMPORT_PROBE_PATH = Path(__file__).with_name("import_probe.py")


def discover_python_files(package_root):
    """Return a stable list of Python files inside the package."""
    return sorted(
        candidate
        for candidate in Path(package_root).rglob("*.py")
        if candidate.is_file()
    )


class BasePythonChecker:
    """Base class for extensible Python checks."""

    name = "base"

    def run(self, package_root, python_files):
        raise NotImplementedError


@dataclass
class SyntaxCheckReport:
    """Summarize syntax validation results across multiple files."""

    valid_files: list[Path] = field(default_factory=list)
    invalid_files: list[Path] = field(default_factory=list)
    error_messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self):
        return bool(self.error_messages)

    def render_error_message(self):
        return "\n".join(self.error_messages)


class SyntaxChecker(BasePythonChecker):
    """Validate Python syntax without importing code."""

    name = "syntax"

    def check_files(self, python_files):
        report = SyntaxCheckReport()
        for python_file in python_files:
            try:
                ast.parse(python_file.read_text(encoding="utf-8"), filename=str(python_file))
            except SyntaxError as exc:
                report.invalid_files.append(Path(python_file))
                report.error_messages.append(
                    "Syntax check failed for {0}: {1}".format(python_file, exc)
                )
            else:
                report.valid_files.append(Path(python_file))
        return report

    def run(self, package_root, python_files):
        report = self.check_files(python_files)
        if report.has_errors:
            raise DeploymentError(report.render_error_message())
        return report


class ImportChecker(BasePythonChecker):
    """Import each Python file to catch missing dependencies and import-time errors."""

    name = "import"

    def __init__(
        self,
        extra_pythonpath=None,
        activation_command="",
        shell_executable="/bin/bash",
        python_executable=None,
        timeout_seconds=300,
    ):
        self.extra_pythonpath = [
            Path(item).expanduser().resolve() for item in (extra_pythonpath or [])
        ]
        self.activation_command = str(activation_command or "").strip()
        self.shell_executable = str(shell_executable or "/bin/bash")
        self.python_executable = str(python_executable or sys.executable)
        self.timeout_seconds = int(timeout_seconds)

    def run(self, package_root, python_files):
        import_root = Path(package_root).expanduser().resolve().parent
        package_root = Path(package_root).expanduser().resolve()
        module_names = [self._module_name(import_root, file_path) for file_path in python_files]
        command = self._build_probe_command(import_root, package_root, module_names)
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"

        try:
            if self.activation_command:
                completed = subprocess.run(
                    [self.shell_executable, "-lc", command],
                    capture_output=True,
                    text=True,
                    env=environment,
                    timeout=self.timeout_seconds,
                    check=False,
                )
            else:
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    env=environment,
                    timeout=self.timeout_seconds,
                    check=False,
                )
        except subprocess.TimeoutExpired as exc:
            raise DeploymentError(
                "Import check timed out after {0} seconds.".format(self.timeout_seconds)
            ) from exc
        except OSError as exc:
            raise DeploymentError(
                "Import check could not start the configured Python environment: {0}".format(exc)
            ) from exc

        _remove_python_cache_artifacts(package_root)
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            if not detail:
                detail = "Import probe exited with code {0}.".format(completed.returncode)
            raise DeploymentError(detail)

    def _module_name(self, import_root, file_path):
        relative = Path(file_path).expanduser().resolve().relative_to(import_root).with_suffix("")
        return ".".join(relative.parts)

    def _build_probe_command(self, import_root, package_root, module_names):
        probe_args = [
            self.python_executable,
            "-B",
            str(IMPORT_PROBE_PATH),
            "--import-root",
            str(import_root),
            "--package-root",
            str(package_root),
        ]
        for extra_path in self.extra_pythonpath:
            probe_args.extend(["--extra-pythonpath", str(extra_path)])
        for module_name in module_names:
            probe_args.extend(["--module", module_name])

        if self.activation_command:
            quoted = " ".join(shlex.quote(item) for item in probe_args)
            return "{0} && {1}".format(self.activation_command, quoted)
        return probe_args


def _remove_python_cache_artifacts(package_root):
    for cache_dir in Path(package_root).rglob("__pycache__"):
        if cache_dir.is_dir():
            shutil.rmtree(str(cache_dir))

    for compiled_file in Path(package_root).rglob("*.py[co]"):
        if compiled_file.is_file():
            compiled_file.unlink()


class PythonCheckOrchestrator:
    """Run a configurable sequence of Python validation checks."""

    def __init__(self, checkers):
        self.checkers = list(checkers)

    def run(self, package_root):
        python_files = discover_python_files(package_root)
        if not python_files:
            raise DeploymentError("No Python files found under {0}".format(package_root))
        for checker in self.checkers:
            checker.run(package_root, python_files)
        return python_files
