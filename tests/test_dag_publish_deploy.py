import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import types
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = REPO_ROOT / "scripts" / "dag_publish"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import package_and_upload_dag
from cli_support import RuntimeLoggingSettings, StepReporter, cleanup_log_directory
from deploy_dag_from_nexus import main, run
from package_and_upload_dag import main as package_and_upload_main
from deploy_steps.archive import ArchiveExtractor
from deploy_steps.checksum import ChecksumValidator
from deploy_steps.config import load_pipeline_config
from deploy_steps.exceptions import DeploymentError
from deploy_steps.publisher import DeploymentPublisher
from deploy_steps.python_checks import ImportChecker, PythonCheckOrchestrator, SyntaxChecker
from deploy_steps.rules import RuleChecker
from deploy_steps.tag_processor import TagProcessor


class ConfigAndArchiveTests(unittest.TestCase):
    def test_step_reporter_numbers_sections(self):
        stdout = io.StringIO()
        reporter = StepReporter(enabled=True)
        reporter.section("🧭", "Initialize", stream=stdout)
        reporter.section("🔍", "Validate", stream=stdout)
        output = stdout.getvalue()
        self.assertIn("Step 1. Initialize", output)
        self.assertIn("Step 2. Validate", output)

    def test_load_pipeline_config_resolves_relative_paths_and_override(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config_path = temp_path / "deploy_pipeline.json"
            config_path.write_text(
                json.dumps(
                    {
                        "paths": {
                            "working_root": "work",
                            "landing_root": "landing",
                            "dags_root": "dags",
                            "backup_root": "backup"
                        },
                        "nexus": {
                            "repository_url": "https://example.invalid/repository/raw",
                            "timeout_seconds": 30,
                            "verify_tls": True
                        },
                        "archive": {
                            "allowed_suffixes": [".zip", ".tar.gz"],
                            "require_single_top_level_dir": True
                        },
                        "checksum": {
                            "mode": "compute_only",
                            "sidecar_suffix": ".sha256"
                        },
                        "logging": {
                            "directory": "logs",
                            "retention_days": 21
                        },
                        "airflow_cli": {
                            "temp_root": "airflow_cli_temp",
                            "env": {
                                "AIRFLOW__CORE__DAGS_FOLDER": "{session_root}/custom_staging",
                                "AIRFLOW__CORE__LOAD_EXAMPLES": False,
                                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:///{session_root}/custom.db"
                            }
                        },
                        "imports": {
                            "extra_pythonpath": ["deps"]
                        },
                        "tagging": {
                            "source_variable_name": "source",
                            "managed_tags": ["GDTET_US_DAG", "GDT_ET_GLOBAL_DAG"],
                            "us_sources": ["camp-us"],
                            "us_tag": "GDTET_US_DAG",
                            "global_tag": "GDT_ET_GLOBAL_DAG"
                        },
                        "rules": {
                            "name_rules": {"enabled": False},
                            "queue_rules": {"enabled": False},
                            "dag_variable_rules": [
                                {
                                    "name": "GDT_ET_FEED_SOURCE",
                                    "required": True,
                                    "allowed_values": ["camp-us", "ucm", "norkom", "na"]
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            config = load_pipeline_config(
                explicit_path=config_path,
                working_root_override=temp_path / "override_work",
                environment="uat",
            )
            self.assertEqual(config.environment, "uat")
            self.assertEqual(config.paths.working_root, (temp_path / "override_work").resolve())
            self.assertEqual(config.paths.landing_root, (temp_path / "landing").resolve())
            self.assertEqual(config.logging.directory, (temp_path / "logs").resolve())
            self.assertEqual(config.logging.retention_days, 21)
            self.assertEqual(config.airflow_cli.temp_root, (temp_path / "airflow_cli_temp").resolve())
            self.assertEqual(
                config.airflow_cli.env["AIRFLOW__CORE__DAGS_FOLDER"],
                "{session_root}/custom_staging",
            )
            self.assertEqual(config.airflow_cli.env["AIRFLOW__CORE__LOAD_EXAMPLES"], "False")
            self.assertEqual(config.imports.extra_pythonpath[0], (temp_path / "deps").resolve())
            self.assertEqual(config.imports.python_executable, "python")
            self.assertEqual(config.imports.activation_command, "")
            self.assertEqual(len(config.rules.dag_variable_rules), 1)
            self.assertEqual(config.rules.dag_variable_rules[0].name, "GDT_ET_FEED_SOURCE")
            self.assertEqual(
                config.rules.dag_variable_rules[0].allowed_values,
                ["camp-us", "ucm", "norkom", "na"],
            )

    def test_cleanup_log_directory_keeps_recent_logs_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            old_log = temp_path / "old.log"
            new_log = temp_path / "new.err.log"
            old_log.write_text("old\n", encoding="utf-8")
            new_log.write_text("new\n", encoding="utf-8")

            now = 1_800_000_000
            old_time = now - (20 * 24 * 60 * 60)
            recent_time = now - (2 * 24 * 60 * 60)
            os.utime(old_log, (old_time, old_time))
            os.utime(new_log, (recent_time, recent_time))

            cleanup_log_directory(
                temp_path,
                retention_days=7,
                now=datetime.fromtimestamp(now, tz=timezone.utc),
            )

            self.assertFalse(old_log.exists())
            self.assertTrue(new_log.exists())

    def test_archive_extractor_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = temp_path / "bad.zip"
            with zipfile.ZipFile(str(archive_path), "w") as archive:
                archive.writestr("../evil.py", "print('nope')\n")
            extractor = ArchiveExtractor([".zip"], True)
            with self.assertRaises(DeploymentError):
                extractor.extract(archive_path, temp_path / "extract")

    def test_archive_extractor_supports_tgz(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            package_dir = temp_path / "pkg"
            package_dir.mkdir()
            (package_dir / "dag.py").write_text("print('ok')\n", encoding="utf-8")
            archive_path = temp_path / "pkg.tgz"
            with tarfile.open(str(archive_path), "w:gz") as archive:
                archive.add(str(package_dir), arcname="pkg")
            extractor = ArchiveExtractor([".tgz"], True)
            extracted = extractor.extract(archive_path, temp_path / "extract")
            self.assertTrue((extracted / "dag.py").is_file())


class ChecksumAndPythonCheckTests(unittest.TestCase):
    def test_checksum_modes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = temp_path / "artifact.zip"
            archive_path.write_bytes(b"demo-bytes")
            actual = ChecksumValidator("compute_only").validate(archive_path)
            self.assertEqual(actual.actual_sha256, ChecksumValidator.compute_sha256(archive_path))

            cli_result = ChecksumValidator("cli_value").validate(
                archive_path,
                expected_sha256=actual.actual_sha256,
            )
            self.assertEqual(cli_result.expected_sha256, actual.actual_sha256)

            sidecar = "{0}  artifact.zip\n".format(actual.actual_sha256)
            sidecar_result = ChecksumValidator("sidecar_file").validate(
                archive_path,
                sidecar_content=sidecar,
            )
            self.assertEqual(sidecar_result.expected_sha256, actual.actual_sha256)

    def test_syntax_checker_fails_on_invalid_python(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bad_file = temp_path / "bad.py"
            bad_file.write_text("def broken(:\n", encoding="utf-8")
            orchestrator = PythonCheckOrchestrator([SyntaxChecker()])
            with self.assertRaises(DeploymentError):
                orchestrator.run(temp_path)

    def test_syntax_checker_collects_all_invalid_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bad_one = temp_path / "bad_one.py"
            bad_two = temp_path / "bad_two.py"
            bad_one.write_text("def broken_one(:\n", encoding="utf-8")
            bad_two.write_text("if True print('broken')\n", encoding="utf-8")

            report = SyntaxChecker().check_files([bad_one, bad_two])

            self.assertTrue(report.has_errors)
            self.assertEqual(set(report.invalid_files), {bad_one, bad_two})
            rendered = report.render_error_message()
            self.assertIn("bad_one.py", rendered)
            self.assertIn("bad_two.py", rendered)

    def test_import_checker_supports_extra_pythonpath_and_reports_missing_modules(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            package_root = temp_path / "pkg"
            deps_root = temp_path / "deps"
            package_root.mkdir()
            deps_root.mkdir()
            (deps_root / "helper.py").write_text("VALUE = 42\n", encoding="utf-8")
            (package_root / "module_ok.py").write_text("from helper import VALUE\n", encoding="utf-8")
            (package_root / "module_bad.py").write_text("import definitely_missing_module\n", encoding="utf-8")

            checker = ImportChecker(extra_pythonpath=[deps_root])
            with self.assertRaises(DeploymentError):
                checker.run(package_root, sorted(package_root.glob("*.py")))

            (package_root / "module_bad.py").unlink()
            checker.run(package_root, sorted(package_root.glob("*.py")))

    def test_import_checker_supports_activation_command(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            package_root = temp_path / "pkg"
            deps_root = temp_path / "deps"
            package_root.mkdir()
            deps_root.mkdir()
            (deps_root / "helper.py").write_text("VALUE = 7\n", encoding="utf-8")
            (package_root / "module_ok.py").write_text("from helper import VALUE\n", encoding="utf-8")
            activation_script = temp_path / "activate_airflow_env.sh"
            activation_script.write_text(
                'export PYTHONPATH="{0}:${{PYTHONPATH:-}}"\n'.format(deps_root),
                encoding="utf-8",
            )
            checker = ImportChecker(
                activation_command="source {0}".format(activation_script),
                shell_executable="/bin/bash",
                python_executable=sys.executable,
            )
            checker.run(package_root, sorted(package_root.glob("*.py")))

    def test_package_upload_reports_readable_syntax_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bad_dag = temp_path / "bad_dag.py"
            bad_dag.write_text("def broken(:\n", encoding="utf-8")
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                package_and_upload_dag,
                "resolve_runtime_logging_settings",
                return_value=_runtime_logging_settings(temp_path),
            ):
                with mock.patch("builtins.input", return_value=""):
                    with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                        exit_code = package_and_upload_main(
                            [
                                str(bad_dag),
                                "--artifact-id",
                                "bad-dag",
                                "--version",
                                "1.0.0",
                                "--credentials-file",
                                str(credentials_file),
                                "--dry-run",
                            ]
                        )

            self.assertEqual(exit_code, 1)
            error_output = stderr.getvalue()
            self.assertIn("Python syntax validation reported issues.", error_output)
            self.assertIn("Syntax check failed for", error_output)
            self.assertIn("bad_dag.py", error_output)
            self.assertNotIn("Traceback", error_output)
            self.assertIn("Execution Logs", stdout.getvalue())

    def test_package_upload_can_continue_after_syntax_error_when_user_types_go(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bad_python = temp_path / "bad_module.py"
            bad_python.write_text("def broken(:\n", encoding="utf-8")
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                package_and_upload_dag,
                "resolve_runtime_logging_settings",
                return_value=_runtime_logging_settings(temp_path),
            ):
                with mock.patch("builtins.input", return_value="go"):
                    with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                        exit_code = package_and_upload_main(
                            [
                                str(bad_python),
                                "--artifact-id",
                                "bad-python",
                                "--version",
                                "1.0.0",
                                "--credentials-file",
                                str(credentials_file),
                                "--dry-run",
                            ]
                        )

            self.assertEqual(exit_code, 0)
            self.assertIn("Syntax check failed for", stderr.getvalue())
            self.assertIn("Archive created:", stdout.getvalue())
            self.assertIn("Airflow CLI validation was skipped because no syntax-valid Python files were available.", stdout.getvalue())
            self.assertIn("STDOUT log:", stdout.getvalue())

    def test_package_upload_runs_airflow_validation_for_valid_files_after_syntax_override(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bad_python = temp_path / "bad_module.py"
            good_dag = temp_path / "good_dag.py"
            bad_python.write_text("def broken(:\n", encoding="utf-8")
            good_dag.write_text(
                textwrap.dedent(
                    """
                    from airflow import DAG

                    with DAG(dag_id="good_demo"):
                        pass
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )
            fake_config = _package_validation_config()
            fake_config.airflow_cli.temp_root = (temp_path / "airflow_cli").resolve()

            calls = []

            def fake_run(command, **kwargs):
                calls.append((command, kwargs))
                env = kwargs.get("env") or {}
                staged_root = Path(env["AIRFLOW__CORE__DAGS_FOLDER"])
                self.assertTrue(any(path.name == "good_dag.py" for path in staged_root.rglob("good_dag.py")))
                self.assertFalse(any(path.name == "bad_module.py" for path in staged_root.rglob("bad_module.py")))
                if len(calls) == 1:
                    return subprocess.CompletedProcess(
                        args=command,
                        returncode=0,
                        stdout="Database migrated.\n",
                        stderr="",
                    )
                return subprocess.CompletedProcess(
                    args=command,
                    returncode=0,
                    stdout="[]",
                    stderr="",
                )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(package_and_upload_dag, "load_pipeline_config", return_value=fake_config):
                with mock.patch.object(
                    package_and_upload_dag,
                    "resolve_runtime_logging_settings",
                    return_value=_runtime_logging_settings(temp_path),
                ):
                    with mock.patch.object(package_and_upload_dag.subprocess, "run", side_effect=fake_run):
                        with mock.patch("builtins.input", return_value="go"):
                            with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                                exit_code = package_and_upload_main(
                                    [
                                        str(temp_path),
                                        "--artifact-id",
                                        "mixed-python",
                                        "--version",
                                        "1.0.0",
                                        "--credentials-file",
                                        str(credentials_file),
                                        "--dry-run",
                                    ]
                                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(calls), 2)
            self.assertIn("Only syntax-valid Python files will be included in Airflow CLI validation.", stdout.getvalue())
            self.assertIn("Python files staged for Airflow CLI validation", stdout.getvalue())
            self.assertIn("Syntax check failed for", stderr.getvalue())

    def test_package_upload_warns_on_airflow_check_and_can_continue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "example_dag.py"
            dag_file.write_text("from airflow import DAG\n", encoding="utf-8")
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )
            fake_config = _package_validation_config()
            migrate_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow", "db", "migrate"],
                returncode=0,
                stdout="Database migrated.\n",
                stderr="",
            )
            airflow_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow", "dags", "list-import-errors", "-l", "-o", "json"],
                returncode=1,
                stdout=json.dumps(
                    [{"filepath": "/tmp/example_dag.py", "error": "Broken DAG: import failed"}]
                ),
                stderr="",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            log_settings = _runtime_logging_settings(temp_path)
            with mock.patch.object(package_and_upload_dag, "load_pipeline_config", return_value=fake_config):
                with mock.patch.object(
                    package_and_upload_dag,
                    "resolve_runtime_logging_settings",
                    return_value=log_settings,
                ):
                    with mock.patch.object(
                        package_and_upload_dag.subprocess,
                        "run",
                        side_effect=[migrate_result, airflow_result],
                    ) as run_mock:
                        with mock.patch("builtins.input", return_value="go") as input_mock:
                            with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                                exit_code = package_and_upload_main(
                                    [
                                        str(dag_file),
                                        "--artifact-id",
                                        "demo",
                                        "--version",
                                        "1.0.0",
                                        "--credentials-file",
                                        str(credentials_file),
                                        "--dry-run",
                                    ]
                                )

            self.assertEqual(exit_code, 0)
            self.assertTrue(input_mock.called)
            self.assertEqual(run_mock.call_count, 2)
            self.assertEqual(run_mock.call_args_list[0].args[0], ["python", "-m", "airflow", "db", "migrate"])
            self.assertEqual(
                run_mock.call_args_list[1].args[0],
                ["python", "-m", "airflow", "dags", "list-import-errors", "-l", "-o", "json"],
            )
            self.assertIn("Airflow DAG validation reported issues.", stderr.getvalue())
            self.assertIn("Broken DAG: import failed", stderr.getvalue())
            self.assertIn("Archive created:", stdout.getvalue())
            self.assertIn("STDERR log:", stdout.getvalue())
            self.assertEqual(len(sorted((temp_path / "logs").glob("*.log"))), 2)

    def test_package_upload_allows_single_directory_imports_from_scripts_package(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_root = temp_path / "dag_bundle_testing_001"
            scripts_dir = source_root / "scripts"
            scripts_dir.mkdir(parents=True)
            (scripts_dir / "__init__.py").write_text("", encoding="utf-8")
            (scripts_dir / "script_one.py").write_text(
                "def process_source_a():\n    return 'ok-a'\n",
                encoding="utf-8",
            )
            (scripts_dir / "script_two.py").write_text(
                "def process_source_b():\n    return 'ok-b'\n",
                encoding="utf-8",
            )
            dag_file = source_root / "test_import_scripts_dag.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    from datetime import datetime
                    from airflow.decorators import dag, task
                    from scripts.script_one import process_source_a
                    from scripts.script_two import process_source_b

                    GDT_ET_FEED_SOURCE = "na"

                    @dag(
                        dag_id="test_import_scripts_dag",
                        start_date=datetime(2024, 1, 1),
                        schedule=None,
                        catchup=False,
                    )
                    def test_import_scripts_dag():
                        @task
                        def run_script_one():
                            return process_source_a()

                        @task
                        def run_script_two(previous_result: str):
                            return process_source_b() + previous_result

                        run_script_two(run_script_one())

                    test_import_scripts_dag()
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )
            fake_config = _package_validation_config()
            fake_config.airflow_cli.temp_root = (temp_path / "airflow_cli").resolve()

            calls = []

            def fake_run(command, **kwargs):
                calls.append((command, kwargs))
                if len(calls) == 1:
                    return subprocess.CompletedProcess(
                        args=command,
                        returncode=0,
                        stdout="Database migrated.\n",
                        stderr="",
                    )

                env = kwargs.get("env") or {}
                staged_root = Path(env["AIRFLOW__CORE__DAGS_FOLDER"])
                dag_path = staged_root / "test_import_scripts_dag.py"
                scripts_path = staged_root / "scripts" / "script_one.py"
                self.assertTrue(dag_path.is_file())
                self.assertTrue(scripts_path.is_file())
                airflow_pkg = staged_root / "airflow"
                decorators_pkg = airflow_pkg / "decorators"
                airflow_pkg.mkdir(exist_ok=True)
                decorators_pkg.mkdir(parents=True, exist_ok=True)
                (airflow_pkg / "__init__.py").write_text("class DAG:\n    pass\n", encoding="utf-8")
                (
                    decorators_pkg / "__init__.py"
                ).write_text(
                    "def dag(*args, **kwargs):\n"
                    "    def wrapper(fn):\n"
                    "        return fn\n"
                    "    return wrapper\n\n"
                    "def task(fn):\n"
                    "    return fn\n",
                    encoding="utf-8",
                )

                import importlib.util

                previous_sys_path = list(sys.path)
                sys.path.insert(0, str(staged_root))
                try:
                    spec = importlib.util.spec_from_file_location("test_import_scripts_dag", dag_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                finally:
                    sys.path[:] = previous_sys_path

                return subprocess.CompletedProcess(
                    args=command,
                    returncode=0,
                    stdout="[]",
                    stderr="",
                )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(package_and_upload_dag, "load_pipeline_config", return_value=fake_config):
                with mock.patch.object(
                    package_and_upload_dag,
                    "resolve_runtime_logging_settings",
                    return_value=_runtime_logging_settings(temp_path),
                ):
                    with mock.patch.object(package_and_upload_dag.subprocess, "run", side_effect=fake_run):
                        with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                            exit_code = package_and_upload_main(
                                [
                                    str(source_root),
                                    "--artifact-id",
                                    "dag-test-001",
                                    "--version",
                                    "1.0.0",
                                    "--credentials-file",
                                    str(credentials_file),
                                    "--dry-run",
                                ]
                            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(calls), 2)
            self.assertIn("Airflow CLI validation finished.", stdout.getvalue())
            self.assertEqual("", stderr.getvalue())

    def test_package_upload_warns_on_dag_variable_rule_and_can_continue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "example_dag.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    from airflow import DAG

                    with DAG(dag_id="demo"):
                        pass
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )
            fake_config = _package_validation_config()
            fake_config.airflow_cli.temp_root = (temp_path / "airflow_cli").resolve()
            fake_config.rules.dag_variable_rules = [
                types.SimpleNamespace(
                    name="GDT_ET_FEED_SOURCE",
                    required=True,
                    allowed_values=["camp-us", "ucm", "norkom", "na"],
                )
            ]
            migrate_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow", "db", "migrate"],
                returncode=0,
                stdout="Database migrated.\n",
                stderr="",
            )
            airflow_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow", "dags", "list-import-errors", "-l", "-o", "json"],
                returncode=0,
                stdout="[]",
                stderr="",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(package_and_upload_dag, "load_pipeline_config", return_value=fake_config):
                with mock.patch.object(
                    package_and_upload_dag,
                    "resolve_runtime_logging_settings",
                    return_value=_runtime_logging_settings(temp_path),
                ):
                    with mock.patch.object(
                        package_and_upload_dag.subprocess,
                        "run",
                        side_effect=[migrate_result, airflow_result],
                    ):
                        with mock.patch("builtins.input", return_value="go"):
                            with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                                exit_code = package_and_upload_main(
                                    [
                                        str(dag_file),
                                        "--artifact-id",
                                        "demo",
                                        "--version",
                                        "1.0.0",
                                        "--credentials-file",
                                        str(credentials_file),
                                        "--dry-run",
                                    ]
                                )

            self.assertEqual(exit_code, 0)
            self.assertIn("DAG rule validation reported issues.", stderr.getvalue())
            self.assertIn("GDT_ET_FEED_SOURCE", stderr.getvalue())
            self.assertIn("Archive created:", stdout.getvalue())
            self.assertIn("Configured DAG rule checks", stdout.getvalue())

    def test_package_upload_warns_on_decorated_dag_missing_feed_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "decorated_dag.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    from airflow.decorators import dag

                    @dag(schedule=None, start_date=None)
                    def demo():
                        return None
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )
            fake_config = _package_validation_config()
            fake_config.airflow_cli.temp_root = (temp_path / "airflow_cli").resolve()
            fake_config.rules.dag_variable_rules = [
                types.SimpleNamespace(
                    name="GDT_ET_FEED_SOURCE",
                    required=True,
                    allowed_values=["camp-us", "ucm", "norkom", "na"],
                )
            ]
            migrate_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow", "db", "migrate"],
                returncode=0,
                stdout="Database migrated.\n",
                stderr="",
            )
            airflow_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow", "dags", "list-import-errors", "-l", "-o", "json"],
                returncode=0,
                stdout="[]",
                stderr="",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(package_and_upload_dag, "load_pipeline_config", return_value=fake_config):
                with mock.patch.object(
                    package_and_upload_dag,
                    "resolve_runtime_logging_settings",
                    return_value=_runtime_logging_settings(temp_path),
                ):
                    with mock.patch.object(
                        package_and_upload_dag.subprocess,
                        "run",
                        side_effect=[migrate_result, airflow_result],
                    ):
                        with mock.patch("builtins.input", return_value="go"):
                            with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                                exit_code = package_and_upload_main(
                                    [
                                        str(dag_file),
                                        "--artifact-id",
                                        "decorated-demo",
                                        "--version",
                                        "1.0.0",
                                        "--credentials-file",
                                        str(credentials_file),
                                        "--dry-run",
                                    ]
                                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Airflow DAG files detected", stdout.getvalue())
            self.assertIn("decorated_dag.py", stdout.getvalue())
            self.assertIn("DAG rule validation reported issues.", stderr.getvalue())
            self.assertIn("GDT_ET_FEED_SOURCE", stderr.getvalue())

    def test_package_upload_debug_prints_airflow_command(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "example_dag.py"
            dag_file.write_text("from airflow import DAG\n", encoding="utf-8")
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )
            fake_config = _package_validation_config(
                activation_command="source /opt/miniconda/etc/profile.d/conda.sh && conda activate airflow3_dev"
            )
            migrate_result = subprocess.CompletedProcess(
                args=["/bin/bash", "-lc", "python -m airflow db migrate"],
                returncode=0,
                stdout="Database migrated.\n",
                stderr="",
            )
            airflow_result = subprocess.CompletedProcess(
                args=["python", "-m", "airflow"],
                returncode=0,
                stdout="[]",
                stderr="",
            )

            stdout = io.StringIO()
            with mock.patch.object(package_and_upload_dag, "load_pipeline_config", return_value=fake_config):
                with mock.patch.object(
                    package_and_upload_dag,
                    "resolve_runtime_logging_settings",
                    return_value=_runtime_logging_settings(temp_path),
                ):
                    with mock.patch.object(
                        package_and_upload_dag.subprocess,
                        "run",
                        side_effect=[migrate_result, airflow_result],
                    ):
                        with mock.patch("sys.stdout", new=stdout):
                            exit_code = package_and_upload_main(
                                [
                                    str(dag_file),
                                    "--artifact-id",
                                    "demo",
                                    "--version",
                                    "1.0.0",
                                    "--credentials-file",
                                    str(credentials_file),
                                    "--dry-run",
                                    "--debug",
                                ]
                            )

            self.assertEqual(exit_code, 0)
            debug_output = stdout.getvalue()
            self.assertIn("[DEBUG] Starting package_and_upload_dag.", debug_output)
            self.assertIn("Airflow DB migrate command:", debug_output)
            self.assertIn("python -m airflow db migrate", debug_output)
            self.assertIn("Airflow validation command:", debug_output)
            self.assertIn("python -m airflow dags list-import-errors -l -o json", debug_output)
            self.assertIn("Executing shell command:", debug_output)
            self.assertIn("Python files staged for Airflow CLI validation", debug_output)


class TagAndRuleTests(unittest.TestCase):
    def test_tag_processor_rewrites_managed_tags_and_preserves_other_tags(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "example.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    source = "camp-us"

                    with DAG(
                        dag_id="demo_one",
                        tags=["keep-me", "GDT_ET_GLOBAL_DAG"],
                    ) as dag:
                        pass

                    dag = DAG(dag_id="demo_two")
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            TagProcessor(
                source_variable_name="source",
                managed_tags=["GDTET_US_DAG", "GDT_ET_GLOBAL_DAG"],
                us_sources=["camp-us"],
                us_tag="GDTET_US_DAG",
                global_tag="GDT_ET_GLOBAL_DAG",
            ).process_package(temp_path)
            updated = dag_file.read_text(encoding="utf-8")
            self.assertIn("['keep-me', 'GDTET_US_DAG']", updated)
            self.assertIn("tags=['GDTET_US_DAG']", updated)
            self.assertNotIn("GDT_ET_GLOBAL_DAG", updated)

    def test_tag_processor_requires_source_and_literal_tags(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            missing_source = temp_path / "missing_source.py"
            missing_source.write_text(
                "dag = DAG(dag_id='demo', tags=get_tags())\n",
                encoding="utf-8",
            )
            processor = TagProcessor(
                source_variable_name="source",
                managed_tags=["GDTET_US_DAG", "GDT_ET_GLOBAL_DAG"],
                us_sources=["camp-us"],
                us_tag="GDTET_US_DAG",
                global_tag="GDT_ET_GLOBAL_DAG",
            )
            with self.assertRaises(DeploymentError):
                processor.process_package(temp_path)

    def test_rule_checker_validates_dag_ids_and_queues(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "rule_dag.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    GDT_ET_FEED_SOURCE = "camp-us"
                    dag = DAG(dag_id="customer_sync_demo")
                    task = DummyOperator(task_id="run", queue="default")
                    """
                ),
                encoding="utf-8",
            )
            checker = RuleChecker(
                name_rules=_rule(enabled=True, allow_patterns=["^customer_sync_"], deny_patterns=[]),
                queue_rules=_rule(enabled=True, allow_patterns=["^default$"], deny_patterns=["^forbidden$"]),
                dag_variable_rules=[],
            )
            checker.validate(temp_path)

            dag_file.write_text(
                'GDT_ET_FEED_SOURCE = "camp-us"\ndag = DAG(dag_id="wrong_name")\n',
                encoding="utf-8",
            )
            with self.assertRaises(DeploymentError):
                checker.validate(temp_path)

    def test_rule_checker_requires_configured_dag_variable_for_airflow_files_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "rule_dag.py"
            helper_file = temp_path / "helper.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    from airflow import DAG

                    with DAG(dag_id="customer_sync_demo"):
                        pass
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            helper_file.write_text("print('plain python file')\n", encoding="utf-8")
            checker = RuleChecker(
                name_rules=_rule(enabled=False, allow_patterns=[], deny_patterns=[]),
                queue_rules=_rule(enabled=False, allow_patterns=[], deny_patterns=[]),
                dag_variable_rules=[
                    types.SimpleNamespace(
                        name="GDT_ET_FEED_SOURCE",
                        required=True,
                        allowed_values=["camp-us", "ucm", "norkom", "na"],
                    )
                ],
            )
            with self.assertRaises(DeploymentError) as error_context:
                checker.validate(temp_path)
            self.assertIn("GDT_ET_FEED_SOURCE", str(error_context.exception))

            dag_file.write_text(
                textwrap.dedent(
                    """
                    from airflow import DAG

                    GDT_ET_FEED_SOURCE = "invalid-source"

                    with DAG(dag_id="customer_sync_demo"):
                        pass
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(DeploymentError) as error_context:
                checker.validate(temp_path)
            self.assertIn("must be one of", str(error_context.exception))

            dag_file.write_text(
                textwrap.dedent(
                    """
                    from airflow import DAG

                    GDT_ET_FEED_SOURCE = "na"

                    with DAG(dag_id="customer_sync_demo"):
                        pass
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            checker.validate(temp_path)

    def test_rule_checker_treats_decorated_functions_as_airflow_dags(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_file = temp_path / "decorated_dag.py"
            dag_file.write_text(
                textwrap.dedent(
                    """
                    from airflow.decorators import dag

                    @dag(schedule=None, start_date=None)
                    def demo():
                        return None
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            checker = RuleChecker(
                name_rules=_rule(enabled=False, allow_patterns=[], deny_patterns=[]),
                queue_rules=_rule(enabled=False, allow_patterns=[], deny_patterns=[]),
                dag_variable_rules=[
                    types.SimpleNamespace(
                        name="GDT_ET_FEED_SOURCE",
                        required=True,
                        allowed_values=["camp-us", "ucm", "norkom", "na"],
                    )
                ],
            )
            with self.assertRaises(DeploymentError) as error_context:
                checker.validate(temp_path)
            self.assertIn("GDT_ET_FEED_SOURCE", str(error_context.exception))


class IntegrationTests(unittest.TestCase):
    def test_deploy_pipeline_dry_run_and_real_publish(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = self._build_demo_archive(temp_path / "demo.zip")
            config_path = self._write_config(temp_path, enable_rules=True)

            with mock.patch(
                "deploy_steps.airflow_cli.execute_airflow_validation",
                return_value=_successful_airflow_cli_result(),
            ):
                dry_run_exit = main([
                    "--archive-file",
                    str(archive_path),
                    "--config",
                    str(config_path),
                    "--dry-run",
                ])
            self.assertEqual(dry_run_exit, 0)
            self.assertFalse(any((temp_path / "landing").glob("**/*")))

            with mock.patch(
                "deploy_steps.airflow_cli.execute_airflow_validation",
                return_value=_successful_airflow_cli_result(),
            ):
                result = run([
                    "--archive-file",
                    str(archive_path),
                    "--config",
                    str(config_path),
                ])
            live_file = result["publish_result"].live_target_dir / "example_dag.py"
            self.assertTrue(live_file.is_file())
            deployed_text = live_file.read_text(encoding="utf-8")
            self.assertIn("GDTET_US_DAG", deployed_text)
            self.assertNotIn("GDT_ET_GLOBAL_DAG", deployed_text)
            landing_file = result["publish_result"].landing_release_dir / "example_dag.py"
            self.assertTrue(landing_file.is_file())
            self.assertFalse(any((temp_path / "landing").glob("**/__pycache__")))
            self.assertFalse(any((temp_path / "dags").glob("**/__pycache__")))

    def test_deploy_pipeline_reports_detected_files_and_log_locations(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = self._build_demo_archive(temp_path / "demo.zip")
            config_path = self._write_config(temp_path, enable_rules=True)

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch(
                "deploy_steps.airflow_cli.execute_airflow_validation",
                return_value=_successful_airflow_cli_result(),
            ):
                with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                    exit_code = main(
                        [
                            "--archive-file",
                            str(archive_path),
                            "--config",
                            str(config_path),
                            "--dry-run",
                            "--debug",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertIn("Python files detected", stdout.getvalue())
            self.assertIn("Run Airflow CLI Validation", stdout.getvalue())
            self.assertIn("Python files staged for Airflow CLI validation", stdout.getvalue())
            self.assertIn("Airflow DAG files detected", stdout.getvalue())
            self.assertIn("Execution Logs", stdout.getvalue())
            self.assertIn("STDOUT log:", stdout.getvalue())

    def test_deploy_pipeline_reports_airflow_cli_import_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = self._build_demo_archive(temp_path / "demo.zip")
            config_path = self._write_config(temp_path, enable_rules=True)

            stdout = io.StringIO()
            stderr = io.StringIO()

            with mock.patch(
                "deploy_steps.airflow_cli.execute_airflow_validation",
                return_value=subprocess.CompletedProcess(
                    args=[sys.executable, "-m", "airflow", "dags", "list-import-errors", "-l", "-o", "json"],
                    returncode=1,
                    stdout=json.dumps(
                        [
                            {
                                "filepath": "/tmp/example_dag.py",
                                "error": "Broken DAG: import failed",
                            }
                        ]
                    ),
                    stderr="",
                ),
            ):
                with mock.patch("sys.stdout", new=stdout), mock.patch("sys.stderr", new=stderr):
                    exit_code = main(
                        [
                            "--archive-file",
                            str(archive_path),
                            "--config",
                            str(config_path),
                            "--dry-run",
                        ]
                    )

            self.assertEqual(exit_code, 1)
            self.assertIn("Airflow reported DAG import errors", stderr.getvalue())
            self.assertIn("Broken DAG: import failed", stderr.getvalue())
            self.assertIn("Run Airflow CLI Validation", stdout.getvalue())

    def test_publish_rolls_back_on_rename_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            publisher = DeploymentPublisher(
                landing_root=temp_path / "landing",
                dags_root=temp_path / "dags",
                backup_root=temp_path / "backup",
            )
            source_dir = temp_path / "source_pkg"
            source_dir.mkdir()
            (source_dir / "dag.py").write_text("new\n", encoding="utf-8")
            live_dir = temp_path / "dags" / "source_pkg"
            live_dir.parent.mkdir(parents=True, exist_ok=True)
            live_dir.mkdir()
            (live_dir / "dag.py").write_text("old\n", encoding="utf-8")

            original_rename = Path.rename

            def failing_rename(path_obj, target):
                path_obj = Path(path_obj)
                if str(path_obj).endswith("/.incoming/release-1/source_pkg"):
                    raise OSError("simulated rename failure")
                return original_rename(path_obj, target)

            with mock.patch.object(Path, "rename", new=failing_rename):
                with self.assertRaises(DeploymentError):
                    publisher.publish(source_dir, "release-1", dry_run=False)

            self.assertEqual((live_dir / "dag.py").read_text(encoding="utf-8"), "old\n")

    def _build_demo_archive(self, archive_path):
        source_root = archive_path.parent / "source_root"
        dag_dir = source_root / "customer_sync"
        dag_dir.mkdir(parents=True)
        (dag_dir / "example_dag.py").write_text(
            textwrap.dedent(
                """
                from airflow import DAG
                from airflow.operators.empty import EmptyOperator

                GDT_ET_FEED_SOURCE = "camp-us"
                source = "camp-us"

                with DAG(
                    dag_id="customer_sync_demo",
                    tags=["keep-me", "GDT_ET_GLOBAL_DAG"],
                ) as dag:
                    EmptyOperator(task_id="start", queue="default")
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        with zipfile.ZipFile(str(archive_path), "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for candidate in sorted(source_root.rglob("*")):
                if candidate.is_file():
                    archive.write(str(candidate), candidate.relative_to(source_root).as_posix())
        return archive_path

    def _write_config(self, temp_path, enable_rules):
        deps_root = temp_path / "deps"
        (deps_root / "airflow" / "operators").mkdir(parents=True)
        (deps_root / "airflow" / "__init__.py").write_text(
            textwrap.dedent(
                """
                class DAG:
                    def __init__(self, *args, **kwargs):
                        self.args = args
                        self.kwargs = kwargs

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, tb):
                        return False
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (deps_root / "airflow" / "operators" / "__init__.py").write_text("", encoding="utf-8")
        (deps_root / "airflow" / "operators" / "empty.py").write_text(
            "class EmptyOperator:\n    def __init__(self, *args, **kwargs):\n        pass\n",
            encoding="utf-8",
        )
        config_path = temp_path / "deploy_pipeline.json"
        config_path.write_text(
            json.dumps(
                {
                    "paths": {
                        "working_root": str(temp_path / "work"),
                        "landing_root": str(temp_path / "landing"),
                        "dags_root": str(temp_path / "dags"),
                        "backup_root": str(temp_path / "backup")
                    },
                    "nexus": {
                        "repository_url": "https://example.invalid/repository/raw",
                        "timeout_seconds": 30,
                        "verify_tls": True
                    },
                    "archive": {
                        "allowed_suffixes": [".zip", ".tar.gz", ".tgz"],
                        "require_single_top_level_dir": True
                    },
                    "checksum": {
                        "mode": "compute_only",
                        "sidecar_suffix": ".sha256"
                    },
                    "logging": {
                        "directory": str(temp_path / "logs"),
                        "retention_days": 14
                    },
                    "airflow_cli": {
                        "temp_root": str(temp_path / "airflow_cli"),
                        "env": {
                            "AIRFLOW__CORE__DAGS_FOLDER": "{session_root}/staging",
                            "AIRFLOW__CORE__LOAD_EXAMPLES": False,
                            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:///{session_root}/airflow_metadata.db"
                        }
                    },
                    "imports": {
                        "extra_pythonpath": [str(deps_root)],
                        "python_executable": sys.executable,
                        "timeout_seconds": 30
                    },
                    "tagging": {
                        "source_variable_name": "source",
                        "managed_tags": ["GDTET_US_DAG", "GDT_ET_GLOBAL_DAG"],
                        "us_sources": ["camp-us", "ucm", "norkom", "mdmd"],
                        "us_tag": "GDTET_US_DAG",
                        "global_tag": "GDT_ET_GLOBAL_DAG"
                    },
                    "rules": {
                        "name_rules": {
                            "enabled": enable_rules,
                            "allow_patterns": ["^customer_sync_"],
                            "deny_patterns": []
                        },
                        "queue_rules": {
                            "enabled": enable_rules,
                            "allow_patterns": ["^default$"],
                            "deny_patterns": []
                        },
                        "dag_variable_rules": [
                            {
                                "name": "GDT_ET_FEED_SOURCE",
                                "required": True,
                                "allowed_values": ["camp-us", "ucm", "norkom", "na"]
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        return config_path

def _rule(enabled, allow_patterns, deny_patterns):
    class SimpleRule:
        pass

    rule = SimpleRule()
    rule.enabled = enabled
    rule.allow_patterns = allow_patterns
    rule.deny_patterns = deny_patterns
    return rule


def _package_validation_config(activation_command=""):
    airflow_cli = types.SimpleNamespace(
        temp_root=Path("/tmp/fake_airflow_cli_temp"),
        env={
            "AIRFLOW__CORE__DAGS_FOLDER": "{session_root}/staging",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:///{session_root}/airflow_metadata.db",
            "PYTHONDONTWRITEBYTECODE": "1",
        },
    )
    imports = types.SimpleNamespace(
        extra_pythonpath=[],
        shell_executable="/bin/bash",
        activation_command=activation_command,
        python_executable="python",
        timeout_seconds=30,
    )
    rules = types.SimpleNamespace(
        name_rules=_rule(enabled=False, allow_patterns=[], deny_patterns=[]),
        queue_rules=_rule(enabled=False, allow_patterns=[], deny_patterns=[]),
        dag_variable_rules=[],
    )
    return types.SimpleNamespace(
        config_path=Path("/tmp/fake_deploy_pipeline.dev.json"),
        airflow_cli=airflow_cli,
        imports=imports,
        rules=rules,
    )


def _runtime_logging_settings(temp_path):
    return RuntimeLoggingSettings(
        directory=(temp_path / "logs").resolve(),
        retention_days=14,
    )


def _successful_airflow_cli_result():
    return subprocess.CompletedProcess(
        args=[sys.executable, "-m", "airflow", "dags", "list-import-errors", "-l", "-o", "json"],
        returncode=0,
        stdout="[]",
        stderr="",
    )


if __name__ == "__main__":
    unittest.main()
