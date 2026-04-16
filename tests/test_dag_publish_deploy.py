import json
import shutil
import sys
import tarfile
import tempfile
import textwrap
import unittest
import zipfile
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = REPO_ROOT / "scripts" / "dag_publish"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from deploy_dag_from_nexus import main, run
from deploy_steps.archive import ArchiveExtractor
from deploy_steps.checksum import ChecksumValidator
from deploy_steps.config import load_pipeline_config
from deploy_steps.exceptions import DeploymentError
from deploy_steps.publisher import DeploymentPublisher
from deploy_steps.python_checks import ImportChecker, PythonCheckOrchestrator, SyntaxChecker
from deploy_steps.rules import RuleChecker
from deploy_steps.tag_processor import TagProcessor


class ConfigAndArchiveTests(unittest.TestCase):
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
                            "queue_rules": {"enabled": False}
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
            self.assertEqual(config.imports.extra_pythonpath[0], (temp_path / "deps").resolve())
            self.assertEqual(config.imports.python_executable, "python")
            self.assertEqual(config.imports.activation_command, "")

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
                    dag = DAG(dag_id="customer_sync_demo")
                    task = DummyOperator(task_id="run", queue="default")
                    """
                ),
                encoding="utf-8",
            )
            checker = RuleChecker(
                name_rules=_rule(enabled=True, allow_patterns=["^customer_sync_"], deny_patterns=[]),
                queue_rules=_rule(enabled=True, allow_patterns=["^default$"], deny_patterns=["^forbidden$"]),
            )
            checker.validate(temp_path)

            dag_file.write_text(
                'dag = DAG(dag_id="wrong_name")\n',
                encoding="utf-8",
            )
            with self.assertRaises(DeploymentError):
                checker.validate(temp_path)


class IntegrationTests(unittest.TestCase):
    def test_deploy_pipeline_dry_run_and_real_publish(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = self._build_demo_archive(temp_path / "demo.zip")
            config_path = self._write_config(temp_path, enable_rules=True)

            dry_run_exit = main([
                "--archive-file",
                str(archive_path),
                "--config",
                str(config_path),
                "--dry-run",
            ])
            self.assertEqual(dry_run_exit, 0)
            self.assertFalse(any((temp_path / "landing").glob("**/*")))

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
                        }
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


if __name__ == "__main__":
    unittest.main()
