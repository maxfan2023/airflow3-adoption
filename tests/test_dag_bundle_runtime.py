import json
import shutil
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = REPO_ROOT / "scripts" / "dag_publish"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import package_and_upload_dag
from my_company.airflow_bundles.nexus import (
    AirflowException,
    BundleManifest,
    NexusDagBundle,
    prewarm_bundle_cache,
)


class PackageReleaseMetadataTests(unittest.TestCase):
    def test_package_upload_emits_bundle_metadata_objects(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dag_dir = temp_path / "customer_sync"
            dag_dir.mkdir()
            (dag_dir / "example_dag.py").write_text("from airflow import DAG\n", encoding="utf-8")
            credentials_file = temp_path / "nexus_credentials.dev.env"
            credentials_file.write_text(
                "NEXUS_USERNAME=tester\nNEXUS_PASSWORD=secret\n",
                encoding="utf-8",
            )

            uploaded_texts = []

            args = package_and_upload_dag.parse_args(
                [
                    str(dag_dir),
                    "--artifact-id",
                    "customer-sync",
                    "--bundle-name",
                    "customer_sync",
                    "--version",
                    "1.0.0",
                    "--credentials-file",
                    str(credentials_file),
                    "--change-ticket",
                    "CHG123456",
                    "--source-commit",
                    "abcdef123456",
                    "--released-by",
                    "max.wang",
                    "--release-notes",
                    "demo release",
                ]
            )

            with mock.patch.object(package_and_upload_dag, "run_airflow_dag_validation", return_value=None):
                with mock.patch.object(package_and_upload_dag, "run_dag_rule_validation", return_value=None):
                    with mock.patch.object(package_and_upload_dag, "upload_archive", return_value=None):
                        with mock.patch.object(package_and_upload_dag, "fetch_optional_json", return_value={"version": "0.9.0"}):
                            with mock.patch.object(
                                package_and_upload_dag,
                                "upload_text",
                                side_effect=lambda upload_url, text, **kwargs: uploaded_texts.append((upload_url, text)),
                            ):
                                result = package_and_upload_dag._run_with_args(args)

            self.assertTrue(result["uploaded"])
            self.assertEqual(result["bundle_name"], "customer_sync")
            self.assertEqual(result["previous_version"], "0.9.0")
            self.assertEqual(len(uploaded_texts), 4)

            sidecar_url, sidecar_text = uploaded_texts[0]
            self.assertTrue(sidecar_url.endswith(".zip.sha256"))
            self.assertIn("customer-sync.1.0.0.zip", sidecar_text)

            version_record = json.loads(uploaded_texts[1][1])
            self.assertEqual(version_record["bundle_name"], "customer_sync")
            self.assertEqual(version_record["version"], "1.0.0")
            self.assertEqual(version_record["previous_version"], "0.9.0")
            self.assertEqual(version_record["change_ticket"], "CHG123456")
            self.assertEqual(version_record["source_commit"], "abcdef123456")

            release_record = json.loads(uploaded_texts[2][1])
            self.assertEqual(release_record["release_record_path"], result["release_record_path"])
            self.assertEqual(release_record["version_record_path"], result["version_record_path"])

            latest_manifest = json.loads(uploaded_texts[3][1])
            self.assertEqual(latest_manifest["bundle_name"], "customer_sync")
            self.assertEqual(latest_manifest["version"], "1.0.0")
            self.assertEqual(latest_manifest["version_record_path"], result["version_record_path"])


class NexusDagBundleRuntimeTests(unittest.TestCase):
    def test_refresh_caches_bundle_and_reuses_current_on_outage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            artifact_path, manifests = _build_demo_bundle_artifacts(temp_path, version="1.0.0")
            client = _FakeClient(temp_path, manifests, artifact_path)

            bundle = NexusDagBundle(
                bundle_name="customer_sync",
                manifest_path="com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/latest.json",
                nexus_conn_id="dag_bundle_nexus_dev",
                cache_root=temp_path / "cache",
                repository_url="https://example.invalid/repository/raw",
            )
            bundle._build_client = lambda: client  # type: ignore[method-assign]
            bundle.refresh()
            cached_path = Path(bundle.path)
            self.assertTrue(cached_path.is_dir())
            self.assertEqual(bundle.get_current_version(), "1.0.0")

            outage_bundle = NexusDagBundle(
                bundle_name="customer_sync",
                manifest_path="com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/latest.json",
                nexus_conn_id="dag_bundle_nexus_dev",
                cache_root=temp_path / "cache",
                repository_url="https://example.invalid/repository/raw",
            )
            outage_bundle._build_client = lambda: _FailingClient()  # type: ignore[method-assign]
            outage_bundle.refresh()
            self.assertEqual(outage_bundle.get_current_version(), "1.0.0")
            self.assertEqual(Path(outage_bundle.path), cached_path)

    def test_refresh_without_cache_fails_when_nexus_is_unavailable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundle = NexusDagBundle(
                bundle_name="customer_sync",
                manifest_path="com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/latest.json",
                nexus_conn_id="dag_bundle_nexus_dev",
                cache_root=temp_path / "cache",
                repository_url="https://example.invalid/repository/raw",
            )
            bundle._build_client = lambda: _FailingClient()  # type: ignore[method-assign]

            with self.assertRaises(AirflowException):
                bundle.refresh()

    def test_prewarm_bundle_cache_downloads_requested_version(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            artifact_path, manifests = _build_demo_bundle_artifacts(temp_path, version="2.0.0")

            fake_client_factory = lambda **kwargs: _FakeClient(temp_path, manifests, artifact_path)
            with mock.patch("my_company.airflow_bundles.nexus.NexusArtifactClient", side_effect=fake_client_factory):
                metadata = prewarm_bundle_cache(
                    bundle_name="customer_sync",
                    repository_url="https://example.invalid/repository/raw",
                    cache_root=temp_path / "cache",
                    username="tester",
                    password="secret",
                    manifest_path="com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/latest.json",
                    version="2.0.0",
                )

            self.assertEqual(metadata["version"], "2.0.0")
            self.assertTrue(Path(metadata["package_root"]).is_dir())


def _build_demo_bundle_artifacts(temp_path, version):
    package_root = temp_path / "bundle_source" / "customer_sync"
    package_root.mkdir(parents=True)
    (package_root / "example_dag.py").write_text("from airflow import DAG\n", encoding="utf-8")

    archive_path = temp_path / "customer_sync.{0}.zip".format(version)
    with zipfile.ZipFile(str(archive_path), "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for candidate in sorted(package_root.parent.rglob("*")):
            if candidate.is_file():
                archive.write(str(candidate), candidate.relative_to(package_root.parent).as_posix())

    sha256 = package_and_upload_dag.hashlib.sha256(archive_path.read_bytes()).hexdigest()
    sidecar_path = Path(str(archive_path) + ".sha256")
    sidecar_path.write_text("{0}  {1}\n".format(sha256, archive_path.name), encoding="utf-8")

    latest_path = "com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/latest.json"
    version_record_path = "com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/versions/{0}.json".format(version)
    manifest = BundleManifest(
        bundle_name="customer_sync",
        version=version,
        artifact_path="com/hsbc/gdt/et/fctm/1646753/CHG123456/{0}".format(archive_path.name),
        sha256=sha256,
        released_at="2026-04-21T12:34:56Z",
        release_record_path="com/hsbc/gdt/et/fctm/bundles/dev/customer_sync/releases/2026-04-21T12:34:56Z.json",
        version_record_path=version_record_path,
    )
    manifests = {
        latest_path: manifest,
        version_record_path: manifest,
    }
    artifact_map = {
        manifest.artifact_path: archive_path,
        manifest.artifact_path + ".sha256": sidecar_path,
    }
    return artifact_map, manifests


class _FakeClient:
    def __init__(self, temp_path, manifests, artifact_map):
        self.temp_path = temp_path
        self.manifests = manifests
        self.artifact_map = artifact_map

    def read_manifest(self, manifest_path):
        try:
            return self.manifests[manifest_path]
        except KeyError as exc:
            raise AirflowException("Missing manifest: {0}".format(manifest_path)) from exc

    def read_optional_manifest(self, manifest_path):
        return self.manifests.get(manifest_path)

    def download_to_path(self, artifact_path, target_path):
        source_path = self.artifact_map[artifact_path]
        target_path = Path(target_path).expanduser().resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source_path), str(target_path))
        return target_path

    def build_object_url(self, artifact_path):
        return "https://example.invalid/repository/raw/{0}".format(artifact_path)


class _FailingClient:
    def read_manifest(self, manifest_path):
        raise AirflowException("Nexus is unavailable")

    def read_optional_manifest(self, manifest_path):
        raise AirflowException("Nexus is unavailable")

    def download_to_path(self, artifact_path, target_path):
        raise AirflowException("Nexus is unavailable")

    def build_object_url(self, artifact_path):
        return "https://example.invalid/repository/raw/{0}".format(artifact_path)


if __name__ == "__main__":
    unittest.main()
