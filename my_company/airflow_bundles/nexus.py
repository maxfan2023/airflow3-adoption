"""Nexus-backed Dag Bundle implementation with persistent local caching."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import ssl
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional
from urllib import error, parse, request

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback is best-effort only.
    fcntl = None

try:  # pragma: no cover - exercised indirectly when Airflow is installed.
    from airflow.dag_processing.bundles.base import BaseDagBundle
except ImportError:  # pragma: no cover - local tests run without Airflow installed.
    class BaseDagBundle:  # type: ignore[override]
        supports_versioning = True

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

try:  # pragma: no cover - exercised indirectly when Airflow is installed.
    from airflow.exceptions import AirflowException
except ImportError:  # pragma: no cover
    class AirflowException(RuntimeError):
        """Fallback exception used when Airflow is not installed."""


SHA256_LENGTH = 64


@dataclass
class BundleManifest:
    bundle_name: str
    version: str
    artifact_path: str
    sha256: str
    released_at: str
    release_record_path: str = ""
    version_record_path: str = ""

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise AirflowException("Bundle manifest must be a JSON object.")
        required_fields = ["bundle_name", "version", "artifact_path", "sha256", "released_at"]
        missing = [field for field in required_fields if not str(raw.get(field) or "").strip()]
        if missing:
            raise AirflowException(
                "Bundle manifest is missing required field(s): {0}".format(", ".join(missing))
            )
        sha256 = str(raw["sha256"]).strip().lower()
        if len(sha256) != SHA256_LENGTH or any(character not in "0123456789abcdef" for character in sha256):
            raise AirflowException("Bundle manifest sha256 is not a valid 64-character lowercase hex string.")
        return cls(
            bundle_name=str(raw["bundle_name"]).strip(),
            version=str(raw["version"]).strip(),
            artifact_path=str(raw["artifact_path"]).strip(),
            sha256=sha256,
            released_at=str(raw["released_at"]).strip(),
            release_record_path=str(raw.get("release_record_path") or "").strip(),
            version_record_path=str(raw.get("version_record_path") or "").strip(),
        )

    def to_dict(self):
        return {
            "bundle_name": self.bundle_name,
            "version": self.version,
            "artifact_path": self.artifact_path,
            "sha256": self.sha256,
            "released_at": self.released_at,
            "release_record_path": self.release_record_path,
            "version_record_path": self.version_record_path,
        }


class NexusArtifactClient:
    """Read DAG bundle metadata and archives from Nexus Raw."""

    def __init__(self, repository_url, username=None, password=None, verify_tls=True, timeout_seconds=60):
        self.repository_url = str(repository_url or "").rstrip("/")
        self.username = username
        self.password = password
        self.verify_tls = bool(verify_tls)
        self.timeout_seconds = int(timeout_seconds)

    def read_manifest(self, manifest_path):
        payload = self.read_json(self.build_object_url(manifest_path))
        return BundleManifest.from_dict(payload)

    def read_optional_manifest(self, manifest_path):
        payload = self.read_optional_json(self.build_object_url(manifest_path))
        if payload is None:
            return None
        return BundleManifest.from_dict(payload)

    def read_json(self, target_url):
        body = self.read_text(target_url)
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise AirflowException("JSON metadata at {0} is invalid.".format(target_url)) from exc

    def read_optional_json(self, target_url):
        body = self.read_optional_text(target_url)
        if body is None:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise AirflowException("JSON metadata at {0} is invalid.".format(target_url)) from exc

    def read_text(self, target_url):
        try:
            with request.urlopen(
                self._build_request(target_url),
                timeout=self.timeout_seconds,
                context=self._ssl_context(),
            ) as response:
                return response.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace").strip()
            message = "Unable to read Nexus object {0}: HTTP {1}".format(target_url, exc.code)
            if error_body:
                message = "{0}: {1}".format(message, error_body)
            raise AirflowException(message) from exc
        except error.URLError as exc:
            raise AirflowException(
                "Unable to reach Nexus object {0}: {1}".format(target_url, exc.reason)
            ) from exc

    def read_optional_text(self, target_url):
        try:
            return self.read_text(target_url)
        except AirflowException as exc:
            if "HTTP 404" in str(exc):
                return None
            raise

    def download_to_path(self, artifact_path, target_path):
        target_url = self.build_object_url(artifact_path)
        target_path = Path(target_path).expanduser().resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with request.urlopen(
                self._build_request(target_url),
                timeout=self.timeout_seconds,
                context=self._ssl_context(),
            ) as response:
                with target_path.open("wb") as handle:
                    shutil.copyfileobj(response, handle)
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace").strip()
            message = "Unable to download Nexus object {0}: HTTP {1}".format(target_url, exc.code)
            if error_body:
                message = "{0}: {1}".format(message, error_body)
            raise AirflowException(message) from exc
        except error.URLError as exc:
            raise AirflowException(
                "Unable to reach Nexus object {0}: {1}".format(target_url, exc.reason)
            ) from exc
        return target_path

    def build_object_url(self, artifact_path):
        return "{0}/{1}".format(
            self.repository_url.rstrip("/"),
            parse.quote(str(artifact_path).lstrip("/"), safe="/"),
        )

    def _build_request(self, target_url):
        headers = {}
        if self.username and self.password:
            auth = base64.b64encode(
                "{0}:{1}".format(self.username, self.password).encode("utf-8")
            ).decode("ascii")
            headers["Authorization"] = "Basic {0}".format(auth)
        return request.Request(target_url, headers=headers)

    def _ssl_context(self):
        if self.verify_tls:
            return None
        return ssl._create_unverified_context()


class NexusDagBundle(BaseDagBundle):
    """Dag Bundle that reads a versioned bundle from Nexus and caches it locally."""

    supports_versioning = True

    def __init__(
        self,
        *,
        bundle_name,
        manifest_path,
        nexus_conn_id,
        cache_root,
        refresh_interval=300,
        verify_tls=True,
        repository_url=None,
        timeout_seconds=60,
        version=None,
        **kwargs
    ):
        base_kwargs = dict(kwargs)
        base_kwargs.setdefault("refresh_interval", refresh_interval)
        if version is not None:
            base_kwargs.setdefault("version", version)
        try:
            super().__init__(**base_kwargs)
        except TypeError:  # pragma: no cover - fallback for the local stub base class.
            super().__init__()

        self.bundle_name = str(bundle_name).strip()
        self.manifest_path = str(manifest_path).strip()
        self.nexus_conn_id = str(nexus_conn_id).strip()
        self.verify_tls = bool(verify_tls)
        self.repository_url = str(repository_url or "").strip()
        self.timeout_seconds = int(timeout_seconds)
        self.requested_version = str(version or "").strip()
        self.cache_root = Path(cache_root).expanduser().resolve()
        self.bundle_root = self.cache_root / self.bundle_name
        self.current_pointer_path = self.bundle_root / "current.json"
        self.lock_path = self.bundle_root / ".bundle.lock"
        self._initialized = False

    def initialize(self):
        if self._initialized:
            return
        self.bundle_root.mkdir(parents=True, exist_ok=True)
        self._initialized = True
        if self.requested_version and not self._load_version_metadata(self.requested_version):
            self._ensure_version_available(self.requested_version)

    @property
    def path(self):
        metadata = self._resolve_active_metadata()
        if not metadata:
            raise AirflowException("No cached DAG bundle is available for '{0}'.".format(self.bundle_name))
        return Path(metadata["package_root"]).expanduser().resolve()

    def get_current_version(self):
        metadata = self._resolve_active_metadata()
        return str(metadata["version"])

    def refresh(self):
        self.initialize()
        with self._bundle_lock():
            if self.requested_version:
                self._ensure_version_available(self.requested_version)
                return

            current_pointer = self._load_current_pointer()
            previous_version = ""
            if current_pointer:
                previous_version = str(current_pointer.get("version") or "").strip()

            try:
                manifest = self._fetch_latest_manifest()
            except AirflowException:
                if current_pointer and Path(current_pointer["package_root"]).expanduser().resolve().is_dir():
                    return
                raise

            try:
                metadata = self._ensure_manifest_cached(manifest)
            except AirflowException:
                if current_pointer and self._has_valid_cached_package(current_pointer):
                    return
                raise
            self._write_current_pointer(metadata)
            self._prune_cached_versions(keep_versions=[manifest.version, previous_version])

    def view_url(self):
        client = self._build_client()
        if self.requested_version:
            try:
                version_manifest = self._fetch_manifest_for_version(self.requested_version)
            except AirflowException:
                return client.build_object_url(self.manifest_path)
            return client.build_object_url(version_manifest.artifact_path)
        return client.build_object_url(self.manifest_path)

    def _fetch_latest_manifest(self):
        client = self._build_client()
        return client.read_manifest(self.manifest_path)

    def _fetch_manifest_for_version(self, version):
        client = self._build_client()
        latest_manifest = client.read_optional_manifest(self.manifest_path)
        if latest_manifest and latest_manifest.version == version:
            return latest_manifest
        return client.read_manifest(self._build_version_record_path(version))

    def _ensure_version_available(self, version):
        version = str(version or "").strip()
        if not version:
            raise AirflowException("Requested bundle version cannot be empty.")
        metadata = self._load_version_metadata(version)
        if metadata:
            return metadata
        with self._bundle_lock():
            metadata = self._load_version_metadata(version)
            if metadata:
                return metadata
            manifest = self._fetch_manifest_for_version(version)
            return self._ensure_manifest_cached(manifest)

    def _ensure_manifest_cached(self, manifest):
        metadata = self._load_version_metadata(manifest.version)
        if metadata:
            return metadata

        version_root = self.bundle_root / manifest.version
        temporary_root = self.bundle_root / ".tmp" / manifest.version
        if temporary_root.exists():
            shutil.rmtree(str(temporary_root))
        temporary_root.mkdir(parents=True, exist_ok=True)

        try:
            client = self._build_client()
            archive_path = client.download_to_path(manifest.artifact_path, temporary_root / Path(manifest.artifact_path).name)
            sidecar_path = client.download_to_path(
                manifest.artifact_path + ".sha256",
                temporary_root / (archive_path.name + ".sha256"),
            )
            sidecar_hash = _parse_sha256_sidecar(sidecar_path.read_text(encoding="utf-8"))
            if sidecar_hash != manifest.sha256:
                raise AirflowException(
                    "Checksum sidecar for bundle '{0}' does not match latest manifest.".format(
                        self.bundle_name
                    )
                )
            archive_hash = _compute_sha256(archive_path)
            if archive_hash != manifest.sha256:
                raise AirflowException(
                    "Checksum mismatch for bundle '{0}': expected {1}, got {2}".format(
                        self.bundle_name,
                        manifest.sha256,
                        archive_hash,
                    )
                )
            package_root_name = _extract_single_top_level_directory(archive_path, temporary_root / "extract")
            final_package_root = version_root / package_root_name
            metadata = {
                "bundle_name": manifest.bundle_name,
                "version": manifest.version,
                "artifact_path": manifest.artifact_path,
                "sha256": manifest.sha256,
                "released_at": manifest.released_at,
                "package_root_name": package_root_name,
                "package_root": str(final_package_root.resolve()),
            }
            if version_root.exists():
                shutil.rmtree(str(version_root))
            version_root.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(temporary_root / "extract"), str(version_root))
            _write_json_atomic(version_root / ".bundle-version.json", metadata)
            return metadata
        finally:
            if temporary_root.exists():
                shutil.rmtree(str(temporary_root))

    def _build_version_record_path(self, version):
        manifest_path = PurePosixPath(self.manifest_path)
        parent = manifest_path.parent
        return "{0}/versions/{1}.json".format(parent.as_posix(), version)

    def _build_client(self):
        connection_details = self._resolve_connection_details()
        return NexusArtifactClient(
            repository_url=connection_details["repository_url"],
            username=connection_details.get("username"),
            password=connection_details.get("password"),
            verify_tls=connection_details["verify_tls"],
            timeout_seconds=connection_details["timeout_seconds"],
        )

    def _resolve_connection_details(self):
        if self.repository_url:
            return {
                "repository_url": self.repository_url.rstrip("/"),
                "username": None,
                "password": None,
                "verify_tls": self.verify_tls,
                "timeout_seconds": self.timeout_seconds,
            }

        conn = self._load_airflow_connection()
        extras = self._connection_extras(conn)
        repository_url = str(extras.get("repository_url") or "").strip()
        if not repository_url:
            repository_url = self._build_repository_url_from_connection(conn, extras)
        if not repository_url:
            raise AirflowException(
                "Nexus connection '{0}' must provide repository_url in extras or host/base_path fields.".format(
                    self.nexus_conn_id
                )
            )
        return {
            "repository_url": repository_url.rstrip("/"),
            "username": getattr(conn, "login", None),
            "password": getattr(conn, "password", None),
            "verify_tls": bool(extras.get("verify_tls", self.verify_tls)),
            "timeout_seconds": int(extras.get("timeout_seconds", self.timeout_seconds)),
        }

    def _load_airflow_connection(self):  # pragma: no cover - mocked in tests.
        try:
            from airflow.hooks.base import BaseHook
        except ImportError:
            try:
                from airflow.hooks.base_hook import BaseHook  # type: ignore
            except ImportError as exc:
                raise AirflowException(
                    "Airflow BaseHook is not available; cannot resolve Nexus connection '{0}'.".format(
                        self.nexus_conn_id
                    )
                ) from exc
        return BaseHook.get_connection(self.nexus_conn_id)

    def _connection_extras(self, conn):
        extras = getattr(conn, "extra_dejson", None)
        if isinstance(extras, dict):
            return extras
        raw_extra = getattr(conn, "extra", None)
        if not raw_extra:
            return {}
        try:
            return json.loads(raw_extra)
        except json.JSONDecodeError:
            return {}

    def _build_repository_url_from_connection(self, conn, extras):
        explicit_host = str(getattr(conn, "host", "") or extras.get("host") or "").strip()
        if explicit_host.startswith("http://") or explicit_host.startswith("https://"):
            base_url = explicit_host.rstrip("/")
        elif explicit_host:
            schema = str(getattr(conn, "schema", "") or extras.get("schema") or "https").strip()
            port = getattr(conn, "port", None)
            host_port = "{0}:{1}".format(explicit_host, port) if port else explicit_host
            base_url = "{0}://{1}".format(schema, host_port).rstrip("/")
        else:
            base_url = ""
        base_path = str(extras.get("base_path") or "").strip().strip("/")
        if base_path:
            return "{0}/{1}".format(base_url.rstrip("/"), base_path)
        return base_url

    def _load_current_pointer(self):
        return self._load_json_file(self.current_pointer_path)

    def _resolve_active_metadata(self):
        self.initialize()
        if self.requested_version:
            return self._ensure_version_available(self.requested_version)

        current_pointer = self._load_current_pointer()
        if current_pointer and not self._has_valid_cached_package(current_pointer):
            current_pointer = None

        try:
            manifest = self._fetch_latest_manifest()
        except AirflowException:
            if current_pointer:
                return current_pointer
            raise

        if current_pointer and str(current_pointer.get("version") or "").strip() == manifest.version:
            return current_pointer

        previous_version = ""
        if current_pointer:
            previous_version = str(current_pointer.get("version") or "").strip()

        try:
            metadata = self._ensure_manifest_cached(manifest)
        except AirflowException:
            if current_pointer:
                return current_pointer
            raise

        self._write_current_pointer(metadata)
        self._prune_cached_versions(keep_versions=[manifest.version, previous_version])
        return metadata

    def _load_version_metadata(self, version):
        version_path = self.bundle_root / str(version).strip() / ".bundle-version.json"
        metadata = self._load_json_file(version_path)
        if not metadata:
            return None
        if not self._has_valid_cached_package(metadata):
            return None
        return metadata

    def _has_valid_cached_package(self, metadata):
        package_root = Path(metadata.get("package_root") or "").expanduser().resolve()
        return package_root.is_dir()

    def _write_current_pointer(self, metadata):
        _write_json_atomic(self.current_pointer_path, metadata)

    def _prune_cached_versions(self, keep_versions):
        keep = {str(item).strip() for item in keep_versions if str(item or "").strip()}
        if not keep:
            return
        for candidate in self.bundle_root.iterdir():
            if not candidate.is_dir():
                continue
            if candidate.name in {".tmp"}:
                continue
            if candidate.name not in keep:
                shutil.rmtree(str(candidate))

    def _load_json_file(self, path):
        path = Path(path).expanduser().resolve()
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise AirflowException("Local bundle metadata file is invalid JSON: {0}".format(path)) from exc

    @contextmanager
    def _bundle_lock(self):
        self.bundle_root.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as handle:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def prewarm_bundle_cache(
    *,
    bundle_name,
    repository_url,
    cache_root,
    username=None,
    password=None,
    verify_tls=True,
    timeout_seconds=60,
    manifest_path,
    version=None,
):
    """Download and cache one bundle version without changing Airflow configuration."""
    bundle = NexusDagBundle(
        bundle_name=bundle_name,
        manifest_path=manifest_path,
        nexus_conn_id="",
        cache_root=cache_root,
        verify_tls=verify_tls,
        repository_url=repository_url,
        timeout_seconds=timeout_seconds,
        version=version,
    )
    client = NexusArtifactClient(
        repository_url=repository_url,
        username=username,
        password=password,
        verify_tls=verify_tls,
        timeout_seconds=timeout_seconds,
    )
    bundle._resolve_connection_details = lambda: {  # type: ignore[method-assign]
        "repository_url": repository_url,
        "username": username,
        "password": password,
        "verify_tls": verify_tls,
        "timeout_seconds": timeout_seconds,
    }
    bundle._build_client = lambda: client  # type: ignore[method-assign]
    bundle.initialize()
    if version:
        metadata = bundle._ensure_version_available(version)
    else:
        bundle.refresh()
        metadata = bundle._load_current_pointer()
    if not metadata:
        raise AirflowException("No bundle version was cached for '{0}'.".format(bundle_name))
    return metadata


def _compute_sha256(path):
    sha256 = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _parse_sha256_sidecar(text):
    tokens = str(text or "").strip().split()
    if not tokens:
        raise AirflowException("Checksum sidecar is empty.")
    checksum = tokens[0].strip().lower()
    if len(checksum) != SHA256_LENGTH or any(character not in "0123456789abcdef" for character in checksum):
        raise AirflowException("Checksum sidecar does not contain a valid SHA256 value.")
    return checksum


def _write_json_atomic(target_path, payload):
    target_path = Path(target_path).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(target_path.parent),
        delete=False,
        prefix=".tmp_bundle_",
        suffix=".json",
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    temporary_path.replace(target_path)


def _extract_single_top_level_directory(archive_path, extract_root):
    archive_path = Path(archive_path).expanduser().resolve()
    extract_root = Path(extract_root).expanduser().resolve()
    if extract_root.exists():
        shutil.rmtree(str(extract_root))
    extract_root.mkdir(parents=True, exist_ok=True)

    top_levels = set()
    with zipfile.ZipFile(str(archive_path), "r") as archive:
        for member in archive.infolist():
            safe_path = _validate_member_name(member.filename, archive_path)
            if not safe_path.parts:
                continue
            if _is_zip_symlink(member):
                raise AirflowException(
                    "Archive contains an unsupported symlink entry: {0}".format(member.filename)
                )
            top_levels.add(safe_path.parts[0])
            target_path = extract_root.joinpath(*safe_path.parts)
            if member.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member, "r") as source, target_path.open("wb") as handle:
                shutil.copyfileobj(source, handle)

    if not top_levels:
        raise AirflowException("Bundle archive is empty: {0}".format(archive_path))
    if len(top_levels) != 1:
        raise AirflowException(
            "Bundle archive must contain exactly one top-level directory. Found: {0}".format(
                ", ".join(sorted(top_levels))
            )
        )
    top_level_name = sorted(top_levels)[0]
    top_level_path = extract_root / top_level_name
    if not top_level_path.is_dir():
        raise AirflowException(
            "Bundle top-level entry must be a directory, found: {0}".format(top_level_name)
        )
    return top_level_name


def _validate_member_name(member_name, archive_path):
    normalized = PurePosixPath(str(member_name))
    if normalized.is_absolute():
        raise AirflowException(
            "Bundle archive contains an absolute path entry: {0} ({1})".format(member_name, archive_path)
        )
    parts = []
    for part in normalized.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            raise AirflowException(
                "Bundle archive contains a path traversal entry: {0} ({1})".format(
                    member_name,
                    archive_path,
                )
            )
        parts.append(part)
    return PurePosixPath(*parts)


def _is_zip_symlink(member):
    mode = member.external_attr >> 16
    return (mode & 0o170000) == 0o120000
