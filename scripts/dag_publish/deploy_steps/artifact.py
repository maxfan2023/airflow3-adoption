"""Artifact fetching for DAG deployment."""

import base64
import shutil
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib import error, parse, request

from common import build_repository_object_url
from deploy_steps.exceptions import DeploymentError


@dataclass
class ArtifactReference:
    source_kind: str
    source_value: str
    local_archive_path: Path
    resolved_url: Optional[str] = None
    sidecar_content: Optional[str] = None


class ArtifactFetcher:
    """Fetch artifacts from Nexus or stage a local archive."""

    def __init__(self, repository_url, timeout_seconds, verify_tls=True, username=None, password=None):
        self.repository_url = (repository_url or "").rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.verify_tls = verify_tls
        self.username = username
        self.password = password

    def fetch(
        self,
        artifact_url=None,
        artifact_path=None,
        archive_file=None,
        download_dir=None,
        checksum_mode="compute_only",
        sidecar_suffix=".sha256",
    ):
        download_dir = Path(download_dir).expanduser().resolve()
        download_dir.mkdir(parents=True, exist_ok=True)

        if archive_file:
            return self._stage_local_archive(
                archive_file,
                download_dir,
                checksum_mode=checksum_mode,
                sidecar_suffix=sidecar_suffix,
            )
        if artifact_path:
            if not self.repository_url:
                raise DeploymentError("nexus.repository_url is required when --artifact-path is used.")
            artifact_url = build_repository_object_url(self.repository_url, artifact_path)
            return self._download_artifact(
                artifact_url,
                artifact_path,
                download_dir,
                checksum_mode=checksum_mode,
                sidecar_suffix=sidecar_suffix,
                source_kind="artifact_path",
            )
        if artifact_url:
            return self._download_artifact(
                artifact_url,
                artifact_url,
                download_dir,
                checksum_mode=checksum_mode,
                sidecar_suffix=sidecar_suffix,
                source_kind="artifact_url",
            )
        raise DeploymentError("One artifact source must be provided.")

    def _stage_local_archive(self, archive_file, download_dir, checksum_mode, sidecar_suffix):
        source_path = Path(archive_file).expanduser().resolve()
        if not source_path.is_file():
            raise DeploymentError("Archive file does not exist: {0}".format(source_path))

        target_path = download_dir / source_path.name
        if source_path != target_path:
            shutil.copy2(str(source_path), str(target_path))

        sidecar_content = None
        if checksum_mode == "sidecar_file":
            sidecar_path = Path(str(source_path) + sidecar_suffix)
            if not sidecar_path.is_file():
                raise DeploymentError(
                    "Checksum sidecar file does not exist: {0}".format(sidecar_path)
                )
            sidecar_content = sidecar_path.read_text(encoding="utf-8")

        return ArtifactReference(
            source_kind="archive_file",
            source_value=str(source_path),
            local_archive_path=target_path,
            sidecar_content=sidecar_content,
        )

    def _download_artifact(
        self,
        artifact_url,
        source_value,
        download_dir,
        checksum_mode,
        sidecar_suffix,
        source_kind,
    ):
        filename = self._filename_from_url(artifact_url)
        target_path = download_dir / filename
        self._download_to_file(artifact_url, target_path)

        sidecar_content = None
        if checksum_mode == "sidecar_file":
            sidecar_content = self._read_text(artifact_url + sidecar_suffix)

        return ArtifactReference(
            source_kind=source_kind,
            source_value=str(source_value),
            local_archive_path=target_path,
            resolved_url=artifact_url,
            sidecar_content=sidecar_content,
        )

    def _download_to_file(self, url, target_path):
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with request.urlopen(
                self._build_request(url),
                timeout=self.timeout_seconds,
                context=self._ssl_context(),
            ) as response:
                with target_path.open("wb") as handle:
                    shutil.copyfileobj(response, handle)
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace").strip()
            message = "Unable to download artifact from {0}: HTTP {1}".format(url, exc.code)
            if error_body:
                message = "{0}: {1}".format(message, error_body)
            raise DeploymentError(message) from exc
        except error.URLError as exc:
            raise DeploymentError("Unable to download artifact from {0}: {1}".format(url, exc.reason)) from exc

    def _read_text(self, url):
        try:
            with request.urlopen(
                self._build_request(url),
                timeout=self.timeout_seconds,
                context=self._ssl_context(),
            ) as response:
                return response.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace").strip()
            message = "Unable to download checksum sidecar from {0}: HTTP {1}".format(url, exc.code)
            if error_body:
                message = "{0}: {1}".format(message, error_body)
            raise DeploymentError(message) from exc
        except error.URLError as exc:
            raise DeploymentError(
                "Unable to download checksum sidecar from {0}: {1}".format(url, exc.reason)
            ) from exc

    def _build_request(self, url):
        headers = {}
        if self.username and self.password:
            auth = base64.b64encode(
                "{0}:{1}".format(self.username, self.password).encode("utf-8")
            ).decode("ascii")
            headers["Authorization"] = "Basic {0}".format(auth)
        return request.Request(url, headers=headers)

    def _ssl_context(self):
        if self.verify_tls:
            return None
        return ssl._create_unverified_context()

    def _filename_from_url(self, artifact_url):
        parsed_url = parse.urlparse(artifact_url)
        filename = Path(parse.unquote(parsed_url.path)).name
        if not filename:
            raise DeploymentError("Unable to determine filename from artifact URL: {0}".format(artifact_url))
        return filename
