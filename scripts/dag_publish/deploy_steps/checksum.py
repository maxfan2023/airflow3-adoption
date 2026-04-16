"""Checksum handling for DAG deployment."""

import hashlib
import re
from dataclasses import dataclass

from deploy_steps.exceptions import DeploymentError


SHA256_PATTERN = re.compile(r"\b([0-9a-fA-F]{64})\b")


@dataclass
class ChecksumResult:
    actual_sha256: str
    expected_sha256: str = ""
    mode: str = "compute_only"


class ChecksumValidator:
    """Validate or compute checksums based on pipeline configuration."""

    def __init__(self, mode, sidecar_suffix=".sha256"):
        self.mode = mode
        self.sidecar_suffix = sidecar_suffix

    def validate(self, archive_path, expected_sha256=None, sidecar_content=None):
        actual_sha256 = self.compute_sha256(archive_path)
        expected = ""

        if self.mode == "compute_only":
            return ChecksumResult(actual_sha256=actual_sha256, mode=self.mode)
        if self.mode == "cli_value":
            expected = self._normalize_hash(expected_sha256, "CLI")
        elif self.mode == "sidecar_file":
            if not sidecar_content:
                raise DeploymentError("Checksum sidecar content is required in sidecar_file mode.")
            expected = self._normalize_hash(self._extract_hash_from_text(sidecar_content), "sidecar")
        else:
            raise DeploymentError("Unsupported checksum mode: {0}".format(self.mode))

        if actual_sha256 != expected:
            raise DeploymentError(
                "Checksum mismatch: expected {0}, got {1}".format(expected, actual_sha256)
            )
        return ChecksumResult(actual_sha256=actual_sha256, expected_sha256=expected, mode=self.mode)

    @staticmethod
    def compute_sha256(archive_path):
        sha256 = hashlib.sha256()
        with open(archive_path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _extract_hash_from_text(self, text):
        match = SHA256_PATTERN.search(text)
        if not match:
            raise DeploymentError("Checksum sidecar does not contain a valid SHA256 value.")
        return match.group(1)

    def _normalize_hash(self, value, source_label):
        if not value:
            raise DeploymentError("{0} checksum value is required.".format(source_label))
        normalized = str(value).strip().lower()
        if not SHA256_PATTERN.fullmatch(normalized):
            raise DeploymentError("{0} checksum is not a valid SHA256 value.".format(source_label))
        return normalized
