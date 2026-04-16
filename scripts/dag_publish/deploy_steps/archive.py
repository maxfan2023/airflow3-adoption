"""Archive extraction and safety checks for DAG deployment."""

import shutil
import stat
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

from deploy_steps.exceptions import DeploymentError


def strip_archive_suffix(filename, allowed_suffixes):
    """Strip the longest configured archive suffix from a filename."""
    normalized_name = str(filename)
    for suffix in sorted(allowed_suffixes, key=len, reverse=True):
        if normalized_name.endswith(suffix):
            return normalized_name[: -len(suffix)]
    raise DeploymentError("Unsupported archive format: {0}".format(filename))


class ArchiveExtractor:
    """Safely unpack supported archive types."""

    def __init__(self, allowed_suffixes, require_single_top_level_dir=True):
        self.allowed_suffixes = list(allowed_suffixes)
        self.require_single_top_level_dir = require_single_top_level_dir

    def extract(self, archive_path, extract_root):
        archive_path = Path(archive_path).expanduser().resolve()
        extract_root = Path(extract_root).expanduser().resolve()
        if extract_root.exists():
            shutil.rmtree(str(extract_root))
        extract_root.mkdir(parents=True, exist_ok=True)

        suffix = self._match_suffix(archive_path.name)
        if suffix == ".zip":
            top_levels = self._extract_zip(archive_path, extract_root)
        elif suffix in {".tar.gz", ".tgz"}:
            top_levels = self._extract_tar(archive_path, extract_root)
        else:
            raise DeploymentError("Unsupported archive format: {0}".format(archive_path.name))

        if not top_levels:
            raise DeploymentError("Archive is empty: {0}".format(archive_path))

        if self.require_single_top_level_dir and len(top_levels) != 1:
            raise DeploymentError(
                "Archive must contain exactly one top-level directory. Found: {0}".format(
                    ", ".join(sorted(top_levels))
                )
            )

        top_level_name = sorted(top_levels)[0]
        top_level_path = extract_root / top_level_name
        if self.require_single_top_level_dir and not top_level_path.is_dir():
            raise DeploymentError(
                "Archive top-level entry must be a directory, found: {0}".format(top_level_name)
            )
        return top_level_path

    def _extract_zip(self, archive_path, extract_root):
        top_levels = set()
        with zipfile.ZipFile(str(archive_path), "r") as archive:
            for member in archive.infolist():
                safe_path = self._validate_member_name(member.filename, archive_path)
                if not safe_path.parts:
                    continue
                if self._is_zip_symlink(member):
                    raise DeploymentError(
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
        return top_levels

    def _extract_tar(self, archive_path, extract_root):
        top_levels = set()
        with tarfile.open(str(archive_path), "r:*") as archive:
            for member in archive.getmembers():
                safe_path = self._validate_member_name(member.name, archive_path)
                if not safe_path.parts:
                    continue
                if member.issym() or member.islnk():
                    raise DeploymentError(
                        "Archive contains an unsupported symlink entry: {0}".format(member.name)
                    )
                if not (member.isfile() or member.isdir()):
                    raise DeploymentError(
                        "Archive contains an unsupported entry type: {0}".format(member.name)
                    )
                top_levels.add(safe_path.parts[0])
                target_path = extract_root.joinpath(*safe_path.parts)
                if member.isdir():
                    target_path.mkdir(parents=True, exist_ok=True)
                    continue
                target_path.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise DeploymentError("Unable to read archive member: {0}".format(member.name))
                with source, target_path.open("wb") as handle:
                    shutil.copyfileobj(source, handle)
        return top_levels

    def _match_suffix(self, filename):
        for suffix in sorted(self.allowed_suffixes, key=len, reverse=True):
            if filename.endswith(suffix):
                return suffix
        raise DeploymentError(
            "Archive suffix is not allowed for file '{0}'. Allowed suffixes: {1}".format(
                filename,
                ", ".join(self.allowed_suffixes),
            )
        )

    def _validate_member_name(self, member_name, archive_path):
        normalized = PurePosixPath(str(member_name))
        if normalized.is_absolute():
            raise DeploymentError(
                "Archive contains an absolute path entry: {0} ({1})".format(member_name, archive_path)
            )
        parts = []
        for part in normalized.parts:
            if part in {"", "."}:
                continue
            if part == "..":
                raise DeploymentError(
                    "Archive contains a path traversal entry: {0} ({1})".format(
                        member_name,
                        archive_path,
                    )
                )
            parts.append(part)
        return PurePosixPath(*parts)

    def _is_zip_symlink(self, member):
        mode = member.external_attr >> 16
        return stat.S_ISLNK(mode)
