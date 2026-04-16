"""Landing and live DAG publication helpers."""

import shutil
from dataclasses import dataclass
from pathlib import Path

from deploy_steps.exceptions import DeploymentError


@dataclass
class PublishResult:
    release_name: str
    landing_release_dir: Path
    live_target_dir: Path
    backup_dir: Path
    dry_run: bool = False


class DeploymentPublisher:
    """Copy processed content to landing and promote it into the live DAG folder."""

    def __init__(self, landing_root, dags_root, backup_root):
        self.landing_root = Path(landing_root).expanduser().resolve()
        self.dags_root = Path(dags_root).expanduser().resolve()
        self.backup_root = Path(backup_root).expanduser().resolve()

    def publish(self, package_root, release_name, dry_run=False):
        package_root = Path(package_root).expanduser().resolve()
        landing_release_dir = self.landing_root / release_name / package_root.name
        live_target_dir = self.dags_root / package_root.name
        backup_dir = self.backup_root / "{0}.{1}.bak".format(package_root.name, release_name)

        if dry_run:
            return PublishResult(
                release_name=release_name,
                landing_release_dir=landing_release_dir,
                live_target_dir=live_target_dir,
                backup_dir=backup_dir,
                dry_run=True,
            )

        self._copy_to_landing(package_root, landing_release_dir)
        self._promote_to_live(landing_release_dir, live_target_dir, backup_dir, release_name)
        return PublishResult(
            release_name=release_name,
            landing_release_dir=landing_release_dir,
            live_target_dir=live_target_dir,
            backup_dir=backup_dir,
            dry_run=False,
        )

    def _copy_to_landing(self, package_root, landing_release_dir):
        landing_parent = landing_release_dir.parent
        landing_parent.mkdir(parents=True, exist_ok=True)
        if landing_release_dir.exists():
            shutil.rmtree(str(landing_release_dir))
        shutil.copytree(str(package_root), str(landing_release_dir))

    def _promote_to_live(self, landing_release_dir, live_target_dir, backup_dir, release_name):
        self.dags_root.mkdir(parents=True, exist_ok=True)
        self.backup_root.mkdir(parents=True, exist_ok=True)
        incoming_parent = self.dags_root / ".incoming" / release_name
        incoming_target = incoming_parent / live_target_dir.name

        if incoming_parent.exists():
            shutil.rmtree(str(incoming_parent))
        incoming_parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(str(landing_release_dir), str(incoming_target))

        backup_created = False
        try:
            if live_target_dir.exists():
                if backup_dir.exists():
                    raise DeploymentError("Backup path already exists: {0}".format(backup_dir))
                live_target_dir.rename(backup_dir)
                backup_created = True
            incoming_target.rename(live_target_dir)
        except Exception as exc:
            if backup_created and not live_target_dir.exists() and backup_dir.exists():
                backup_dir.rename(live_target_dir)
            raise DeploymentError(
                "Failed to publish DAG directory to {0}: {1}".format(
                    live_target_dir,
                    exc,
                )
            ) from exc
        finally:
            if incoming_parent.exists():
                shutil.rmtree(str(incoming_parent))
