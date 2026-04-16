"""Shared exceptions for the DAG deployment pipeline."""


class DeploymentError(Exception):
    """Raised when the deployment pipeline cannot safely continue."""

