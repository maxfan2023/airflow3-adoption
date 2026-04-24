# Nexus DAG Bundle Flow Overview

## Executive Summary

This document explains how DAG packages are published to Nexus and how Airflow later downloads and caches those DAG packages for use at runtime.

In simple terms, the implementation creates a controlled delivery path:

1. A DAG package is validated, compressed, and uploaded to Nexus.
2. Nexus stores the package, checksum, and release metadata.
3. Airflow reads the Nexus metadata, downloads the correct package, verifies it, and stores it in a local cache.
4. Airflow then imports DAGs from the verified local cache instead of relying on a direct file copy process.

The result is a more traceable, repeatable, and controlled DAG release process.

## Audience

This document is intended for readers who need to understand the process at a business or operational level. It does not require prior knowledge of the source code.

## Related Flowcharts

### DAG Upload To Nexus

![DAG Upload To Nexus](flowcharts/dag-upload-to-nexus-flow.drawio.svg)

Editable source: [dag-upload-to-nexus-flow.drawio](flowcharts/dag-upload-to-nexus-flow.drawio)

### Airflow Nexus DAG Cache

![Airflow Nexus DAG Cache](flowcharts/airflow-nexus-dag-cache-flow.drawio.svg)

Editable source: [airflow-nexus-dag-cache-flow.drawio](flowcharts/airflow-nexus-dag-cache-flow.drawio)

## Business Objective

The objective is to manage Airflow DAG releases through Nexus as a controlled artifact repository.

This provides several business benefits:

- Release packages are stored in a central repository.
- Each release has metadata, including version, change ticket, source commit, release time, and operator.
- Airflow can retrieve DAGs in a consistent way across environments.
- Integrity checks reduce the risk of running an incomplete or modified package.
- Local caching reduces repeated network dependency after a package has been downloaded.

## Flow 1: Uploading A DAG Package To Nexus

The upload process is implemented mainly in `scripts/dag_publish/package_and_upload_dag.py`.

At a high level, the script performs the following actions.

### 1. Resolve Inputs

The operator runs the upload script with the DAG source path, artifact id, version, and target environment.

The script then resolves:

- the deployment environment, such as `dev`, `uat`, or `prod`
- the Nexus credentials file
- the source files to package
- the artifact id and bundle name
- the release version

### 2. Validate The DAG Package

Before uploading anything, the script performs validation checks.

The validation includes:

- Python syntax validation for packaged `.py` files
- Airflow import validation using a temporary Airflow environment
- DAG rule validation, such as DAG naming rules, queue rules, and required top-level variables

If issues are found, the operator is shown the findings. In some cases, the process can continue only after the operator explicitly confirms that the issue should be overridden.

### 3. Create The ZIP Archive

After validation, the script creates a ZIP archive.

The archive keeps the intended package structure so that Airflow can later extract and use the DAG bundle predictably.

The script also calculates a SHA256 checksum for the archive. This checksum is a digital fingerprint used later to confirm that the downloaded package is exactly the same as the uploaded package.

### 4. Build Release Metadata

The script creates several metadata records.

These records describe the release and make it discoverable by Airflow:

- **Version record**: describes a specific version of the bundle.
- **Release record**: records release audit information, including release time and change ticket.
- **Latest manifest**: points Airflow to the latest approved version of the bundle.
- **Checksum sidecar**: stores the expected SHA256 checksum beside the ZIP archive.

### 5. Upload To Nexus

If the command is not running in dry-run mode, the script uploads the following objects to Nexus:

- the DAG bundle ZIP file
- the `.sha256` checksum file
- the version record JSON
- the release record JSON
- the latest manifest JSON

If dry-run mode is enabled, the script prepares the archive and prints the target Nexus URLs without uploading.

## Flow 2: Airflow Downloading And Caching A DAG Package

The runtime download and cache behavior is implemented mainly in `my_company/airflow_bundles/nexus.py`.

At a high level, Airflow uses a `NexusDagBundle` implementation to resolve the correct DAG package from Nexus and store it in a local cache.

### 1. Airflow Requests The DAG Bundle Path

When Airflow needs to parse DAGs, it asks the bundle implementation for a local path.

The bundle first initializes its cache directory and lock file. The lock prevents multiple Airflow processes from downloading and extracting the same package at the same time.

### 2. Resolve The Required Version

The bundle can operate in two modes:

- **Latest version mode**: read `latest.json` from Nexus.
- **Pinned version mode**: read the specific version record from Nexus.

The manifest tells Airflow which archive path to download and what checksum to expect.

### 3. Reuse Cache When Possible

Before downloading, the bundle checks whether the requested version is already available in the local cache.

If a valid cached package exists, Airflow uses it immediately.

### 4. Download And Verify

If the package is not already cached, the bundle downloads:

- the ZIP archive
- the `.sha256` checksum sidecar

It then verifies:

- the checksum sidecar matches the manifest
- the downloaded archive checksum matches the manifest

If the checks do not match, the package is rejected.

### 5. Extract Safely

The archive is extracted into a temporary location first.

The implementation verifies that the archive has one top-level directory and rejects unsafe entries, such as paths that try to escape the extraction folder.

Only after these checks pass is the extracted bundle moved into the final cache location.

### 6. Update The Local Pointer

After a successful cache update, the bundle writes local metadata:

- `.bundle-version.json` for the cached version
- `current.json` to point to the active version

This allows Airflow to quickly reuse the cached package on later runs.

### 7. Fall Back When Appropriate

If Airflow cannot read Nexus or cannot cache a newly discovered latest version, it can continue using the current valid cached version.

If no valid cached version exists, the process raises an error rather than running an unverified package.

## Key Implementation Files

| File | Purpose |
| --- | --- |
| `scripts/dag_publish/package_and_upload_dag.py` | Packages DAG sources, validates them, creates metadata, and uploads release objects to Nexus. |
| `my_company/airflow_bundles/nexus.py` | Implements the Airflow-side Nexus bundle reader, downloader, verifier, extractor, and cache manager. |
| `scripts/dag_publish/common.py` | Provides shared helpers for paths, environment handling, release metadata paths, and Git commit resolution. |
| `scripts/dag_publish/prewarm_dag_bundle_cache.py` | Allows an Airflow node to pre-download a bundle into cache before runtime usage. |
| `docs/dag_publish/flowcharts/` | Stores editable draw.io diagrams and exported SVG files. |

## Controls And Safeguards

The implementation includes several safeguards:

- **Validation before upload**: checks Python syntax, Airflow import behavior, and configured DAG rules.
- **Release traceability**: records version, change ticket, source commit, release time, and operator.
- **Checksum verification**: confirms package integrity during download.
- **Safe extraction**: prevents unsafe archive paths from being extracted.
- **Cache locking**: avoids concurrent cache writes.
- **Fallback behavior**: allows Airflow to keep using a known valid cached version if a new latest version cannot be downloaded.
- **Dry-run support**: allows operators to review the target archive and Nexus paths before upload.

## Expected Operational Outcome

This design makes DAG release management more controlled and auditable.

Operators can publish DAG bundles to Nexus with validation and release metadata. Airflow can then retrieve the correct package, verify its integrity, cache it locally, and run from a stable local directory.

The approach reduces manual file movement, improves release traceability, and provides a safer path for adopting Nexus-backed Airflow DAG distribution.
