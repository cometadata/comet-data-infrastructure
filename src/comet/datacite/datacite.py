import datetime
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError
import pendulum
import requests

from comet.aws import download_source_task, s3_uri
from comet.model.dataset_version_model import DatasetRelease
from comet.utils import run_process

logger = logging.getLogger(__name__)


def fetch_datacite_aws_credentials(
    *, account_id: str | None = None, password: str | None = None
) -> tuple[str, str, str]:
    """Fetch temporary AWS credentials from the DataCite API.

    Uses ``account_id``/``password`` or their env var fallbacks to authenticate.

    Args:
        account_id: DataCite account ID. Falls back to ``DATACITE_ACCOUNT_ID`` env var if not provided.
        password: DataCite password. Falls back to ``DATACITE_PASSWORD`` env var if not provided.

    Returns:
        A tuple containing (access_key_id, secret_access_key, session_token).

    Raises:
        RuntimeError: If credentials are missing or the API request fails.
    """
    account_id = account_id or os.getenv("DATACITE_ACCOUNT_ID")
    password = password or os.getenv("DATACITE_PASSWORD")

    if not account_id or not password:
        raise RuntimeError(
            "DataCite account_id and password must be provided or set via DATACITE_ACCOUNT_ID and DATACITE_PASSWORD environment variables."
        )

    url = "https://api.datacite.org/credentials/datafile"

    try:
        response = requests.get(url, auth=(account_id, password), timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError("Failed to fetch DataCite credentials") from e

    try:
        data = response.json()
        access_key_id = data["access_key_id"]
        secret_access_key = data["secret_access_key"]
        session_token = data["session_token"]
    except (KeyError, ValueError) as e:
        raise RuntimeError("Unexpected response format from DataCite credentials endpoint.") from e

    return access_key_id, secret_access_key, session_token


def get_new_datacite_release(
    *,
    datacite_bucket_name: str,
    datacite_bucket_region: str,
    published_after: pendulum.DateTime | None = None,
    account_id: str | None = None,
    password: str | None = None,
) -> DatasetRelease | None:
    """Return the newest DataCite release published strictly after ``published_after``.

    The release is detected by reading the DataCite S3 bucket's STATUS.json, which requires
    credentials obtained via ``fetch_datacite_aws_credentials`` (the same mechanism used by the
    download job).

    Args:
        datacite_bucket_name: Name of the DataCite S3 bucket.
        datacite_bucket_region: Region of the DataCite S3 bucket.
        published_after: Release date of the most recent known version. Only releases published
            strictly after this date are considered. If None, no lower bound is applied.
        account_id: DataCite account ID. Falls back to ``DATACITE_ACCOUNT_ID`` env var if not provided.
        password: DataCite password. Falls back to ``DATACITE_PASSWORD`` env var if not provided.

    Returns:
        DatasetRelease with release_date set, or None if unavailable or no newer release.
    """
    try:
        access_key_id, secret_access_key, session_token = fetch_datacite_aws_credentials(
            account_id=account_id, password=password
        )
    except RuntimeError:
        logger.exception("Failed to obtain DataCite AWS credentials")
        return None

    s3 = boto3.client(
        "s3",
        region_name=datacite_bucket_region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
    )
    try:
        resp = s3.get_object(Bucket=datacite_bucket_name, Key="STATUS.json")
        status = json.loads(resp["Body"].read())
    except (ClientError, json.JSONDecodeError):
        logger.exception("Failed to read DataCite STATUS.json")
        return None

    if status.get("status") != "Complete":
        logger.info(f"DataCite status is not Complete: {status.get('status')}")
        return None

    dt_str = status.get("datetime")
    if not dt_str:
        return None

    try:
        release_date = datetime.datetime.fromisoformat(dt_str).date()
    except (TypeError, ValueError):
        logger.exception(f"Failed to parse DataCite datetime from STATUS.json: {dt_str}")
        return None

    logger.info(f"DataCite STATUS.json: status={status.get('status')} release_date={release_date}")

    if published_after is not None and release_date <= published_after.date():
        return None

    file_count, total_bytes = fetch_datacite_manifest_stats(s3, datacite_bucket_name)
    logger.info(f"DataCite MANIFEST.json: file_count={file_count} total_size_bytes={total_bytes}")
    return DatasetRelease(
        release_date=release_date,
        metadata={"file_count": str(file_count), "total_size_bytes": str(total_bytes)},
    )


def fetch_datacite_manifest_stats(s3_client, datacite_bucket_name: str) -> tuple[int, int]:
    """Return the ``.jsonl.gz`` file count and total size (bytes) from DataCite's MANIFEST.json.

    MANIFEST.json is a JSON array of ``{"filename", "size", "sha256"}`` (size in bytes) — the
    authoritative per-file manifest of the snapshot. Only ``.jsonl.gz`` entries are counted, to
    match what we download and enrich.

    Args:
        s3_client: A boto3 S3 client authorized for the DataCite bucket.
        datacite_bucket_name: Name of the DataCite S3 bucket.

    Returns:
        A tuple of (file_count, total_size_bytes).

    Raises:
        RuntimeError: If MANIFEST.json cannot be read or parsed.
    """
    try:
        resp = s3_client.get_object(Bucket=datacite_bucket_name, Key="MANIFEST.json")
        entries = json.loads(resp["Body"].read())
        count = 0
        total = 0
        for entry in entries:
            if entry["filename"].endswith(".jsonl.gz"):
                count += 1
                total += entry["size"]
    except (ClientError, json.JSONDecodeError, KeyError, TypeError) as e:
        raise RuntimeError("Failed to read DataCite MANIFEST.json") from e
    return count, total


def snapshot_stats(release: DatasetRelease) -> tuple[int, int] | None:
    """Return (file_count, total_size_bytes) stored on a release, or None if absent."""
    meta = release.metadata
    if "file_count" in meta and "total_size_bytes" in meta:
        return int(meta["file_count"]), int(meta["total_size_bytes"])
    return None


def release_is_smaller(new: DatasetRelease, last: DatasetRelease) -> bool:
    """Return True if ``new`` has fewer .jsonl.gz files or fewer bytes than ``last``.

    DataCite's monthly export is cumulative, so a shrink signals an incomplete snapshot. Returns
    False when ``last`` has no stored stats (a record persisted before this check existed).
    """
    new_stats = snapshot_stats(new)
    last_stats = snapshot_stats(last)
    if new_stats is None or last_stats is None:
        return False
    return new_stats[0] < last_stats[0] or new_stats[1] < last_stats[1]


def download_datacite(
    *,
    target_uri: str,
    datacite_bucket_name: str,
    expected_file_count: int,
    expected_total_bytes: int,
):
    """Download DataCite from the DataCite S3 bucket and upload to the DMP Tool S3 bucket.

    Verifies the downloaded ``.jsonl.gz`` files match the manifest stats computed at detection
    before the upload runs, so a partial copy is never published.

    Args:
        target_uri: S3 URI to upload the DataCite snapshot to (trailing slash).
        datacite_bucket_name: the name of the DataCite AWS S3 bucket.
        expected_file_count: Expected number of .jsonl.gz files (from MANIFEST.json).
        expected_total_bytes: Expected total size of the .jsonl.gz files in bytes.
    """
    access_key_id, secret_access_key, session_token = fetch_datacite_aws_credentials()
    env = os.environ.copy()
    env.update(
        {
            "AWS_ACCESS_KEY_ID": access_key_id,
            "AWS_SECRET_ACCESS_KEY": secret_access_key,
            "AWS_SESSION_TOKEN": session_token,
        }
    )

    with download_source_task(target_uri) as ctx:
        run_process(
            [
                "s5cmd",
                "cp",
                # Skip the per-month .csv.gz duplicates. s5cmd's `*` matches across `/`.
                s3_uri(datacite_bucket_name, "dois/*.jsonl.gz"),
                f"{ctx.download_dir}/",
            ],
            env=env,
        )

        # Verify against the manifest before uploading.
        files = list(ctx.download_dir.rglob("*.jsonl.gz"))
        actual_count = len(files)
        actual_bytes = sum(f.stat().st_size for f in files)
        logger.info(
            f"Verifying download against manifest: actual {actual_count} files / {actual_bytes} bytes, "
            f"expected {expected_file_count} files / {expected_total_bytes} bytes"
        )
        if (actual_count, actual_bytes) != (expected_file_count, expected_total_bytes):
            raise ValueError(
                f"DataCite download incomplete: got {actual_count} files / {actual_bytes} bytes; "
                f"manifest expects {expected_file_count} / {expected_total_bytes}"
            )
        logger.info("DataCite download matches the manifest (file count and total bytes equal)")
