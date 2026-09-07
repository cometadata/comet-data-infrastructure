"""Publishing enrichment releases to the Hugging Face export bucket."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import os
import shutil
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from comet.aws import (
    local_dir_for_uri,
    s3_uri,
    s3_uri_has_files,
    s5cmd_clean_prefix,
    s5cmd_download_files,
    s5cmd_upload_files,
)
from comet.constants import DIFF_RELEASE_TYPE, FULL_RELEASE_TYPE, Enrichment, enrichments_for_source
from comet.dynamodb_store import (
    DatasetReleaseRecord,
    get_latest_published_release,
    get_release,
    list_releases,
    mark_published,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from botocore.client import BaseClient

log = logging.getLogger(__name__)

INDEX_SCHEMA_VERSION = 1
HF_S3_REGION = "us-east-1"


def index_key(source: str) -> str:
    """Return the release index key for a source on the export bucket."""
    return f"{source}/index.json"


def release_prefix(enrichment: Enrichment, release_date: str, release_type: str) -> str:
    """Return the export bucket prefix for a full or diff release.

    Args:
        enrichment: The enrichment being published.
        release_date: ISO date string "YYYY-MM-DD".
        release_type: ``FULL_RELEASE_TYPE`` or ``DIFF_RELEASE_TYPE``.

    Returns:
        The key prefix, e.g. "datacite/funders/2026-01-02/full/".
    """
    return f"{enrichment.source.identifier}/{enrichment.method}/{release_date}/{release_type}/"


def hf_credentials() -> tuple[str, str]:
    """Return the Hugging Face bucket key pair from the environment.

    Returns:
        The access key ID and secret access key.

    Raises:
        RuntimeError: If the Hugging Face credential variables are not set.
    """
    access_key_id = os.environ.get("HF_S3_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("HF_S3_SECRET_ACCESS_KEY")
    if not access_key_id or not secret_access_key:
        raise RuntimeError("HF_S3_ACCESS_KEY_ID and HF_S3_SECRET_ACCESS_KEY must be set")
    return access_key_id, secret_access_key


def hf_env() -> dict[str, str]:
    """Return a subprocess environment authenticating as the Hugging Face bucket user.

    The Batch job definition injects the Hugging Face keys as ``HF_S3_*`` variables so they
    cannot shadow the AWS task role; this remaps them to the standard names for the
    subprocesses that talk to the Hugging Face endpoint.

    Returns:
        A copy of the environment with the Hugging Face keys under the standard AWS names.

    Raises:
        RuntimeError: If the Hugging Face credential variables are not set.
    """
    access_key_id, secret_access_key = hf_credentials()
    env = os.environ.copy()
    env.update(
        {
            "AWS_ACCESS_KEY_ID": access_key_id,
            "AWS_SECRET_ACCESS_KEY": secret_access_key,
            "AWS_REGION": HF_S3_REGION,
        }
    )
    # A leftover session token would be sent with the Hugging Face keys and fail auth.
    env.pop("AWS_SESSION_TOKEN", None)
    return env


def hf_s3_client(endpoint_url: str) -> BaseClient:
    """Return a boto3 S3 client for the Hugging Face endpoint.

    Args:
        endpoint_url: The S3-compatible endpoint URL.

    Returns:
        A boto3 S3 client authenticated with the Hugging Face keys.
    """
    access_key_id, secret_access_key = hf_credentials()
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(
            region_name=HF_S3_REGION,
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def indexed_release_prefixes(*, source: str, hf_bucket: str, s3_client: BaseClient) -> set[str]:
    """Return the release prefixes referenced by the source's live index.

    Args:
        source: The source dataset name, e.g. "datacite".
        hf_bucket: The Hugging Face bucket name.
        s3_client: Client for the Hugging Face endpoint.

    Returns:
        The referenced key prefixes without trailing slashes; empty when no index exists.
    """
    try:
        response = s3_client.get_object(Bucket=hf_bucket, Key=index_key(source))
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return set()
        raise

    index = json.loads(response["Body"].read())
    return {
        entry.get("path", "").rstrip("/")
        for method in index.get("datasets", {}).get(source, {}).values()
        for entry in [method.get("latest", {}), *method.get("releases", [])]
    }


def copy_release_to_hf(
    *,
    source_uri: str,
    hf_bucket: str,
    hf_prefix: str,
    endpoint_url: str,
    s3_client: BaseClient,
    env: dict[str, str],
) -> None:
    """Copy one release directory from the data bucket to the Hugging Face bucket.

    Stages files locally and clears the target folder before uploading to remove
    stale files from previous attempts. Removes local files on success or failure.

    Args:
        source_uri: S3 URI of the release directory on the data bucket (trailing slash).
        hf_bucket: The Hugging Face bucket name.
        hf_prefix: The release folder key prefix, from :func:`release_prefix`.
        endpoint_url: The Hugging Face S3-compatible endpoint URL.
        s3_client: Client for the Hugging Face endpoint.
        env: Subprocess environment from :func:`hf_env`.

    Raises:
        RuntimeError: If the source directory has no ``manifest.json``.
    """
    if not s3_uri_has_files(f"{source_uri}manifest.json"):
        raise RuntimeError(f"No manifest.json under {source_uri}; refusing to publish an incomplete release")
    target_uri = s3_uri(hf_bucket, hf_prefix)
    stage_dir = local_dir_for_uri(target_uri)
    shutil.rmtree(stage_dir, ignore_errors=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    try:
        s5cmd_download_files(f"{source_uri}*", stage_dir)
        s5cmd_clean_prefix(target_uri, s3_client=s3_client, endpoint_url=endpoint_url, env=env)
        s5cmd_upload_files(stage_dir, target_uri, endpoint_url=endpoint_url, env=env)
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)


def index_entry(record: DatasetReleaseRecord, release_type: str, path: str) -> dict:
    """Return one release index entry.

    Args:
        record: The published release record.
        release_type: ``FULL_RELEASE_TYPE`` or ``DIFF_RELEASE_TYPE``.
        path: The release's key prefix on the export bucket.
    """
    return {
        "release_date": record.release_date,
        "type": release_type,
        "path": path,
        "published_at": record.published_at,
    }


def render_index(source: str, records_by_enrichment: Mapping[Enrichment, Sequence[DatasetReleaseRecord]]) -> dict:
    """Render a source's release index from its enrichment release records.

    Only published releases appear; enrichments with nothing published are omitted.

    Args:
        source: The source dataset name, e.g. "datacite".
        records_by_enrichment: Release records keyed by enrichment.

    Returns:
        The index document.
    """
    datasets: dict[str, dict] = {}
    for enrichment, records in records_by_enrichment.items():
        published = sorted((r for r in records if r.published_at), key=lambda r: r.release_date)
        # Put each full release last so latest below points to a full snapshot.
        entries: list[dict] = []
        for record in published:
            if record.diff_export_path:
                entries.append(index_entry(record, DIFF_RELEASE_TYPE, record.diff_export_path))
            entries.append(index_entry(record, FULL_RELEASE_TYPE, record.export_path))
        if not entries:
            continue
        latest = {key: entries[-1][key] for key in ("release_date", "type", "path")}
        datasets[enrichment.method] = {"latest": latest, "releases": entries}

    return {
        "schema_version": INDEX_SCHEMA_VERSION,
        "updated_at": datetime.now(UTC).isoformat(),
        "datasets": {source: datasets},
    }


def publish_index(*, source: str, hf_bucket: str, s3_client: BaseClient) -> None:
    """Render a source's index from the releases table and upload it to the Hugging Face bucket.

    This is the commit point: consumers discover releases only through the index, so it is
    uploaded after all release files are in place.

    Args:
        source: The source dataset name, e.g. "datacite".
        hf_bucket: The Hugging Face bucket name.
        s3_client: Client for the Hugging Face endpoint.
    """
    records_by_enrichment = {e: list_releases(dataset=e.identifier) for e in enrichments_for_source(source)}
    index = render_index(source, records_by_enrichment)
    body = json.dumps(index, indent=2).encode("utf-8")
    log.info(f"Uploading index for {len(index['datasets'][source])} datasets to {index_key(source)}")
    s3_client.put_object(Bucket=hf_bucket, Key=index_key(source), Body=body, ContentType="application/json")


def publish_releases(
    *,
    source: str,
    release_date: str,
    datasets: Sequence[str],
    data_bucket: str,
    hf_bucket: str,
    endpoint_url: str,
) -> None:
    """Publish a source's enrichment releases for a snapshot date, then commit the index.

    Skips datasets already marked published. Marks each remaining dataset published
    after copying its full and any diff directory. A retry replaces both directories
    if the previous attempt failed before marking the dataset published.

    Args:
        source: The source dataset name, e.g. "datacite".
        release_date: ISO date string "YYYY-MM-DD" of the snapshot to publish.
        datasets: The dataset identifiers to publish, e.g. "datacite-funders".
        data_bucket: The data bucket holding the release directories.
        hf_bucket: The Hugging Face bucket name.
        endpoint_url: The Hugging Face S3-compatible endpoint URL.

    Raises:
        ValueError: If the source is unknown.
        RuntimeError: If a dataset is unknown, has no release record for ``release_date``,
            has no full directory, has a diff whose baseline is not the newest published
            release, or its release prefix is referenced by the live index and was not
            published from this record.
    """
    enrichments = {e.identifier: e for e in enrichments_for_source(source)}
    unknown = set(datasets) - set(enrichments)
    if unknown:
        raise RuntimeError(f"Unknown dataset(s): {', '.join(sorted(unknown))}")

    # Validate the credentials and reach the bucket before the expensive copies.
    env = hf_env()
    s3_client = hf_s3_client(endpoint_url)
    indexed = indexed_release_prefixes(source=source, hf_bucket=hf_bucket, s3_client=s3_client)

    for dataset in datasets:
        record = get_release(dataset=dataset, release_date=release_date)
        if record is None:
            raise RuntimeError(f"No release record for {dataset}/{release_date}")
        if record.published_at:
            log.info(f"Skipping {dataset} {release_date}: already published")
            continue
        if not record.full_source_prefix:
            raise RuntimeError(f"Release record for {dataset}/{release_date} has no full_source_prefix")
        if record.diff_source_prefix:
            newest = get_latest_published_release(dataset=dataset)
            if newest is None or newest.release_date != record.diff_base_release_date:
                raise RuntimeError(
                    f"Diff for {dataset}/{release_date} was cut against {record.diff_base_release_date} "
                    f"but the newest published release is {newest.release_date if newest else 'none'}"
                )
        enrichment = enrichments[dataset]

        artifacts = [(FULL_RELEASE_TYPE, record.full_source_prefix)]
        if record.diff_source_prefix:
            artifacts.append((DIFF_RELEASE_TYPE, record.diff_source_prefix))
        # A re-run with replace_published keeps the export paths it may overwrite.
        own_prefixes = {record.export_path, record.diff_export_path}
        export_paths = {}
        for release_type, source_prefix in artifacts:
            hf_prefix = release_prefix(enrichment, release_date, release_type)
            if hf_prefix.rstrip("/") in indexed and hf_prefix not in own_prefixes:
                raise RuntimeError(
                    f"Release prefix {s3_uri(hf_bucket, hf_prefix)} is already referenced by {index_key(source)}"
                )
            copy_release_to_hf(
                source_uri=s3_uri(data_bucket, source_prefix),
                hf_bucket=hf_bucket,
                hf_prefix=hf_prefix,
                endpoint_url=endpoint_url,
                s3_client=s3_client,
                env=env,
            )
            export_paths[release_type] = hf_prefix
        mark_published(
            dataset=dataset,
            release_date=release_date,
            export_path=export_paths[FULL_RELEASE_TYPE],
            diff_export_path=export_paths.get(DIFF_RELEASE_TYPE),
            expected_updated_at=record.updated_at,
        )
    publish_index(source=source, hf_bucket=hf_bucket, s3_client=s3_client)
