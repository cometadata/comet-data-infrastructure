"""Publishing enrichment releases to the Hugging Face export bucket."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import os
import shutil
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

from comet.aws import clean_s3_prefix, download_files_from_s3, local_dir_for_uri, s3_uri, upload_files_to_s3
from comet.constants import Enrichment, enrichments_for_source
from comet.dynamodb_store import DatasetReleaseRecord, get_release, list_releases, mark_published

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from botocore.client import BaseClient

log = logging.getLogger(__name__)

INDEX_SCHEMA_VERSION = 1

# Shared by the release folder path and the recorded release type so they cannot disagree.
FULL_RELEASE_TYPE = "full"


def index_key(source: str) -> str:
    """Return the release index key for a source on the export bucket."""
    return f"{source}/index.json"


def full_release_prefix(enrichment: Enrichment, release_date: str) -> str:
    """Return the export bucket key prefix for an enrichment's full release.

    Args:
        enrichment: The enrichment being published.
        release_date: ISO date string "YYYY-MM-DD".

    Returns:
        The key prefix, e.g. "datacite/funders/2026-01-02/full/".
    """
    return f"{enrichment.source}/{enrichment.method}/{release_date}/{FULL_RELEASE_TYPE}/"


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
    env.update({"AWS_ACCESS_KEY_ID": access_key_id, "AWS_SECRET_ACCESS_KEY": secret_access_key})
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
    *, source_uri: str, hf_bucket: str, hf_prefix: str, endpoint_url: str, s3_client: BaseClient, env: dict[str, str]
) -> None:
    """Copy one release's files from the data bucket to the Hugging Face bucket.

    Stages the run prefix to local disk, then uploads it to the release folder on the
    Hugging Face bucket. If a previous attempt left objects in the target folder, they are
    removed first so a re-publish cannot leave stale shards behind. Other releases are
    never touched.

    Args:
        source_uri: S3 URI of the enrich run prefix on the data bucket (trailing slash).
        hf_bucket: The Hugging Face bucket name.
        hf_prefix: The release folder key prefix, from :func:`full_release_prefix`.
        endpoint_url: The Hugging Face S3-compatible endpoint URL.
        s3_client: Client for the Hugging Face endpoint.
        env: Subprocess environment from :func:`hf_env`.
    """
    target_uri = s3_uri(hf_bucket, hf_prefix)
    stage_dir = local_dir_for_uri(target_uri)
    shutil.rmtree(stage_dir, ignore_errors=True)
    stage_dir.mkdir(parents=True, exist_ok=True)
    try:
        download_files_from_s3(f"{source_uri}*", stage_dir)
        clean_s3_prefix(target_uri, s3_client=s3_client, endpoint_url=endpoint_url, env=env)
        upload_files_to_s3(stage_dir, target_uri, endpoint_url=endpoint_url, env=env)
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)


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
        if not published:
            continue
        releases = [
            {
                "release_date": record.release_date,
                "type": record.release_type,
                "path": record.export_path,
                "published_at": record.published_at,
            }
            for record in published
        ]
        latest = {key: releases[-1][key] for key in ("release_date", "type", "path")}
        datasets[enrichment.method] = {"latest": latest, "releases": releases}

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
    *, source: str, release_date: str, source_uris: Mapping[str, str], hf_bucket: str, endpoint_url: str
) -> None:
    """Publish a source's enrichment releases for a snapshot date, then commit the index.

    Publishes only the datasets present in ``source_uris``. Datasets already marked
    published are skipped, so re-running after a partial failure copies only what is
    missing before re-uploading the index.

    Args:
        source: The source dataset name, e.g. "datacite".
        release_date: ISO date string "YYYY-MM-DD" of the snapshot to publish.
        source_uris: Enrich run prefix S3 URI on the data bucket, keyed by dataset.
        hf_bucket: The Hugging Face bucket name.
        endpoint_url: The Hugging Face S3-compatible endpoint URL.

    Raises:
        ValueError: If the source is unknown.
        RuntimeError: If a dataset is unknown, has no release record for ``release_date``,
            or its release prefix is already referenced by the live index.
    """
    enrichments = {e.identifier: e for e in enrichments_for_source(source)}
    unknown = set(source_uris) - set(enrichments)
    if unknown:
        raise RuntimeError(f"Unknown dataset(s): {', '.join(sorted(unknown))}")

    # Validate the credentials and reach the bucket before the expensive copies.
    env = hf_env()
    s3_client = hf_s3_client(endpoint_url)
    indexed = indexed_release_prefixes(source=source, hf_bucket=hf_bucket, s3_client=s3_client)

    for dataset, source_uri in source_uris.items():
        record = get_release(dataset=dataset, release_date=release_date)
        if record is None:
            raise RuntimeError(f"No release record for {dataset}/{release_date}")
        if record.published_at:
            log.info(f"Skipping {dataset} {release_date}: already published")
            continue
        hf_prefix = full_release_prefix(enrichments[dataset], release_date)
        if hf_prefix.rstrip("/") in indexed:
            raise RuntimeError(
                f"Release prefix {s3_uri(hf_bucket, hf_prefix)} is already referenced by {index_key(source)}"
            )
        copy_release_to_hf(
            source_uri=source_uri,
            hf_bucket=hf_bucket,
            hf_prefix=hf_prefix,
            endpoint_url=endpoint_url,
            s3_client=s3_client,
            env=env,
        )
        mark_published(
            dataset=dataset, release_date=release_date, export_path=hf_prefix, release_type=FULL_RELEASE_TYPE
        )
    publish_index(source=source, hf_bucket=hf_bucket, s3_client=s3_client)
