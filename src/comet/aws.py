from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import logging
import pathlib
import shutil
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from comet.utils import local_path, run_process

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence
    from datetime import datetime

    from botocore.client import BaseClient

logger = logging.getLogger(__name__)


def s3_uri(bucket_name: str, *parts: str) -> str:
    """Construct an S3 URI from a bucket name and path parts.

    Args:
        bucket_name: The name of the S3 bucket.
        *parts: Path components to append to the bucket URI.

    Returns:
        A string representing the S3 URI.
    """
    path = "/".join(parts)
    return f"s3://{bucket_name}/{path}" if path else f"s3://{bucket_name}"


def run_prefix(dag_id: str, run_id: str) -> str:
    """Return the key prefix a DAG run writes its output under.

    Args:
        dag_id: The DAG ID.
        run_id: The run ID, or a Jinja placeholder that renders it.

    Returns:
        The key prefix with a trailing slash, e.g. "datacite_enrich_funders/{run_id}/".
    """
    return f"{dag_id}/{run_id}/"


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse an S3 URI into bucket and prefix.

    Args:
        s3_uri: The S3 URI to parse.

    Returns:
        A tuple containing (bucket, prefix).

    Raises:
        ValueError: If the URI scheme is not 's3'.
    """
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def s3_uri_has_files(
    s3_uri: str,
    *,
    s3_client: BaseClient | None = None,
) -> bool:
    """Check if an S3 URI prefix contains any files.

    Args:
        s3_uri: The S3 URI prefix to check.
        s3_client: Optional boto3 S3 client.

    Returns:
        True if files exist at the prefix, False otherwise.

    Raises:
        RuntimeError: If listing objects fails.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, prefix = parse_s3_uri(s3_uri)

    try:
        resp = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
    except ClientError as err:
        raise RuntimeError(f"Unable to list {s3_uri}") from err

    return "Contents" in resp


def s5cmd_command(*args: str, endpoint_url: str | None = None) -> list[str]:
    """Build an s5cmd argv with quiet logging and an end-of-run summary.

    ``--log error`` drops s5cmd's default per-object operation lines (otherwise streamed one
    line per file through :func:`comet.utils.run_process`, e.g. ~14k lines for the DataCite
    snapshot); ``--stat`` replaces them with a single end-of-run summary (counts + errors).

    Args:
        *args: The s5cmd subcommand and its arguments (e.g. ``"cp", src, dst``).
        endpoint_url: Optional S3-compatible endpoint (e.g. a Hugging Face bucket);
            None targets AWS S3.

    Returns:
        The full s5cmd argv, with global flags before the subcommand.
    """
    endpoint_args = ["--endpoint-url", endpoint_url] if endpoint_url else []
    return ["s5cmd", "--log", "error", "--stat", *endpoint_args, *args]


def s5cmd_clean_prefix(
    s3_uri: str,
    *,
    s3_client: BaseClient | None = None,
    endpoint_url: str | None = None,
    env: Mapping[str, str] | None = None,
):
    """Delete all objects at the specified S3 URI prefix using an s5cmd subprocess.

    Args:
        s3_uri: The S3 URI prefix to clean.
        s3_client: Optional boto3 S3 client used to check the prefix.
        endpoint_url: Optional S3-compatible endpoint; None targets AWS S3.
        env: Optional environment for the delete subprocess.

    Raises:
        ValueError: If the URI's key is empty or root-like, or does not end in ``/``;
            both guards prevent a bug from deleting unintended objects.
    """
    _, prefix = parse_s3_uri(s3_uri)
    if not prefix.strip("/ "):
        raise ValueError(f"Refusing to delete an empty prefix: {s3_uri!r}")
    if not prefix.endswith("/"):
        raise ValueError(f"Refusing to delete a prefix without a trailing slash: {s3_uri!r}")
    logger.info(f"Checking and cleaning S3 URI: {s3_uri}")
    if s3_uri_has_files(s3_uri, s3_client=s3_client):
        logger.info(f"Objects found at {s3_uri}, deleting...")
        run_process(s5cmd_command("rm", f"{s3_uri}*", endpoint_url=endpoint_url), env=env)
    else:
        logger.info(f"No objects found at {s3_uri}")


def delete_s3_prefix(
    bucket_name: str, prefix: str, *, s3_client: BaseClient | None = None, dry_run: bool = False
) -> int:
    """Delete an S3 prefix with boto3 and return the matching object count.

    In dry-run mode, matching objects are counted without being removed. The prefix
    must be non-root and end in ``/``.

    Args:
        bucket_name: The bucket to delete from.
        prefix: The key prefix to delete, e.g. "datacite_ingest/run-1/".
        s3_client: Optional boto3 S3 client.
        dry_run: If True, count and log the objects without deleting them.

    Returns:
        The number of objects deleted (or that would be deleted).

    Raises:
        RuntimeError: If S3 reports any object-level deletion errors.
    """
    if not prefix.strip("/ "):
        raise ValueError(f"Refusing to delete an empty prefix: {prefix!r}")
    if not prefix.endswith("/"):
        raise ValueError(f"Refusing to delete a prefix without a trailing slash: {prefix!r}")
    if s3_client is None:
        s3_client = boto3.client("s3")

    deleted = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    # Pages hold at most 1000 keys, the DeleteObjects maximum, so each page is one batch.
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if not keys:
            continue
        if not dry_run:
            response = s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": keys, "Quiet": True})
            errors = response.get("Errors", [])
            if errors:
                raise RuntimeError(f"Failed to delete {len(errors)} objects under {s3_uri(bucket_name, prefix)}")
        deleted += len(keys)

    action = "Would delete" if dry_run else "Deleted"
    logger.info(f"{action} {deleted} objects under {s3_uri(bucket_name, prefix)}")
    return deleted


def list_run_prefixes(
    bucket_name: str,
    producer_dag_ids: Sequence[str],
    *,
    s3_client: BaseClient,
) -> set[str]:
    """List immediate Airflow run prefixes below the configured producer DAGs."""
    runs: set[str] = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for dag_id in producer_dag_ids:
        dag_prefix = f"{dag_id}/"
        for page in paginator.paginate(Bucket=bucket_name, Prefix=dag_prefix, Delimiter="/"):
            for item in page.get("CommonPrefixes", []):
                runs.add(item["Prefix"])
    return runs


def first_object_timestamp(bucket_name: str, prefix: str, *, s3_client: BaseClient) -> datetime | None:
    """Return the modification time of the first object below a run prefix."""
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
    contents = response.get("Contents", [])
    if not contents:
        logger.warning(f"Protecting empty run prefix: {prefix}")
        return None
    return contents[0]["LastModified"]


def s5cmd_upload_files(
    local_dir: pathlib.Path,
    s3_uri: str,
    glob_pattern: str = "*",
    exclude_patterns: tuple[str, ...] = (),
    *,
    endpoint_url: str | None = None,
    env: Mapping[str, str] | None = None,
):
    """Upload files from a local directory to S3.

    Args:
        local_dir: The local directory containing files to upload.
        s3_uri: The destination S3 URI.
        glob_pattern: Glob pattern to match files in the local directory.
        exclude_patterns: Relative glob patterns to exclude from the upload.
        endpoint_url: Optional S3-compatible endpoint; None targets AWS S3.
        env: Optional environment for the upload subprocess.
    """
    logger.info(f"Uploading from {local_dir}/{glob_pattern} to {s3_uri}")
    exclude_args = [arg for pattern in exclude_patterns for arg in ("--exclude", pattern)]
    command = s5cmd_command("cp", *exclude_args, f"{local_dir}/{glob_pattern}", s3_uri, endpoint_url=endpoint_url)
    run_process(command, env=env)


def s5cmd_upload_file(file: pathlib.Path, s3_uri: str):
    """Upload a single file to S3.

    Args:
        file: The local file path.
        s3_uri: The destination S3 URI.
    """
    logger.info(f"Uploading {file} to {s3_uri}")
    run_process(s5cmd_command("cp", f"{file}", s3_uri))


def s5cmd_download_files(source_uri: str, target_dir: pathlib.Path):
    """Download files from S3 to a local directory.

    Args:
        source_uri: The source S3 URI.
        target_dir: The local destination directory.
    """
    logger.info(f"Downloading from {source_uri} to {target_dir}")
    run_process(s5cmd_command("cp", source_uri, f"{target_dir}/"))


def s5cmd_download_file(source_uri: str, target_file: pathlib.Path):
    """Download a single file from S3.

    Args:
        source_uri: The source S3 URI.
        target_file: The local destination file path.
    """
    logger.info(f"Downloading from {source_uri} to {target_file}")
    run_process(s5cmd_command("cp", source_uri, str(target_file)))


AWSEnv = Literal["dev", "stg", "prd"]


# Tag Batch runs consistently with deployed resources; Airflow renders the environment at runtime.
BATCH_JOB_TAGS = {"Environment": "{{ get_env() }}", "Service": "comet"}


def batch_job_name(env: str, name: str) -> str:
    """Build the Batch job (run) name: ``comet-{env}-{name}``."""
    return f"comet-{env}-{name}"


def batch_job_queue_name(env: str, queue: str) -> str:
    """Build the Batch job queue name: ``comet-{env}-batch-{queue}-job-queue``."""
    return f"comet-{env}-batch-{queue}-job-queue"


def batch_job_definition_name(env: str, name: str) -> str:
    """Build the Batch job definition name: ``comet-{env}-{name}-job``."""
    return f"comet-{env}-{name}-job"


def local_dir_for_uri(s3_uri: str) -> pathlib.Path:
    """Return the local scratch directory mirroring an S3 URI's prefix.

    The directory lives under the data dir and is keyed by the URI prefix (e.g.
    ``datacite_ingest/<run_id>``), so concurrent runs on a shared worker never collide.

    Args:
        s3_uri: The S3 URI whose prefix names the local directory.

    Returns:
        The local directory path.

    Raises:
        ValueError: If the URI prefix does not resolve strictly below the data directory.
    """
    _, prefix = parse_s3_uri(s3_uri)
    prefix = prefix.rstrip("/")
    if not prefix or ".." in prefix.split("/"):
        raise ValueError(f"Unsafe scratch path for S3 URI: {s3_uri}")

    scratch_root = local_path().resolve()
    scratch_dir = local_path(prefix).resolve()
    if scratch_dir == scratch_root or not scratch_dir.is_relative_to(scratch_root):
        raise ValueError(f"Unsafe scratch path for S3 URI: {s3_uri}")
    return scratch_dir


def local_file_for_uri(s3_uri: str, work_dir: pathlib.Path) -> pathlib.Path:
    """Return the local path for an S3 object URI's filename, directly inside ``work_dir``.

    Args:
        s3_uri: The S3 URI of a single object.
        work_dir: The local directory the file must land in.

    Returns:
        The local file path.

    Raises:
        ValueError: If the URI's key does not yield a filename that resolves directly
            inside ``work_dir`` (e.g. a bucket root, prefix, or a key ending in ``..``).
    """
    _, key = parse_s3_uri(s3_uri)
    work_dir = work_dir.resolve()
    local_file = (work_dir / pathlib.Path(key).name).resolve()
    if local_file == work_dir or local_file.parent != work_dir:
        raise ValueError(f"Cannot derive a local filename from S3 URI: {s3_uri}")
    return local_file


@contextmanager
def staged_scratch_dir(target_uri: str) -> Generator[pathlib.Path, Any, None]:
    """Yield a clean local stage dir for ``target_uri`` and remove it on exit.

    Clears leftovers from a prior run and the target S3 prefix before yielding, so a
    re-run with the same prefix is idempotent. The stage dir is removed even when the
    body raises, since the next run may land on a different worker.

    Args:
        target_uri: The S3 URI whose prefix keys the stage dir.

    Yields:
        The stage directory path.
    """
    stage_dir = local_dir_for_uri(target_uri)

    # Remove leftovers from a prior run.
    shutil.rmtree(stage_dir, ignore_errors=True)
    s5cmd_clean_prefix(target_uri)
    try:
        yield stage_dir
    finally:
        # Free local disk; the next run may land on a different worker.
        shutil.rmtree(stage_dir, ignore_errors=True)


@dataclass
class DownloadTaskContext:
    """Context for a download task.

    Attributes:
        download_dir: The local directory where files are downloaded.
        target_uri: The S3 URI where files will be uploaded.
    """

    download_dir: pathlib.Path
    target_uri: str


@contextmanager
def download_source_task(target_uri: str) -> Generator[DownloadTaskContext, Any, None]:
    """Download source data into a clean local dir and stage it to S3 on exit.

    The local dir (keyed by ``target_uri``'s prefix) and the target S3 prefix are cleaned
    before yielding, so a re-run with the same prefix is idempotent.

    Args:
        target_uri: The S3 URI the downloaded files are uploaded to (trailing slash).

    Yields:
        A DownloadTaskContext object.
    """
    with staged_scratch_dir(target_uri) as download_dir:
        download_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading to {target_uri}")
        ctx = DownloadTaskContext(
            download_dir=download_dir,
            target_uri=target_uri,
        )
        yield ctx

        s5cmd_upload_files(download_dir, target_uri)


@dataclass
class TransformTaskContext:
    """Context for a transform/enrich task.

    Attributes:
        download_dir: The local directory where source files are downloaded.
        transform_dir: The local directory where output files are written.
        target_uri: The S3 URI where output files will be uploaded.
    """

    download_dir: pathlib.Path
    transform_dir: pathlib.Path
    target_uri: str


@contextmanager
def transform_task(
    source_uri: str,
    target_uri: str,
    upload_glob: str,
    upload_exclude_patterns: tuple[str, ...] = (),
) -> Generator[TransformTaskContext, Any, None]:
    """Download S3 data, yield a dir to transform into, then upload the outputs.

    Stages ``download`` + ``transform`` subdirs under a single local dir keyed by ``target_uri``
    (unique per job: it carries the consuming job's dag_id + run_id). The stage dir and target
    S3 prefix are cleaned before running, so a re-run with the same prefixes is idempotent.

    Keyed by the target, not the source: co-located Batch jobs reading the same upstream snapshot
    would otherwise share ``/data/<source_prefix>`` and clobber each other's in-flight downloads.

    Args:
        source_uri: The S3 URI to download source files from (trailing slash).
        target_uri: The S3 URI to upload output files to (trailing slash).
        upload_glob: Which outputs to upload — an exact filename or a glob.
        upload_exclude_patterns: Relative glob patterns to exclude from the upload.

    Yields:
        A TransformTaskContext object.
    """
    with staged_scratch_dir(target_uri) as stage_dir:
        download_dir = stage_dir / "download"
        transform_dir = stage_dir / "transform"

        download_dir.mkdir(parents=True, exist_ok=True)
        s5cmd_download_files(f"{source_uri}*", download_dir)
        transform_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Transforming {source_uri} -> {target_uri}")
        ctx = TransformTaskContext(
            download_dir=download_dir,
            transform_dir=transform_dir,
            target_uri=target_uri,
        )
        yield ctx

        s5cmd_upload_files(transform_dir, target_uri, upload_glob, upload_exclude_patterns)
