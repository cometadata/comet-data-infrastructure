from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import logging
import shutil
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from comet.utils import local_path, run_process

if TYPE_CHECKING:
    from collections.abc import Generator
    import pathlib

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


def s5cmd_command(*args: str) -> list[str]:
    """Build an s5cmd argv with quiet logging and an end-of-run summary.

    ``--log error`` drops s5cmd's default per-object operation lines (otherwise streamed one
    line per file through :func:`comet.utils.run_process`, e.g. ~14k lines for the DataCite
    snapshot); ``--stat`` replaces them with a single end-of-run summary (counts + errors).

    Args:
        *args: The s5cmd subcommand and its arguments (e.g. ``"cp", src, dst``).

    Returns:
        The full s5cmd argv, with global flags before the subcommand.
    """
    return ["s5cmd", "--log", "error", "--stat", *args]


def clean_s3_prefix(s3_uri: str):
    """Delete all objects at the specified S3 URI prefix.

    Args:
        s3_uri: The S3 URI prefix to clean.
    """
    logger.info(f"Checking and cleaning S3 URI: {s3_uri}")
    if s3_uri_has_files(s3_uri):
        logger.info(f"Objects found at {s3_uri}, deleting...")
        run_process(s5cmd_command("rm", f"{s3_uri}*"))
    else:
        logger.info(f"No objects found at {s3_uri}")


def upload_files_to_s3(
    local_dir: pathlib.Path,
    s3_uri: str,
    glob_pattern: str = "*",
    exclude_patterns: tuple[str, ...] = (),
):
    """Upload files from a local directory to S3.

    Args:
        local_dir: The local directory containing files to upload.
        s3_uri: The destination S3 URI.
        glob_pattern: Glob pattern to match files in the local directory.
        exclude_patterns: Relative glob patterns to exclude from the upload.
    """
    logger.info(f"Uploading from {local_dir}/{glob_pattern} to {s3_uri}")
    exclude_args = [arg for pattern in exclude_patterns for arg in ("--exclude", pattern)]
    run_process(s5cmd_command("cp", *exclude_args, f"{local_dir}/{glob_pattern}", s3_uri))


def upload_file_to_s3(file: pathlib.Path, s3_uri: str):
    """Upload a single file to S3.

    Args:
        file: The local file path.
        s3_uri: The destination S3 URI.
    """
    logger.info(f"Uploading {file} to {s3_uri}")
    run_process(s5cmd_command("cp", f"{file}", s3_uri))


def download_files_from_s3(source_uri: str, target_dir: pathlib.Path):
    """Download files from S3 to a local directory.

    Args:
        source_uri: The source S3 URI.
        target_dir: The local destination directory.
    """
    logger.info(f"Downloading from {source_uri} to {target_dir}")
    run_process(s5cmd_command("cp", source_uri, f"{target_dir}/"))


def download_file_from_s3(source_uri: str, target_file: pathlib.Path):
    """Download a single file from S3.

    Args:
        source_uri: The source S3 URI.
        target_file: The local destination file path.
    """
    logger.info(f"Downloading from {source_uri} to {target_file}")
    run_process(s5cmd_command("cp", source_uri, str(target_file)))


AWSEnv = Literal["dev", "stg", "prd"]


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
    """
    _, prefix = parse_s3_uri(s3_uri)
    return local_path(prefix.rstrip("/"))


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
    download_dir = local_dir_for_uri(target_uri)

    # Remove leftovers from a prior run.
    shutil.rmtree(download_dir, ignore_errors=True)
    clean_s3_prefix(target_uri)
    download_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading to {target_uri}")
    ctx = DownloadTaskContext(
        download_dir=download_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_files_to_s3(download_dir, target_uri)

    # Free local disk; the next run may land on a different worker.
    shutil.rmtree(download_dir, ignore_errors=True)


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
    stage_dir = local_dir_for_uri(target_uri)
    download_dir = stage_dir / "download"
    transform_dir = stage_dir / "transform"

    # Remove leftovers from a prior run.
    shutil.rmtree(stage_dir, ignore_errors=True)
    clean_s3_prefix(target_uri)

    download_dir.mkdir(parents=True, exist_ok=True)
    download_files_from_s3(f"{source_uri}*", download_dir)
    transform_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Transforming {source_uri} -> {target_uri}")
    ctx = TransformTaskContext(
        download_dir=download_dir,
        transform_dir=transform_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_files_to_s3(transform_dir, target_uri, upload_glob, upload_exclude_patterns)

    # Free local disk; the next run may land on a different worker.
    shutil.rmtree(stage_dir, ignore_errors=True)
