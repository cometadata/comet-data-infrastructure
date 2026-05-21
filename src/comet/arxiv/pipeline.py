"""Batch-process arXiv source tars: download, extract LaTeX, upload results."""

import contextlib
import json
import logging
from pathlib import Path
import random
import shlex
import shutil
import subprocess
import tempfile

import boto3
from botocore.exceptions import ClientError

from comet.arxiv.manifest import ManifestEntry, parse_manifest

STATE_FILE = "state.json"

# arXiv YYMM years <= this pivot are 20YY; greater are 19YY.
ARXIV_YEAR_PIVOT = 30

log = logging.getLogger(__name__)


def run_process(args: list[str]) -> None:
    """Run a subprocess, streaming stdout/stderr to the log."""
    log.info(f"run_process command: `{shlex.join(args)}`")

    with subprocess.Popen(  # noqa: S603
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        shell=False,
    ) as proc:
        for line in proc.stdout:
            log.info(line.rstrip())

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args)


def download_manifest(s3, arxiv_bucket: str, manifest_key: str, data_dir: Path) -> tuple[Path, str]:
    """Download the arXiv manifest via a single get_object.

    Streams to a temp file and atomically renames, so a partial write from
    a crash or error never leaves a truncated manifest on disk. Returns
    (local_path, etag); the ETag comes from the same response as the bytes.
    """
    local_path = data_dir / Path(manifest_key).name
    tmp_path = local_path.with_name(local_path.name + ".tmp")
    log.info(f"Downloading manifest s3://{arxiv_bucket}/{manifest_key} -> {local_path}")
    resp = s3.get_object(Bucket=arxiv_bucket, Key=manifest_key, RequestPayer="requester")
    try:
        with contextlib.closing(resp["Body"]) as body, tmp_path.open("wb") as f:
            shutil.copyfileobj(body, f)
        tmp_path.replace(local_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return local_path, resp["ETag"]


def get_manifest_etag(s3, arxiv_bucket: str, manifest_key: str) -> str:
    """HEAD the manifest object and return its ETag."""
    return s3.head_object(Bucket=arxiv_bucket, Key=manifest_key, RequestPayer="requester")["ETag"]


def write_etag_sidecar(etag_path: Path, etag: str) -> None:
    """Atomically record an ETag next to its companion file."""
    tmp = etag_path.with_name(etag_path.name + ".tmp")
    tmp.write_text(etag)
    tmp.replace(etag_path)


def download_batch(entries: list[ManifestEntry], download_dir: Path, arxiv_bucket: str) -> None:
    """Download a batch of tars from S3 using s5cmd batch mode.

    Skips files that already exist locally with the expected size.
    """
    to_download = []
    for entry in entries:
        local_file = download_dir / Path(entry.filename).name
        if local_file.exists() and local_file.stat().st_size == entry.size:
            log.info(f"Skipping already downloaded: {local_file.name}")
            continue
        to_download.append(entry)

    if not to_download:
        log.info("All files already downloaded, skipping s5cmd")
        return

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        for entry in to_download:
            f.write(f"cp s3://{arxiv_bucket}/{entry.filename} {download_dir}/\n")
        batch_file = f.name

    try:
        run_process(["s5cmd", "--request-payer", "requester", "run", batch_file])
    finally:
        Path(batch_file).unlink(missing_ok=True)


def load_state(s3, bucket: str, progress_s3_key: str, run_dir: Path) -> dict | None:
    """Read the resume bookmark, preferring local then falling back to S3."""
    local = run_dir / STATE_FILE
    if not local.exists():
        try:
            s3.download_file(bucket, progress_s3_key, str(local))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return None
            raise
    return json.loads(local.read_text())


def save_state(
    s3,
    bucket: str,
    progress_s3_key: str,
    run_dir: Path,
    state: dict,
) -> None:
    """Write the resume bookmark locally and upload it to S3."""
    local = run_dir / STATE_FILE
    local.write_text(json.dumps(state, indent=2))
    s3.upload_file(str(local), bucket, progress_s3_key)


def check_resume_keys(state: dict, current: dict, state_paths: str) -> None:
    """Raise if any resume key in ``state`` disagrees with ``current``."""
    for key, expected in current.items():
        if state.get(key) != expected:
            raise RuntimeError(
                f"Saved progress has {key}={state.get(key)!r} but current "
                f"run has {expected!r}. Delete {state_paths} to start over."
            )


def reshape_batch_output(output_dir: Path) -> None:
    """Move flat ``arXiv_src_YYMM_*`` shards into ``<YYYY>/`` subdirs.

    Non-matching files (notably latex-extract's ``checkpoint.log``) are
    left at the root. Idempotent: already-reshaped files live inside
    ``<YYYY>/`` and aren't re-iterated at the top level.
    """
    for entry in output_dir.iterdir():
        if not entry.is_file():
            continue
        name = entry.name
        if not name.startswith("arXiv_src_"):
            continue
        yy = name[10:12]
        if not yy.isdigit():
            continue
        year = f"20{yy}" if int(yy) <= ARXIV_YEAR_PIVOT else f"19{yy}"
        target_dir = output_dir / year
        target_dir.mkdir(exist_ok=True)
        entry.rename(target_dir / name)


def upload_batch_output(output_dir: Path, bucket: str, results_s3_prefix: str) -> None:
    """Upload a reshaped batch ``output/`` to S3 under ``results_s3_prefix``.

    ``checkpoint.log`` is excluded so latex-extract's resume state stays
    local to the batch folder.
    """
    run_process(
        [
            "s5cmd",
            "cp",
            "--exclude",
            "checkpoint.log",
            f"{output_dir}/*",
            f"s3://{bucket}/{results_s3_prefix}/",
        ]
    )


def run_arxiv_extract(
    bucket: str,
    release_date: str,
    *,
    batch_size: int = 500,
    max_batches: int | None = None,
    data_dir: Path = Path("/data/arxiv"),
    arxiv_bucket: str = "arxiv",
    manifest_key: str = "src/arXiv_src_manifest.xml",
    release_prefix: str = "arxiv",
    jobs: int | None = None,
    sort_order: str = "chronological",
    shuffle_seed: int | None = None,
    max_shard_rows: int = 5000,
    max_shard_bytes: int = 128_000_000,
    papers_per_shard: int = 256,
    timeout_secs: int = 45,
    max_retries: int = 3,
    cleanup: bool = True,
) -> None:
    """Download arXiv tars in batches and hand each batch to latex-extract.

    Each batch runs in its own ``batch_NNNNN/`` folder (``download/`` tars, ``output/``
    parquet + metrics). After extract, ``output/`` is reshaped into ``<YYYY>/`` subdirs
    (HuggingFace prefers bounded file counts per dir) and uploaded under ``results/<YYYY>/``.
    With ``cleanup`` (default) the batch folder is removed once the checkpoint advances.

    Order is chronological by manifest timestamp; ``sort_order="largest"``/``"smallest"``
    sorts by ``num_items``, or ``shuffle_seed`` randomizes. Resume + memory bounds are
    latex-extract's (``--resume``, ``--papers-per-shard``, ``--max-shard-*``). A non-zero
    ``latex-extract`` exit is retried up to ``max_retries`` times (no backoff).
    """
    s3 = boto3.client("s3")

    run_dir = data_dir / release_date
    run_dir.mkdir(parents=True, exist_ok=True)

    release_s3_prefix = f"{release_prefix}/{release_date}"
    progress_s3_key = f"{release_s3_prefix}/{STATE_FILE}"
    results_s3_prefix = f"{release_s3_prefix}/results"

    manifest_path = run_dir / Path(manifest_key).name
    etag_path = manifest_path.with_name(manifest_path.name + ".etag")
    current_etag = get_manifest_etag(s3, arxiv_bucket, manifest_key)
    cached_etag = etag_path.read_text().strip() if manifest_path.exists() and etag_path.exists() else None
    if cached_etag != current_etag:
        manifest_path, downloaded_etag = download_manifest(s3, arxiv_bucket, manifest_key, run_dir)
        write_etag_sidecar(etag_path, downloaded_etag)
        current_etag = downloaded_etag
    startup_etag = current_etag

    entries = parse_manifest(manifest_path)
    log.info(f"Parsed {len(entries)} entries from manifest")

    if shuffle_seed is not None:
        random.Random(shuffle_seed).shuffle(entries)  # noqa: S311  # batch ordering, not crypto
    elif sort_order == "largest":
        entries.sort(key=lambda e: e.num_items, reverse=True)
    elif sort_order == "smallest":
        entries.sort(key=lambda e: e.num_items)
    else:  # "chronological"
        entries.sort(key=lambda e: e.timestamp)

    total_entries = len(entries)

    resume_keys = {
        "batch_size": batch_size,
        "sort_order": sort_order,
        "shuffle_seed": shuffle_seed,
        "total_entries": total_entries,
        "manifest_etag": startup_etag,
    }
    state_paths = f"{run_dir / STATE_FILE} and s3://{bucket}/{progress_s3_key}"
    state = load_state(s3, bucket, progress_s3_key, run_dir)
    start_batch = 0
    if state is not None:
        check_resume_keys(state, resume_keys, state_paths)
        start_batch = state["next_batch"]
        log.info(f"Resuming from batch {start_batch}")

    if max_batches is not None:
        entries = entries[: max_batches * batch_size]

    for batch_idx, start in enumerate(range(0, len(entries), batch_size)):
        if batch_idx < start_batch:
            continue
        current_etag = get_manifest_etag(s3, arxiv_bucket, manifest_key)
        if current_etag != startup_etag:
            raise RuntimeError(
                f"Manifest s3://{arxiv_bucket}/{manifest_key} changed mid-run "
                f"(ETag {startup_etag} -> {current_etag}). Stopping to avoid "
                f"processing a shifted set of tars. Delete "
                f"{run_dir / STATE_FILE} and re-run to pick up the new manifest."
            )
        batch = entries[start : start + batch_size]
        batch_dir = run_dir / f"batch_{batch_idx + 1:05d}"
        download_dir = batch_dir / "download"
        output_dir = batch_dir / "output"
        download_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Batch {batch_idx + 1}: tars {start}-{start + len(batch) - 1}")
        download_batch(batch, download_dir, arxiv_bucket)

        extract_args = [
            "latex-extract",
            "-d",
            str(download_dir),
            "-o",
            str(output_dir),
            "--output-format",
            "parquet",
            "--max-shard-rows",
            str(max_shard_rows),
            "--max-shard-bytes",
            str(max_shard_bytes),
            "--papers-per-shard",
            str(papers_per_shard),
            "--resume",
            "--metrics",
            "-t",
            str(timeout_secs),
            *(["-j", str(jobs)] if jobs is not None else []),
        ]

        for attempt in range(1, max_retries + 1):
            try:
                run_process(extract_args)
                break
            except subprocess.CalledProcessError as exc:
                if attempt == max_retries:
                    raise
                log.warning(f"latex-extract failed (exit {exc.returncode}), attempt {attempt}/{max_retries}; retrying")

        reshape_batch_output(output_dir)
        upload_batch_output(output_dir, bucket, results_s3_prefix)

        save_state(
            s3,
            bucket,
            progress_s3_key,
            run_dir,
            {"next_batch": batch_idx + 1, **resume_keys},
        )

        if cleanup:
            shutil.rmtree(batch_dir, ignore_errors=True)

    log.info(f"Done. {len(entries)} tars processed.")
