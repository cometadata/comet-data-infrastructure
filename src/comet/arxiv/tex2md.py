"""Batch conversion of arXiv LaTeX papers to Markdown with Parquet output."""

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import comet.pool as pool
from comet.arxiv.archive import PaperArchive, count_tar_entries, extract_papers, extract_single_archive
from comet.arxiv.docling import ConversionResult, convert_paper, make_converter
from comet.pool import run_pool, to_batches

log = logging.getLogger(__name__)


def ramdisk_dir() -> str | None:
    """Return /dev/shm if available, else None (falls back to system default)."""
    path = "/dev/shm"
    if os.path.exists(path):
        return path
    log.warning("RAM disk /dev/shm not available, falling back to default tmpdir")
    return None


RESULT_SCHEMA = pa.schema([
    ("arxiv_id", pa.string()),
    ("status", pa.string()),
    ("file_type", pa.string()),
    ("num_tex_files", pa.uint32()),
    ("markdown_length", pa.uint32()),
    ("markdown", pa.large_utf8()),
    ("entry_name", pa.string()),
    ("outer_tar", pa.string()),
])

worker_converter = None


def pool_initializer(parse_timeout: float) -> None:
    """Create a DocumentConverter in each worker process."""
    global worker_converter
    worker_converter = make_converter(parse_timeout)


def result_to_row(
    result: ConversionResult,
    paper: PaperArchive,
    outer_tar: str | None = None,
) -> dict:
    """Convert a ConversionResult and PaperArchive into a Parquet row dict."""
    md = result.markdown
    return {
        "arxiv_id": result.arxiv_id,
        "status": result.status,
        "file_type": result.file_type.value,
        "num_tex_files": len(paper.tex_files),
        "markdown_length": len(md) if md is not None else None,
        "markdown": md,
        "entry_name": paper.entry_name,
        "outer_tar": outer_tar,
    }


def write_results_parquet(rows: list[dict], output_path: Path) -> None:
    """Write result rows to a zstd-compressed Parquet file."""
    table = pa.Table.from_pylist(rows, schema=RESULT_SCHEMA)
    pq.write_table(table, output_path, compression="zstd", compression_level=3)


# --- Single mode ---


def run_single(archive_path: Path, parse_timeout: float = 60.0) -> None:
    """Process a single paper archive and print JSON to stdout."""
    with tempfile.TemporaryDirectory(dir=ramdisk_dir()) as tmpdir:
        paper = extract_single_archive(archive_path, Path(tmpdir))
        converter = make_converter(parse_timeout)
        result = convert_paper(paper, converter)

    row = result_to_row(result, paper)
    print(json.dumps(row, ensure_ascii=False))


# --- Directory mode ---


@dataclass
class DirectoryTask:
    """A batch of archive files to extract and convert."""

    batch_index: int
    files: list[Path]
    output_dir: Path
    ramdisk: str | None


def directory_worker(task: DirectoryTask) -> None:
    """Process a batch of archive files: extract, convert, write parquet."""
    rows = []
    with tempfile.TemporaryDirectory(dir=task.ramdisk) as tmpdir:
        for archive_path in task.files:
            if pool.POOL_PROGRESS.is_aborted():
                break
            paper = extract_single_archive(archive_path, Path(tmpdir))
            result = convert_paper(paper, worker_converter)
            rows.append(result_to_row(result, paper))
            pool.POOL_PROGRESS.increment()

    if rows:
        output_path = task.output_dir / f"batch_{task.batch_index:04d}.parquet"
        write_results_parquet(rows, output_path)


def run_directory(
    paper_dir: Path,
    output_dir: Path,
    jobs: int | None = None,
    parse_timeout: float = 60.0,
    batch_size: int = 100,
) -> None:
    """Process a directory of individual paper archives, one Parquet per batch."""
    archive_files = sorted(
        p
        for p in paper_dir.iterdir()
        if p.is_file() and p.name.endswith((".tar.gz", ".tgz", ".gz", ".tex", ".pdf"))
    )
    if not archive_files:
        log.warning("No archive files found in %s", paper_dir)
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    ramdisk = ramdisk_dir()

    tasks = [
        DirectoryTask(batch_index=idx, files=batch, output_dir=output_dir, ramdisk=ramdisk)
        for idx, batch in enumerate(to_batches(archive_files, batch_size))
    ]

    run_pool(
        tasks,
        directory_worker,
        total=len(archive_files),
        desc="Converting",
        unit="paper",
        max_workers=jobs,
        initializer=pool_initializer,
        initargs=(parse_timeout,),
    )


# --- Batch mode ---


@dataclass
class BatchTask:
    """A single outer tar to extract and convert."""

    tar_path: Path
    output_dir: Path
    ramdisk: str | None


def batch_worker(task: BatchTask) -> None:
    """Process one outer tar: extract papers lazily, convert, write parquet."""
    rows = []
    tar_name = task.tar_path.name
    with tempfile.TemporaryDirectory(dir=task.ramdisk) as tmpdir:
        for paper in extract_papers(task.tar_path, Path(tmpdir)):
            if pool.POOL_PROGRESS.is_aborted():
                break
            result = convert_paper(paper, worker_converter)
            rows.append(result_to_row(result, paper, outer_tar=tar_name))
            pool.POOL_PROGRESS.increment()

    if rows:
        output_path = task.output_dir / f"{task.tar_path.stem}.parquet"
        write_results_parquet(rows, output_path)

    pool.POOL_PROGRESS.increment_extra("tars")


def run_batch(
    tar_dir: Path,
    output_dir: Path,
    jobs: int | None = None,
    parse_timeout: float = 60.0,
) -> None:
    """Process all outer tars in a directory, one tar per worker."""
    tar_files = sorted(tar_dir.glob("*.tar"))
    if not tar_files:
        log.warning("No .tar files found in %s", tar_dir)
        return

    total_papers = sum(count_tar_entries(p) for p in tar_files)
    output_dir.mkdir(parents=True, exist_ok=True)
    ramdisk = ramdisk_dir()

    tasks = [
        BatchTask(tar_path=tar_path, output_dir=output_dir, ramdisk=ramdisk)
        for tar_path in tar_files
    ]

    run_pool(
        tasks,
        batch_worker,
        total=total_papers,
        desc="Converting",
        unit="paper",
        max_workers=jobs,
        initializer=pool_initializer,
        initargs=(parse_timeout,),
        extras={"tars": len(tar_files)},
    )
