"""Comet CLI."""

import logging
from pathlib import Path
import sys
from typing import Annotated

import cyclopts

from comet.datacite.cli import datacite_app

app = cyclopts.App(name="comet")
arxiv_app = cyclopts.App(name="arxiv", help="arXiv paper processing commands.")
app.command(arxiv_app)
app.command(datacite_app)


def setup_logging() -> None:
    """Configure logging to stderr with timestamps."""
    stream = open(sys.stderr.fileno(), "w", buffering=1, closefd=False)  # noqa: SIM115
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])


@arxiv_app.command
def manifest_parquet(input_file: Path, output_dir: Path = Path()) -> None:
    """Convert an arXiv manifest XML file to Parquet format."""
    setup_logging()

    from comet.arxiv.manifest_parquet import convert_arxiv_manifest

    convert_arxiv_manifest(input_file, output_dir)


@arxiv_app.command
def pipeline(
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
    """Batch-download arXiv source tars and extract LaTeX via latex-extract."""
    setup_logging()

    from comet.arxiv.pipeline import run_arxiv_extract

    run_arxiv_extract(
        bucket=bucket,
        release_date=release_date,
        batch_size=batch_size,
        max_batches=max_batches,
        data_dir=data_dir,
        arxiv_bucket=arxiv_bucket,
        manifest_key=manifest_key,
        release_prefix=release_prefix,
        jobs=jobs,
        sort_order=sort_order,
        shuffle_seed=shuffle_seed,
        max_shard_rows=max_shard_rows,
        max_shard_bytes=max_shard_bytes,
        papers_per_shard=papers_per_shard,
        timeout_secs=timeout_secs,
        max_retries=max_retries,
        cleanup=cleanup,
    )


@arxiv_app.command
def benchmark(
    *,
    extract_dir: Annotated[Path | None, cyclopts.Parameter(name="--extract-dir")] = None,
    extract_parquet: Annotated[Path | None, cyclopts.Parameter(name="--extract-parquet")] = None,
    dataset_dir: Annotated[Path, cyclopts.Parameter(name="--dataset-dir")] = Path("hf_dataset/data"),
    text_column: Annotated[str, cyclopts.Parameter(name="--text-column")] = "markdown",
) -> None:
    """Evaluate extraction quality against VLM reference dataset."""
    setup_logging()

    if extract_dir is not None and extract_parquet is not None:
        print("Error: specify either --extract-dir or --extract-parquet, not both", file=sys.stderr)
        raise SystemExit(1)
    if extract_dir is None and extract_parquet is None:
        print("Error: specify --extract-dir or --extract-parquet", file=sys.stderr)
        raise SystemExit(1)

    from comet.arxiv.benchmark import (
        evaluate,
        load_extractions_from_dir,
        load_extractions_from_parquet,
        print_report,
    )

    if extract_dir is not None:
        extractions = load_extractions_from_dir(str(extract_dir))
    else:
        extractions = load_extractions_from_parquet(str(extract_parquet), text_column=text_column)

    results = evaluate(extractions, str(dataset_dir))
    print_report(results)


def main() -> None:
    """Run the comet CLI."""
    app()


if __name__ == "__main__":
    main()
