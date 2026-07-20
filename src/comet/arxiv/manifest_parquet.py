"""Convert arXiv manifest XML files to Parquet."""

import dataclasses
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from comet.arxiv.manifest import parse_manifest

ARXIV_MANIFEST_SCHEMA = pa.schema(
    [
        ("content_md5sum", pa.string()),
        ("filename", pa.string()),
        ("first_item", pa.string()),
        ("last_item", pa.string()),
        ("md5sum", pa.string()),
        ("num_items", pa.int64()),
        ("seq_num", pa.int64()),
        ("size", pa.int64()),
        ("timestamp", pa.timestamp("us")),
        ("yymm", pa.string()),
    ]
)


def convert_arxiv_manifest(input_file: Path, output_dir: Path) -> Path:
    """Parse an arXiv manifest XML and write it as a Parquet file.

    Args:
        input_file: Path to an arXiv manifest XML file.
        output_dir: Directory to write the output Parquet file.

    Returns:
        Path to the written Parquet file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / input_file.with_suffix(".parquet").name

    entries = parse_manifest(input_file)
    rows = [dataclasses.asdict(e) for e in entries]

    table = pa.Table.from_pylist(rows, schema=ARXIV_MANIFEST_SCHEMA)
    pq.write_table(table, output_path, compression="zstd", compression_level=3)
    print(f"Wrote {len(rows)} rows to {output_path}")
    return output_path
