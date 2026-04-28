"""Tests for tex2md batch conversion."""

import json
from pathlib import Path

import pyarrow.parquet as pq

from comet.arxiv.tex2md import RESULT_SCHEMA, run_batch, run_directory, run_single

from .conftest import TEX_CONTENT, gzip_bytes, make_inner_tar_gz, make_outer_tar


class TestTex2mdBatch:
    def test_run_single_prints_json(self, tmp_path: Path, capsys):
        inner = make_inner_tar_gz({"main.tex": TEX_CONTENT})
        archive = tmp_path / "2401.00001.tar.gz"
        archive.write_bytes(inner)

        run_single(archive, parse_timeout=30.0)

        output = json.loads(capsys.readouterr().out)
        assert output["arxiv_id"] == "2401.00001"
        assert output["status"] == "success"
        assert output["file_type"] == "tex"
        assert "Hello world" in output["markdown"]
        assert output["markdown_length"] > 0

    def test_run_batch_creates_parquet(self, tmp_path: Path):
        inner1 = make_inner_tar_gz({"main.tex": TEX_CONTENT})
        inner2 = make_inner_tar_gz({"paper.tex": TEX_CONTENT})
        tar_dir = tmp_path / "tars"
        tar_dir.mkdir()
        make_outer_tar(
            tar_dir / "arXiv_src_2401_001.tar",
            {
                "2401/2401.00001.tar.gz": inner1,
                "2401/2401.00002.tar.gz": inner2,
            },
        )
        output_dir = tmp_path / "output"

        run_batch(tar_dir, output_dir, jobs=1, parse_timeout=30.0)

        parquet_path = output_dir / "arXiv_src_2401_001.parquet"
        assert parquet_path.exists()
        table = pq.read_table(parquet_path)
        assert table.num_rows == 2
        assert table.schema.equals(RESULT_SCHEMA)
        assert set(table.column("arxiv_id").to_pylist()) == {"2401.00001", "2401.00002"}
        assert all(s == "success" for s in table.column("status").to_pylist())
        assert all(s == "arXiv_src_2401_001.tar" for s in table.column("outer_tar").to_pylist())

    def test_run_directory_creates_parquet(self, tmp_path: Path):
        papers_dir = tmp_path / "papers"
        papers_dir.mkdir()
        inner = make_inner_tar_gz({"main.tex": TEX_CONTENT})
        (papers_dir / "2401.00001.tar.gz").write_bytes(inner)
        (papers_dir / "2401.00002.gz").write_bytes(gzip_bytes(TEX_CONTENT))
        output_dir = tmp_path / "output"

        run_directory(papers_dir, output_dir, jobs=1, parse_timeout=30.0, batch_size=100)

        parquet_path = output_dir / "batch_0000.parquet"
        assert parquet_path.exists()
        table = pq.read_table(parquet_path)
        assert table.num_rows == 2
        assert all(v is None for v in table.column("outer_tar").to_pylist())

    def test_parquet_uses_zstd_compression(self, tmp_path: Path):
        inner = make_inner_tar_gz({"main.tex": TEX_CONTENT})
        tar_dir = tmp_path / "tars"
        tar_dir.mkdir()
        make_outer_tar(
            tar_dir / "test.tar",
            {"2401/2401.00001.tar.gz": inner},
        )
        output_dir = tmp_path / "output"

        run_batch(tar_dir, output_dir, jobs=1, parse_timeout=30.0)

        parquet_path = output_dir / "test.parquet"
        metadata = pq.read_metadata(parquet_path)
        col_meta = metadata.row_group(0).column(0)
        assert col_meta.compression == "ZSTD"
