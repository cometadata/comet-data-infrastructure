"""Tests for comet.cli."""

import subprocess
import sys
import textwrap

import pyarrow.parquet as pq
import pytest

from comet.cli import app

SAMPLE_XML = textwrap.dedent("""\
    <?xml version='1.0' standalone='yes'?>
    <arXivSRC>
      <file>
        <content_md5sum>aaa111</content_md5sum>
        <filename>src/arXiv_src_0001_001.tar</filename>
        <first_item>astro-ph0001001</first_item>
        <last_item>quant-ph0001119</last_item>
        <md5sum>bbb222</md5sum>
        <num_items>100</num_items>
        <seq_num>1</seq_num>
        <size>12345</size>
        <timestamp>2010-12-23 00:13:59</timestamp>
        <yymm>0001</yymm>
      </file>
    </arXivSRC>
""")


class TestPublishCommand:
    def test_invokes_publish_releases_with_parsed_args(self, mocker):
        mock_publish = mocker.patch("comet.exports.publish_releases")
        mocker.patch("comet.cli.setup_logging")

        # cyclopts calls sys.exit(0) once the command returns.
        with pytest.raises(SystemExit) as exc_info:
            app(
                [
                    "publish",
                    "--source",
                    "datacite",
                    "--release-date",
                    "2026-01-02",
                    "--source-uris",
                    '{"datacite-funders": "s3://my-bucket/datacite_enrich_funders/run-1/"}',
                    "--hf-bucket",
                    "my-hf-bucket",
                    "--hf-endpoint-url",
                    "https://s3.example.com",
                ]
            )
        assert exc_info.value.code == 0

        mock_publish.assert_called_once_with(
            source="datacite",
            release_date="2026-01-02",
            source_uris={"datacite-funders": "s3://my-bucket/datacite_enrich_funders/run-1/"},
            hf_bucket="my-hf-bucket",
            endpoint_url="https://s3.example.com",
        )


class TestManifestParquetCommand:
    def test_produces_parquet_file(self, tmp_path):
        xml_file = tmp_path / "test_manifest.xml"
        xml_file.write_text(SAMPLE_XML)
        out_dir = tmp_path / "output"

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "comet.cli",
                "arxiv",
                "manifest-parquet",
                str(xml_file),
                "--output-dir",
                str(out_dir),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        parquet_file = out_dir / "test_manifest.parquet"
        assert parquet_file.exists()
        table = pq.read_table(parquet_file)
        assert table.num_rows == 1

    def test_default_output_dir_is_cwd(self, tmp_path):
        xml_file = tmp_path / "test_manifest.xml"
        xml_file.write_text(SAMPLE_XML)

        result = subprocess.run(
            [sys.executable, "-m", "comet.cli", "arxiv", "manifest-parquet", str(xml_file)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )

        assert result.returncode == 0
        assert (tmp_path / "test_manifest.parquet").exists()
