"""Tests for comet.arxiv.benchmark."""

import json

import pyarrow as pa
import pyarrow.parquet as pq

from comet.arxiv.benchmark import (
    evaluate,
    load_extractions_from_dir,
    load_extractions_from_parquet,
)

LONG_TEXT = "word " * 200


def make_reference_jsonl(path, records):
    """Write a JSONL file with VLM reference records."""
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


class TestLoadExtractions:
    def test_from_dir(self, tmp_path):
        (tmp_path / "0704.0001.txt").write_text("hello world")
        (tmp_path / "0704.0002.txt").write_text("foo bar")
        (tmp_path / "notes.md").write_text("ignored")

        result = load_extractions_from_dir(str(tmp_path))

        assert result == {"0704.0001": "hello world", "0704.0002": "foo bar"}

    def test_from_parquet(self, tmp_path):
        table = pa.table(
            {
                "arxiv_id": ["0704.0001", "0704.0002", "0704.0003"],
                "markdown": ["hello world", None, "foo bar"],
            }
        )
        pq.write_table(table, tmp_path / "results.parquet")

        result = load_extractions_from_parquet(str(tmp_path / "results.parquet"))

        assert result == {
            "0704.0001": "hello world",
            "0704.0002": "",
            "0704.0003": "foo bar",
        }

    def test_from_parquet_custom_text_column(self, tmp_path):
        table = pa.table(
            {
                "arxiv_id": ["0704.0001", "0704.0002"],
                "text": ["hello world", None],
            }
        )
        pq.write_table(table, tmp_path / "results.parquet")

        result = load_extractions_from_parquet(str(tmp_path / "results.parquet"), text_column="text")

        assert result == {"0704.0001": "hello world", "0704.0002": ""}

    def test_from_parquet_directory(self, tmp_path):
        table1 = pa.table(
            {
                "arxiv_id": ["0704.0001"],
                "markdown": ["hello"],
            }
        )
        table2 = pa.table(
            {
                "arxiv_id": ["0704.0002"],
                "markdown": ["world"],
            }
        )
        pq.write_table(table1, tmp_path / "part1.parquet")
        pq.write_table(table2, tmp_path / "part2.parquet")
        (tmp_path / "checkpoint.log").write_text("some non-parquet file")

        result = load_extractions_from_parquet(str(tmp_path))

        assert result == {"0704.0001": "hello", "0704.0002": "world"}


class TestEvaluate:
    def test_short_text_marked_failed(self, tmp_path):
        make_reference_jsonl(
            tmp_path / "train.jsonl",
            [
                {"file": "md/0704.0001.md", "text": LONG_TEXT, "category": ["clean"]},
            ],
        )
        extractions = {"0704.0001": "too short"}

        results = evaluate(extractions, str(tmp_path))

        assert len(results) == 1
        assert results[0].failed is True

    def test_empty_string_marked_failed(self, tmp_path):
        make_reference_jsonl(
            tmp_path / "train.jsonl",
            [
                {"file": "md/0704.0001.md", "text": LONG_TEXT, "category": ["clean"]},
            ],
        )
        extractions = {"0704.0001": ""}

        results = evaluate(extractions, str(tmp_path))

        assert len(results) == 1
        assert results[0].failed is True

    def test_matching_text_has_high_recall(self, tmp_path):
        make_reference_jsonl(
            tmp_path / "train.jsonl",
            [
                {"file": "md/0704.0001.md", "text": LONG_TEXT, "category": ["clean"]},
            ],
        )
        extractions = {"0704.0001": LONG_TEXT}

        results = evaluate(extractions, str(tmp_path))

        assert len(results) == 1
        assert results[0].failed is False
        assert results[0].recall > 0.9
        assert results[0].precision > 0.9

    def test_skips_ids_not_in_references(self, tmp_path):
        make_reference_jsonl(
            tmp_path / "train.jsonl",
            [
                {"file": "md/0704.0001.md", "text": LONG_TEXT, "category": ["clean"]},
            ],
        )
        extractions = {"0704.0001": LONG_TEXT, "9999.9999": LONG_TEXT}

        results = evaluate(extractions, str(tmp_path))

        assert len(results) == 1
        assert results[0].arxiv_id == "0704.0001"
