"""Tests for comet.arxiv.pipeline."""

from datetime import datetime
import json
import random

import pytest

from comet.arxiv.manifest import ManifestEntry
from comet.arxiv.pipeline import (
    check_resume_keys,
    download_manifest,
    get_manifest_etag,
    load_state,
    reshape_batch_output,
    save_state,
    upload_batch_output,
    write_etag_sidecar,
)


def make_entry(
    filename: str,
    num_items: int = 1,
    timestamp: str = "2024-01-01 00:00:00",
) -> ManifestEntry:
    return ManifestEntry(
        content_md5sum="abc",
        filename=filename,
        first_item="",
        last_item="",
        md5sum="def",
        num_items=num_items,
        seq_num=1,
        size=100,
        timestamp=datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S"),
        yymm="2401",
    )


class TestEntryOrdering:
    """Mirrors the sort logic inside run_arxiv_extract."""

    def sort(
        self,
        entries: list[ManifestEntry],
        sort_order: str = "chronological",
        shuffle_seed: int | None = None,
    ) -> list[ManifestEntry]:
        out = list(entries)
        if shuffle_seed is not None:
            random.Random(shuffle_seed).shuffle(out)
        elif sort_order == "largest":
            out.sort(key=lambda e: e.num_items, reverse=True)
        elif sort_order == "smallest":
            out.sort(key=lambda e: e.num_items)
        else:
            out.sort(key=lambda e: e.timestamp)
        return out

    def test_chronological_is_default(self):
        entries = [
            make_entry("c.tar", timestamp="2024-03-01 00:00:00"),
            make_entry("a.tar", timestamp="2024-01-01 00:00:00"),
            make_entry("b.tar", timestamp="2024-02-01 00:00:00"),
        ]
        ordered = self.sort(entries)
        assert [e.filename for e in ordered] == ["a.tar", "b.tar", "c.tar"]

    def test_largest_sorts_by_num_items_desc(self):
        entries = [
            make_entry("small.tar", num_items=10),
            make_entry("big.tar", num_items=1000),
            make_entry("mid.tar", num_items=100),
        ]
        ordered = self.sort(entries, sort_order="largest")
        assert [e.filename for e in ordered] == ["big.tar", "mid.tar", "small.tar"]

    def test_smallest_sorts_by_num_items_asc(self):
        entries = [
            make_entry("small.tar", num_items=10),
            make_entry("big.tar", num_items=1000),
            make_entry("mid.tar", num_items=100),
        ]
        ordered = self.sort(entries, sort_order="smallest")
        assert [e.filename for e in ordered] == ["small.tar", "mid.tar", "big.tar"]

    def test_shuffle_seed_overrides_sort_order(self):
        entries = [make_entry(f"{i}.tar", num_items=i) for i in range(10)]
        ordered = self.sort(entries, sort_order="largest", shuffle_seed=42)
        assert {e.filename for e in ordered} == {e.filename for e in entries}
        assert [e.filename for e in ordered] != [f"{i}.tar" for i in range(9, -1, -1)]

    def test_shuffle_seed_is_deterministic(self):
        entries = [make_entry(f"{i}.tar") for i in range(10)]
        a = self.sort(entries, shuffle_seed=7)
        b = self.sort(entries, shuffle_seed=7)
        assert [e.filename for e in a] == [e.filename for e in b]


class TestCheckResumeKeys:
    def test_accepts_matching_state(self):
        state = {
            "next_batch": 3,
            "batch_size": 500,
            "sort_order": "chronological",
            "shuffle_seed": None,
            "total_entries": 10000,
        }
        current = {"batch_size": 500, "sort_order": "chronological", "shuffle_seed": None, "total_entries": 10000}
        check_resume_keys(state, current, "some-paths")

    def test_rejects_mismatched_batch_size(self):
        state = {
            "next_batch": 3,
            "batch_size": 100,
            "sort_order": "chronological",
            "shuffle_seed": None,
            "total_entries": 10000,
        }
        current = {"batch_size": 500, "sort_order": "chronological", "shuffle_seed": None, "total_entries": 10000}
        with pytest.raises(RuntimeError, match="batch_size"):
            check_resume_keys(state, current, "some-paths")

    def test_rejects_mismatched_manifest_etag(self):
        state = {"manifest_etag": '"abc"'}
        current = {"manifest_etag": '"def"'}
        with pytest.raises(RuntimeError, match="manifest_etag"):
            check_resume_keys(state, current, "some-paths")

    def test_error_names_mismatched_key_and_paths(self):
        state = {"sort_order": "largest"}
        current = {"sort_order": "chronological"}
        with pytest.raises(RuntimeError) as exc:
            check_resume_keys(state, current, "local/state.json and s3://b/p.json")
        message = str(exc.value)
        assert "sort_order" in message
        assert "'largest'" in message
        assert "'chronological'" in message
        assert "local/state.json and s3://b/p.json" in message


class FakeS3:
    """Minimal S3 stub that stores uploads in an in-memory dict."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.etags: dict[tuple[str, str], str] = {}

    def upload_file(self, local_path: str, bucket: str, key: str) -> None:
        with open(local_path, "rb") as f:
            self.objects[(bucket, key)] = f.read()

    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        if (bucket, key) not in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        with open(local_path, "wb") as f:
            f.write(self.objects[(bucket, key)])

    def head_object(self, Bucket: str, Key: str, **_: object) -> dict:
        if (Bucket, Key) not in self.etags:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ETag": self.etags[(Bucket, Key)]}

    def get_object(self, Bucket: str, Key: str, **_: object) -> dict:
        if (Bucket, Key) not in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        import io

        return {
            "Body": io.BytesIO(self.objects[(Bucket, Key)]),
            "ETag": self.etags[(Bucket, Key)],
        }


class TestStatePersistence:
    KEY = "arxiv/2024-01-01/state.json"

    def test_load_returns_none_when_absent(self, tmp_path):
        s3 = FakeS3()
        assert load_state(s3, "b", self.KEY, tmp_path) is None

    def test_save_writes_local_and_uploads(self, tmp_path):
        s3 = FakeS3()
        state = {"next_batch": 3, "batch_size": 500}
        save_state(s3, "b", self.KEY, tmp_path, state)

        assert json.loads((tmp_path / "state.json").read_text()) == state
        assert ("b", self.KEY) in s3.objects

    def test_load_falls_back_to_s3_when_local_missing(self, tmp_path):
        s3 = FakeS3()
        state = {"next_batch": 7, "batch_size": 500}
        save_state(s3, "b", self.KEY, tmp_path, state)
        (tmp_path / "state.json").unlink()

        assert load_state(s3, "b", self.KEY, tmp_path) == state


class TestReshapeBatchOutput:
    def test_moves_shards_into_year_subdirs(self, tmp_path):
        (tmp_path / "arXiv_src_2401_001.parquet").write_bytes(b"a")
        (tmp_path / "arXiv_src_2412_002.parquet").write_bytes(b"b")
        (tmp_path / "arXiv_src_9901_001.parquet").write_bytes(b"c")

        reshape_batch_output(tmp_path)

        assert (tmp_path / "2024" / "arXiv_src_2401_001.parquet").read_bytes() == b"a"
        assert (tmp_path / "2024" / "arXiv_src_2412_002.parquet").read_bytes() == b"b"
        assert (tmp_path / "1999" / "arXiv_src_9901_001.parquet").read_bytes() == b"c"
        assert not (tmp_path / "arXiv_src_2401_001.parquet").exists()

    def test_leaves_non_arxiv_files_at_root(self, tmp_path):
        (tmp_path / "checkpoint.log").write_text("done")
        (tmp_path / "arXiv_src_2401_001.parquet").write_bytes(b"a")

        reshape_batch_output(tmp_path)

        assert (tmp_path / "checkpoint.log").read_text() == "done"
        assert (tmp_path / "2024" / "arXiv_src_2401_001.parquet").exists()

    def test_is_idempotent(self, tmp_path):
        (tmp_path / "arXiv_src_2401_001.parquet").write_bytes(b"a")

        reshape_batch_output(tmp_path)
        reshape_batch_output(tmp_path)

        assert (tmp_path / "2024" / "arXiv_src_2401_001.parquet").read_bytes() == b"a"


class TestUploadBatchOutput:
    def test_invokes_s5cmd_cp_excluding_checkpoint(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr("comet.arxiv.pipeline.run_process", lambda args: calls.append(args))

        upload_batch_output(tmp_path, "my-bucket", "arxiv/2024-01-01/results")

        assert calls == [
            [
                "s5cmd",
                "cp",
                "--exclude",
                "checkpoint.log",
                f"{tmp_path}/*",
                "s3://my-bucket/arxiv/2024-01-01/results/",
            ]
        ]


class TestManifestEtag:
    def test_returns_head_object_etag(self):
        s3 = FakeS3()
        s3.etags[("arxiv", "src/manifest.xml")] = '"abc123"'
        assert get_manifest_etag(s3, "arxiv", "src/manifest.xml") == '"abc123"'


class TestDownloadManifest:
    def test_writes_bytes_and_returns_etag_atomically(self, tmp_path):
        s3 = FakeS3()
        s3.objects[("arxiv", "src/manifest.xml")] = b"<xml/>"
        s3.etags[("arxiv", "src/manifest.xml")] = '"abc123"'

        path, etag = download_manifest(s3, "arxiv", "src/manifest.xml", tmp_path)

        assert path.read_bytes() == b"<xml/>"
        assert etag == '"abc123"'
        assert not (tmp_path / "manifest.xml.tmp").exists()

    def test_cleans_up_temp_on_error(self, tmp_path, monkeypatch):
        s3 = FakeS3()
        s3.objects[("arxiv", "src/manifest.xml")] = b"<xml/>"
        s3.etags[("arxiv", "src/manifest.xml")] = '"abc123"'

        def boom(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr("shutil.copyfileobj", boom)
        with pytest.raises(OSError):
            download_manifest(s3, "arxiv", "src/manifest.xml", tmp_path)

        assert not (tmp_path / "manifest.xml").exists()
        assert not (tmp_path / "manifest.xml.tmp").exists()


class TestEtagSidecar:
    def test_write_is_atomic_via_rename(self, tmp_path):
        path = tmp_path / "manifest.xml.etag"
        write_etag_sidecar(path, '"xyz"')
        assert path.read_text() == '"xyz"'
        assert not (tmp_path / "manifest.xml.etag.tmp").exists()


class TestResumeLoopArithmetic:
    """Mirrors the `if batch_idx < start_batch: continue` + max_batches slice."""

    @staticmethod
    def simulate(total: int, batch_size: int, start_batch: int, max_batches: int | None) -> list[int]:
        entries = list(range(total))
        if max_batches is not None:
            entries = entries[: max_batches * batch_size]
        processed = []
        for batch_idx, start in enumerate(range(0, len(entries), batch_size)):
            if batch_idx < start_batch:
                continue
            processed.append(batch_idx)
        return processed

    def test_increased_max_batches_on_resume_matches_fresh_run(self):
        # First run did batches 0-2; resume with max_batches=5 finishes 3-4.
        assert self.simulate(total=10000, batch_size=500, start_batch=3, max_batches=5) == [3, 4]

    def test_resume_without_max_batches_runs_to_end(self):
        assert self.simulate(total=2000, batch_size=500, start_batch=2, max_batches=None) == [2, 3]

    def test_smaller_max_batches_on_resume_does_nothing(self):
        # First run did batches 0-2; resuming with max_batches=2 has nothing to do.
        assert self.simulate(total=10000, batch_size=500, start_batch=3, max_batches=2) == []

    def test_fresh_run_no_resume(self):
        assert self.simulate(total=1500, batch_size=500, start_batch=0, max_batches=None) == [0, 1, 2]
