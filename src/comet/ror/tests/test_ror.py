from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pendulum
import pytest
import vcr

from comet.aws import DownloadTaskContext
from comet.model.dataset_version_model import DatasetRelease
from comet.ror.ror import download_ror, get_ror_release
from comet.ror.tests.conftest import FIXTURES_DIR

ROR_ZENODO_CASSETTE = FIXTURES_DIR / "ror_zenodo.yaml"


class TestGetRorRelease:
    def test_returns_release_published_on_date(self):
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE)):
            result = get_ror_release(start_date=pendulum.date(2026, 7, 20), end_date=pendulum.date(2026, 7, 20))

        assert result == DatasetRelease(
            release_date=pendulum.date(2026, 7, 20),
            download_url="https://zenodo.org/api/records/21458494/files/v2.10-2026-07-20-ror-data.zip/content",
            file_name="v2.10-2026-07-20-ror-data.zip",
            file_hash="md5:ad7e842ce1b296fc066a0babe86ec48e",
        )

    @pytest.mark.parametrize(
        ("start_date", "end_date"),
        [
            (pendulum.date(2026, 7, 21), pendulum.date(2026, 7, 21)),
            (pendulum.date(2026, 9, 23), None),
        ],
        ids=["no-release-that-day", "nothing-newer"],
    )
    def test_returns_none_when_no_release_matches(self, start_date, end_date):
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE)):
            result = get_ror_release(start_date=start_date, end_date=end_date)

        assert result is None


class TestDownloadRor:
    def test_downloads_zip_into_download_dir(self, mocker, tmp_path):
        download_dir = tmp_path / "ror_ingest" / "run-1"
        download_dir.mkdir(parents=True)
        target_uri = "s3://my-bucket/ror_ingest/run-1/"
        ctx = DownloadTaskContext(
            download_dir=download_dir,
            target_uri=target_uri,
        )

        def fake_retrieve(*, url, known_hash, fname, path, progressbar):
            assert url == "https://zenodo.org/records/123/files/ror.zip"
            assert known_hash == "md5:abc"
            assert fname == "ror.zip"
            assert Path(path) == download_dir
            target = download_dir / fname
            target.write_bytes(b"zip-bytes")
            return str(target)

        mocker.patch("comet.ror.ror.pooch.retrieve", side_effect=fake_retrieve)

        @contextmanager
        def fake_task(passed_uri):
            assert passed_uri == target_uri
            yield ctx

        with patch("comet.ror.ror.download_source_task", fake_task):
            download_ror(
                target_uri=target_uri,
                download_url="https://zenodo.org/records/123/files/ror.zip",
                file_name="ror.zip",
                file_hash="md5:abc",
            )

        # Left in place for the context manager to upload to S3.
        assert (download_dir / "ror.zip").read_bytes() == b"zip-bytes"
