from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pendulum
import vcr

from comet.aws import DownloadTaskContext
from comet.model.dataset_version_model import DatasetRelease
from comet.ror.ror import download_ror, get_new_ror_release
from comet.ror.tests.conftest import FIXTURES_DIR

ROR_ZENODO_CASSETTE = FIXTURES_DIR / "ror_zenodo.yaml"


class TestGetNewRorRelease:
    def test_returns_latest_release(self):
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE)):
            result = get_new_ror_release(published_after=None)

        assert isinstance(result, DatasetRelease)
        assert result.release_date == pendulum.date(2026, 3, 12)
        assert (
            result.download_url == "https://zenodo.org/api/records/18985120/files/v2.4-2026-03-12-ror-data.zip/content"
        )
        assert result.file_hash == "md5:b04f7419253f96846365a0a36b5041aa"
        assert result.file_name == "v2.4-2026-03-12-ror-data.zip"

    def test_latest_release_date_returns_none(self):
        # Passing the date we already have yields nothing newer.
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE)):
            result = get_new_ror_release(published_after=pendulum.datetime(2026, 3, 12, tz="UTC"))

        assert result is None

    def test_future_date_returns_none(self):
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE)):
            result = get_new_ror_release(published_after=pendulum.datetime(2099, 1, 1, tz="UTC"))

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
