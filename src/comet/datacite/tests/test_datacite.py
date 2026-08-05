from __future__ import annotations

from contextlib import contextmanager
import datetime
from functools import partial
import json
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
import pendulum
import pytest
import vcr

from comet.aws import DownloadTaskContext
from comet.datacite.datacite import (
    download_datacite,
    fetch_datacite_aws_credentials,
    fetch_datacite_manifest_stats,
    get_new_datacite_release,
    release_is_smaller,
)
from comet.datacite.tests.conftest import (
    CREDENTIAL_HEADERS,
    FIXTURES_DIR,
    HEADER_KEYS,
    vcrpy_clean_response,
)
from comet.model.dataset_version_model import DatasetRelease

DATACITE_CREDENTIALS_CASSETTE = FIXTURES_DIR / "datacite_credentials.yaml"
DATACITE_STATUS_CASSETTE = FIXTURES_DIR / "datacite_status.yaml"


def fake_s3_client(body_or_error):
    client = MagicMock()
    if isinstance(body_or_error, Exception):
        client.get_object.side_effect = body_or_error
    else:
        body = MagicMock()
        body.read.return_value = body_or_error
        client.get_object.return_value = {"Body": body}
    return client


class TestFetchDataCiteAwsCredentials:
    def test_returns_credentials_tuple(self, monkeypatch):
        monkeypatch.setenv("DATACITE_ACCOUNT_ID", "dummy")
        monkeypatch.setenv("DATACITE_PASSWORD", "dummy")

        with vcr.use_cassette(
            str(DATACITE_CREDENTIALS_CASSETTE),
            filter_headers=CREDENTIAL_HEADERS,
            before_record_response=partial(
                vcrpy_clean_response,
                body_keys=["access_key_id", "secret_access_key", "session_token"],
                header_keys=HEADER_KEYS,
            ),
        ):
            access_key_id, secret_access_key, session_token = fetch_datacite_aws_credentials()

        assert access_key_id == "DUMMY_ACCESS_KEY_ID"
        assert secret_access_key == "DUMMY_SECRET_ACCESS_KEY"
        assert session_token == "DUMMY_SESSION_TOKEN"

    def test_raises_when_credentials_missing(self, monkeypatch):
        monkeypatch.delenv("DATACITE_ACCOUNT_ID", raising=False)
        monkeypatch.delenv("DATACITE_PASSWORD", raising=False)

        with pytest.raises(RuntimeError, match="must be provided"):
            fetch_datacite_aws_credentials()

    def test_raises_on_unexpected_response_shape(self, mocker):
        fake_response = MagicMock()
        fake_response.json.return_value = {"unexpected": "shape"}
        fake_response.raise_for_status.return_value = None
        mocker.patch("comet.datacite.datacite.requests.get", return_value=fake_response)

        with pytest.raises(RuntimeError, match="Unexpected response format"):
            fetch_datacite_aws_credentials(account_id="x", password="y")


class TestDownloadDatacite:
    def run_download(self, mocker, tmp_path, *, expected_file_count, expected_total_bytes):
        mocker.patch(
            "comet.datacite.datacite.fetch_datacite_aws_credentials",
            return_value=("AKID", "SECRET", "TOKEN"),
        )

        download_dir = tmp_path / "datacite_ingest" / "run-1"
        download_dir.mkdir(parents=True)
        target_uri = "s3://my-bucket/datacite_ingest/run-1/"
        ctx = DownloadTaskContext(download_dir=download_dir, target_uri=target_uri)

        # Fake s5cmd: drop one file into the download dir.
        def fake_run_process(cmd, env=None):
            (download_dir / "part_0000.jsonl.gz").write_bytes(b"x" * 10)

        mock_run_process = mocker.patch("comet.datacite.datacite.run_process", side_effect=fake_run_process)

        @contextmanager
        def fake_task(passed_uri):
            assert passed_uri == target_uri
            yield ctx

        with patch("comet.datacite.datacite.download_source_task", fake_task):
            download_datacite(
                target_uri=target_uri,
                datacite_bucket_name="datacite-source-bucket",
                datacite_bucket_region="eu-west-1",
                expected_file_count=expected_file_count,
                expected_total_bytes=expected_total_bytes,
            )
        return mock_run_process, download_dir

    def test_invokes_s5cmd_and_verifies_against_manifest(self, mocker, tmp_path):
        mock_run_process, download_dir = self.run_download(
            mocker, tmp_path, expected_file_count=1, expected_total_bytes=10
        )

        cmd = mock_run_process.call_args.args[0]
        env = mock_run_process.call_args.kwargs["env"]
        assert cmd == [
            "s5cmd",
            "cp",
            "--source-region",
            "eu-west-1",
            "s3://datacite-source-bucket/dois/*.jsonl.gz",
            f"{download_dir}/",
        ]
        assert env["AWS_ACCESS_KEY_ID"] == "AKID"
        assert env["AWS_SECRET_ACCESS_KEY"] == "SECRET"
        assert env["AWS_SESSION_TOKEN"] == "TOKEN"

    def test_raises_when_download_incomplete(self, mocker, tmp_path):
        with pytest.raises(ValueError, match="download incomplete"):
            self.run_download(mocker, tmp_path, expected_file_count=2, expected_total_bytes=10)


class TestGetNewDataCiteRelease:
    @contextmanager
    def status_cassette(self):
        env = {"DATACITE_ACCOUNT_ID": "dummy", "DATACITE_PASSWORD": "dummy"}
        with (
            patch.dict("os.environ", env),
            vcr.use_cassette(
                str(DATACITE_STATUS_CASSETTE),
                filter_headers=CREDENTIAL_HEADERS,
                before_record_response=partial(
                    vcrpy_clean_response,
                    body_keys=["access_key_id", "secret_access_key", "session_token"],
                    header_keys=HEADER_KEYS,
                ),
            ),
        ):
            yield

    def call(self, published_after=None):
        return get_new_datacite_release(
            datacite_bucket_name="monthly-datafile.datacite.org",
            datacite_bucket_region="eu-west-1",
            published_after=published_after,
        )

    def test_returns_latest_release_with_manifest_stats(self, mocker):
        mocker.patch(
            "comet.datacite.datacite.fetch_datacite_manifest_stats",
            return_value=(350, 1200),
        )
        with self.status_cassette():
            result = self.call(published_after=None)

        assert isinstance(result, DatasetRelease)
        assert result.release_date == pendulum.date(2026, 3, 1)
        assert result.metadata == {"file_count": "350", "total_size_bytes": "1200"}

    def test_raises_when_manifest_unreadable(self, mocker):
        mocker.patch(
            "comet.datacite.datacite.fetch_datacite_manifest_stats",
            side_effect=RuntimeError("boom"),
        )
        with self.status_cassette(), pytest.raises(RuntimeError):
            self.call(published_after=None)

    def test_latest_release_date_returns_none(self):
        # Passing the date we already have yields nothing newer.
        with self.status_cassette():
            result = self.call(published_after=pendulum.datetime(2026, 3, 1, tz="UTC"))

        assert result is None

    def test_future_date_returns_none(self):
        with self.status_cassette():
            result = self.call(published_after=pendulum.datetime(2099, 1, 1, tz="UTC"))

        assert result is None

    def mock_status(self, mocker, body_or_error):
        mocker.patch(
            "comet.datacite.datacite.fetch_datacite_aws_credentials",
            return_value=("AKID", "SECRET", "TOKEN"),
        )
        mocker.patch("comet.datacite.datacite.boto3.client", return_value=fake_s3_client(body_or_error))

    @pytest.mark.parametrize(
        "body_or_error, match",
        [
            (ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject"), "STATUS.json"),
            (b"not json", "STATUS.json"),
            (json.dumps({"status": "Failed"}).encode(), "Unexpected"),
            (json.dumps({"status": "Complete"}).encode(), "no datetime"),
            (json.dumps({"status": "Complete", "datetime": "not-a-date"}).encode(), "parse"),
        ],
    )
    def test_raises_on_unreadable_or_invalid_status(self, mocker, body_or_error, match):
        self.mock_status(mocker, body_or_error)

        with pytest.raises(RuntimeError, match=match):
            self.call()

    @pytest.mark.parametrize("status_value", ["In progress", "Uploading"])
    def test_not_ready_status_returns_none(self, mocker, status_value):
        self.mock_status(mocker, json.dumps({"status": status_value}).encode())

        assert self.call() is None


class TestFetchDataciteManifestStats:
    def test_counts_only_jsonl_gz_and_sums_sizes(self):
        manifest = [
            {"filename": "dois/updated_2011-03/part_0000.jsonl.gz", "size": 41, "sha256": "a"},
            {"filename": "dois/updated_2011-03/2011-03.csv.gz", "size": 375, "sha256": "b"},
            {"filename": "dois/updated_2011-12/part_0000.jsonl.gz", "size": 393763, "sha256": "c"},
        ]
        client = fake_s3_client(json.dumps(manifest).encode())

        assert fetch_datacite_manifest_stats(client, "bucket") == (2, 41 + 393763)

    def test_raises_on_unparseable_manifest(self):
        client = fake_s3_client(b"not json")

        with pytest.raises(RuntimeError, match="MANIFEST.json"):
            fetch_datacite_manifest_stats(client, "bucket")


class TestReleaseIsSmaller:
    @pytest.mark.parametrize(
        "new_meta, last_meta, expected",
        [
            ({"file_count": "10", "total_size_bytes": "100"}, {}, False),  # no previous stats
            ({"file_count": "10", "total_size_bytes": "100"}, {"file_count": "10", "total_size_bytes": "100"}, False),
            ({"file_count": "11", "total_size_bytes": "110"}, {"file_count": "10", "total_size_bytes": "100"}, False),
            ({"file_count": "9", "total_size_bytes": "100"}, {"file_count": "10", "total_size_bytes": "100"}, True),
            ({"file_count": "10", "total_size_bytes": "90"}, {"file_count": "10", "total_size_bytes": "100"}, True),
        ],
    )
    def test_flags_fewer_files_or_bytes(self, new_meta, last_meta, expected):
        new = DatasetRelease(release_date=datetime.date(2026, 1, 1), metadata=new_meta)
        last = DatasetRelease(release_date=datetime.date(2025, 12, 1), metadata=last_meta)

        assert release_is_smaller(new, last) is expected
