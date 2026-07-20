import os

# Moto credentials. AWS_ENV / AWS_REGION are set in the repo-level conftest.py.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

import datetime

from moto import mock_aws
import pytest

from comet.dynamodb_store import (
    DatasetReleaseRecord,
    get_latest_release,
    get_release,
    persist_discovered_release,
    scan_by_date_range,
)
from comet.model.dataset_version_model import DatasetRelease


@pytest.fixture
def releases_table():
    with mock_aws():
        DatasetReleaseRecord.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
        yield
        DatasetReleaseRecord.delete_table()


def make_release(date_str: str, **overrides) -> DatasetRelease:
    return DatasetRelease(
        release_date=datetime.date.fromisoformat(date_str),
        file_name=overrides.get("file_name", "v1.zip"),
        download_url=overrides.get("download_url", "https://example.com/v1.zip"),
        file_hash=overrides.get("file_hash", "md5:abc123"),
        metadata=overrides.get("metadata", {}),
    )


class TestGetLatestRelease:
    def test_returns_most_recent_by_release_date(self, releases_table):
        for d in ["2024-01-01", "2024-06-15", "2024-03-10"]:
            persist_discovered_release(dataset="ror", release=make_release(d), run_id=f"run-{d}")

        result = get_latest_release(dataset="ror")
        assert result is not None
        assert result.release_date == "2024-06-15"

    def test_returns_none_when_empty(self, releases_table):
        assert get_latest_release(dataset="ror") is None


class TestGetRelease:
    def test_returns_record_by_primary_key(self, releases_table):
        persist_discovered_release(dataset="ror", release=make_release("2025-01-01"), run_id="run-1")

        result = get_release(dataset="ror", release_date="2025-01-01")
        assert result is not None
        assert result.dataset == "ror"
        assert result.release_date == "2025-01-01"

    def test_returns_none_when_missing(self, releases_table):
        assert get_release(dataset="ror", release_date="2099-01-01") is None


class TestToDatasetRelease:
    def test_round_trips_record_fields_including_run_id(self, releases_table):
        persist_discovered_release(
            dataset="ror",
            release=make_release("2025-01-01", metadata={"k": "v"}),
            run_id="run-xyz",
        )

        release = get_release(dataset="ror", release_date="2025-01-01").to_dataset_release()

        assert release == DatasetRelease(
            release_date=datetime.date(2025, 1, 1),
            file_name="v1.zip",
            download_url="https://example.com/v1.zip",
            file_hash="md5:abc123",
            run_id="run-xyz",
            metadata={"k": "v"},
        )
        assert release.to_dict()["run_id"] == "run-xyz"


class TestPersistDiscoveredRelease:
    def test_creates_record_with_all_fields(self, releases_table):
        release = make_release(
            "2025-01-01",
            file_name="ror.zip",
            file_hash="md5:deadbeef",
            download_url="https://zenodo.org/ror.zip",
        )

        record = persist_discovered_release(dataset="ror", release=release, run_id="metaflow-1234")

        assert record is not None
        assert record.dataset == "ror"
        assert record.release_date == "2025-01-01"
        assert record.file_name == "ror.zip"
        assert record.download_url == "https://zenodo.org/ror.zip"
        assert record.file_hash == "md5:deadbeef"
        assert record.run_id == "metaflow-1234"

    def test_updates_existing_record_on_duplicate(self, releases_table):
        release = make_release("2025-02-01")
        first = persist_discovered_release(dataset="ror", release=release, run_id="run-a")
        second = persist_discovered_release(dataset="ror", release=release, run_id="run-b")

        assert second is not None
        assert get_release(dataset="ror", release_date="2025-02-01").run_id == "run-b"
        assert second.created_at == first.created_at


class TestScanByDateRange:
    @pytest.fixture
    def populated_table(self, releases_table):
        for d in ["2024-01-01", "2024-06-15", "2025-01-01", "2025-06-15"]:
            persist_discovered_release(dataset="ror", release=make_release(d), run_id=f"run-{d}")

    @pytest.mark.parametrize(
        "start_date,end_date,expected",
        [
            (None, None, {"2024-01-01", "2024-06-15", "2025-01-01", "2025-06-15"}),
            ("2025-01-01", None, {"2025-01-01", "2025-06-15"}),
            (None, "2024-06-15", {"2024-01-01", "2024-06-15"}),
            ("2024-06-15", "2025-01-01", {"2024-06-15", "2025-01-01"}),
        ],
    )
    def test_filters_by_bounds(self, populated_table, start_date, end_date, expected):
        results = scan_by_date_range(DatasetReleaseRecord, start_date=start_date, end_date=end_date)
        assert {r.release_date for r in results} == expected
