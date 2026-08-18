import datetime

from pynamodb.exceptions import UpdateError
import pytest

from comet.dynamodb_store import (
    DatasetReleaseRecord,
    get_latest_release,
    get_release,
    list_releases,
    mark_published,
    persist_discovered_release,
    scan_by_date_range,
)
from comet.model.dataset_version_model import DatasetRelease


def make_release(date_str: str, **overrides) -> DatasetRelease:
    return DatasetRelease(
        release_date=datetime.date.fromisoformat(date_str),
        file_name=overrides.get("file_name", "v1.zip"),
        download_url=overrides.get("download_url", "https://example.com/v1.zip"),
        file_hash=overrides.get("file_hash", "md5:abc123"),
        metadata=overrides.get("metadata", {}),
    )


class TestGetLatestRelease:
    def test_uses_a_consistent_read(self, mocker):
        query = mocker.patch.object(DatasetReleaseRecord, "query", return_value=iter([]))

        assert get_latest_release(dataset="ror") is None
        query.assert_called_once_with("ror", consistent_read=True, scan_index_forward=False, limit=1)

    def test_returns_most_recent_by_release_date(self, releases_table):
        for d in ["2024-01-01", "2024-06-15", "2024-03-10"]:
            persist_discovered_release(dataset="ror", release=make_release(d), run_id=f"run-{d}")

        result = get_latest_release(dataset="ror")
        assert result is not None
        assert result.release_date == "2024-06-15"

    def test_returns_none_when_empty(self, releases_table):
        assert get_latest_release(dataset="ror") is None


class TestGetRelease:
    def test_uses_a_consistent_read(self, mocker):
        record = mocker.sentinel.record
        get = mocker.patch.object(DatasetReleaseRecord, "get", return_value=record)

        assert get_release(dataset="ror", release_date="2025-01-01") is record
        get.assert_called_once_with("ror", "2025-01-01", consistent_read=True)

    def test_returns_record_by_primary_key(self, releases_table):
        persist_discovered_release(dataset="ror", release=make_release("2025-01-01"), run_id="run-1")

        result = get_release(dataset="ror", release_date="2025-01-01")
        assert result is not None
        assert result.dataset == "ror"
        assert result.release_date == "2025-01-01"

    def test_returns_none_when_missing(self, releases_table):
        assert get_release(dataset="ror", release_date="2099-01-01") is None


def test_list_releases_uses_a_consistent_read(mocker):
    query = mocker.patch.object(DatasetReleaseRecord, "query", return_value=[])

    assert list_releases(dataset="datacite-funders") == []
    query.assert_called_once_with("datacite-funders", consistent_read=True)


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

        record = persist_discovered_release(
            dataset="ror",
            release=release,
            run_id="metaflow-1234",
            source_prefix="ror_ingest/metaflow-1234/",
        )

        assert record is not None
        assert record.dataset == "ror"
        assert record.release_date == "2025-01-01"
        assert record.file_name == "ror.zip"
        assert record.download_url == "https://zenodo.org/ror.zip"
        assert record.file_hash == "md5:deadbeef"
        assert record.run_id == "metaflow-1234"
        assert record.source_prefix == "ror_ingest/metaflow-1234/"

    def test_updates_existing_record_on_duplicate(self, releases_table):
        release = make_release("2025-02-01")
        first = persist_discovered_release(
            dataset="ror", release=release, run_id="run-a", source_prefix="ror_ingest/run-a/"
        )
        second = persist_discovered_release(dataset="ror", release=release, run_id="run-b")

        assert second is not None
        refreshed = get_release(dataset="ror", release_date="2025-02-01")
        assert refreshed.run_id == "run-b"
        # A refresh without a source_prefix clears the stale one rather than pointing at the old run.
        assert refreshed.source_prefix is None
        assert second.created_at == first.created_at


class TestMarkPublished:
    def test_sets_publish_state_and_preserves_other_fields(self, releases_table):
        persist_discovered_release(dataset="datacite-funders", release=make_release("2026-01-01"), run_id="run-1")

        mark_published(
            dataset="datacite-funders",
            release_date="2026-01-01",
            export_path="datacite/funders/2026-01-01/full/",
            release_type="full",
        )

        record = get_release(dataset="datacite-funders", release_date="2026-01-01")
        assert record.published_at is not None
        assert record.export_path == "datacite/funders/2026-01-01/full/"
        assert record.release_type == "full"
        assert record.run_id == "run-1"
        assert record.updated_at == record.published_at

    def test_raises_when_release_missing(self, releases_table):
        with pytest.raises(UpdateError):
            mark_published(
                dataset="datacite-funders",
                release_date="2099-01-01",
                export_path="datacite/funders/2099-01-01/full/",
                release_type="full",
            )

    def test_publish_state_survives_persist_rerun(self, releases_table):
        release = make_release("2026-01-01")
        persist_discovered_release(dataset="datacite-funders", release=release, run_id="run-a")
        mark_published(
            dataset="datacite-funders",
            release_date="2026-01-01",
            export_path="datacite/funders/2026-01-01/full/",
            release_type="full",
        )

        persist_discovered_release(dataset="datacite-funders", release=release, run_id="run-b")

        record = get_release(dataset="datacite-funders", release_date="2026-01-01")
        assert record.run_id == "run-b"
        assert record.published_at is not None
        assert record.export_path == "datacite/funders/2026-01-01/full/"


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
