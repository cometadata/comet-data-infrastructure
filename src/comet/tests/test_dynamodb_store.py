import datetime

from pynamodb.exceptions import UpdateError
import pytest

from comet.dynamodb_store import (
    DatasetReleaseRecord,
    get_latest_published_release,
    get_latest_release,
    get_previous_release,
    get_release,
    list_all_releases,
    list_releases,
    mark_published,
    mark_release_pruned,
    persist_discovered_release,
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
            persist_discovered_release(
                dataset="ror", release=make_release(d), run_id=f"run-{d}", source_prefix=f"ror_ingest/run-{d}/"
            )

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
        persist_discovered_release(
            dataset="ror", release=make_release("2025-01-01"), run_id="run-1", source_prefix="ror_ingest/run-1/"
        )

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


def test_list_all_releases_uses_a_consistent_scan(mocker):
    scan = mocker.patch.object(DatasetReleaseRecord, "scan", return_value=iter([]))

    assert list_all_releases() == []
    scan.assert_called_once_with(consistent_read=True)


class TestToDatasetRelease:
    def test_round_trips_record_fields_including_run_id(self, releases_table):
        persist_discovered_release(
            dataset="ror",
            release=make_release("2025-01-01", metadata={"k": "v"}),
            run_id="run-xyz",
            source_prefix="ror_ingest/run-xyz/",
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

    def test_recreating_pruned_release_clears_pruned_state(self, releases_table):
        release = make_release("2025-02-01")
        first = persist_discovered_release(
            dataset="ror", release=release, run_id="run-a", source_prefix="ror_ingest/run-a/"
        )
        mark_release_pruned(
            dataset="ror",
            release_date="2025-02-01",
            expected_source_prefix="ror_ingest/run-a/",
        )

        persist_discovered_release(dataset="ror", release=release, run_id="run-b", source_prefix="ror_ingest/run-b/")

        refreshed = get_release(dataset="ror", release_date="2025-02-01")
        assert refreshed.pruned_at is None
        assert refreshed.run_id == "run-b"
        assert refreshed.source_prefix == "ror_ingest/run-b/"
        assert refreshed.created_at == first.created_at


class TestMarkPublished:
    @pytest.mark.parametrize(
        "diff_export_path", [None, "datacite/funders/2026-01-01/diff/"], ids=["full-only", "with-diff"]
    )
    def test_sets_publish_state_and_preserves_other_fields(self, releases_table, diff_export_path):
        record = persist_discovered_release(
            dataset="datacite-funders",
            release=make_release("2026-01-01"),
            run_id="run-1",
            source_prefix="datacite_enrich_funders/run-1/",
        )

        mark_published(
            dataset="datacite-funders",
            release_date="2026-01-01",
            export_path="datacite/funders/2026-01-01/full/",
            diff_export_path=diff_export_path,
            expected_updated_at=record.updated_at,
        )

        record = get_release(dataset="datacite-funders", release_date="2026-01-01")
        assert record.published_at is not None
        assert record.export_path == "datacite/funders/2026-01-01/full/"
        assert record.diff_export_path == diff_export_path
        assert record.run_id == "run-1"
        assert record.updated_at == record.published_at

    def test_raises_when_release_missing(self, releases_table):
        with pytest.raises(UpdateError):
            mark_published(
                dataset="datacite-funders",
                release_date="2099-01-01",
                export_path="datacite/funders/2099-01-01/full/",
                diff_export_path=None,
                expected_updated_at="2099-01-01T00:00:00+00:00",
            )


class TestReleaseDirectoryFields:
    def test_persist_sets_and_clears_the_directories_and_the_diff_baseline(self, releases_table):
        release = make_release("2026-02-02")
        persist_discovered_release(
            dataset="datacite-funders",
            release=release,
            run_id="run-2",
            source_prefix="datacite_enrich_funders/run-2/",
            full_source_prefix="datacite_enrich_funders/run-2/full/",
            diff_source_prefix="datacite_enrich_funders/run-2/diff/",
            diff_base_release_date="2026-01-02",
        )
        record = get_release(dataset="datacite-funders", release_date="2026-02-02")
        assert record.full_source_prefix == "datacite_enrich_funders/run-2/full/"
        assert record.diff_source_prefix == "datacite_enrich_funders/run-2/diff/"
        assert record.diff_base_release_date == "2026-01-02"

        persist_discovered_release(
            dataset="datacite-funders",
            release=release,
            run_id="run-3",
            source_prefix="datacite_enrich_funders/run-3/",
        )
        record = get_release(dataset="datacite-funders", release_date="2026-02-02")
        assert record.full_source_prefix is None
        assert record.diff_source_prefix is None
        assert record.diff_base_release_date is None

    def test_published_release_rejects_persist_rerun(self, releases_table):
        release = make_release("2026-01-01")
        record = persist_discovered_release(
            dataset="datacite-funders",
            release=release,
            run_id="run-a",
            source_prefix="datacite_enrich_funders/run-a/",
        )
        mark_published(
            dataset="datacite-funders",
            release_date="2026-01-01",
            export_path="datacite/funders/2026-01-01/full/",
            diff_export_path=None,
            expected_updated_at=record.updated_at,
        )
        published = get_release(dataset="datacite-funders", release_date="2026-01-01")

        with pytest.raises(UpdateError):
            persist_discovered_release(
                dataset="datacite-funders",
                release=release,
                run_id="run-b",
                source_prefix="datacite_enrich_funders/run-b/",
            )

        record = get_release(dataset="datacite-funders", release_date="2026-01-01")
        assert record.serialize() == published.serialize()

    def test_replace_published_unpublishes_the_record_and_keeps_its_export_paths(self, releases_table):
        release = make_release("2026-01-01")
        record = persist_discovered_release(
            dataset="datacite-funders", release=release, run_id="run-a", source_prefix="datacite_enrich_funders/run-a/"
        )
        mark_published(
            dataset="datacite-funders",
            release_date="2026-01-01",
            export_path="datacite/funders/2026-01-01/full/",
            diff_export_path="datacite/funders/2026-01-01/diff/",
            expected_updated_at=record.updated_at,
        )

        persist_discovered_release(
            dataset="datacite-funders",
            release=release,
            run_id="run-b",
            source_prefix="datacite_enrich_funders/run-b/",
            full_source_prefix="datacite_enrich_funders/run-b/full/",
            replace_published=True,
        )

        record = get_release(dataset="datacite-funders", release_date="2026-01-01")
        assert record.published_at is None
        assert record.run_id == "run-b"
        assert record.full_source_prefix == "datacite_enrich_funders/run-b/full/"
        assert record.export_path == "datacite/funders/2026-01-01/full/"
        assert record.diff_export_path == "datacite/funders/2026-01-01/diff/"


def persist_enrichment(date_str: str, *, full: bool = True, published: bool = True, pruned: bool = False) -> None:
    prefix = f"datacite_enrich_funders/run-{date_str}/"
    record = persist_discovered_release(
        dataset="datacite-funders",
        release=make_release(date_str),
        run_id=f"run-{date_str}",
        source_prefix=prefix,
        full_source_prefix=prefix + "full/" if full else None,
    )
    if published:
        mark_published(
            dataset="datacite-funders",
            release_date=date_str,
            export_path=f"datacite/funders/{date_str}/full/",
            diff_export_path=None,
            expected_updated_at=record.updated_at,
        )
    if pruned:
        mark_release_pruned(dataset="datacite-funders", release_date=date_str, expected_source_prefix=prefix)


class TestGetPreviousRelease:
    def test_returns_the_latest_usable_release_strictly_before_the_date(self, releases_table):
        persist_enrichment("2026-01-02")
        persist_enrichment("2026-02-02")
        persist_enrichment("2026-03-02", pruned=True)
        # The current date's own earlier attempt must not match.
        persist_enrichment("2026-04-02")

        record = get_previous_release(dataset="datacite-funders", before="2026-04-02")

        assert record.release_date == "2026-02-02"
        assert record.full_source_prefix == "datacite_enrich_funders/run-2026-02-02/full/"

    @pytest.mark.parametrize(
        "setup",
        [
            lambda: None,
            lambda: persist_enrichment("2026-04-02"),
            lambda: persist_enrichment("2026-05-02"),
            lambda: persist_enrichment("2026-02-02", full=False),
            lambda: persist_enrichment("2026-02-02", published=False),
            lambda: persist_enrichment("2026-02-02", pruned=True),
        ],
        ids=["empty", "only-current-date", "only-later", "no-full-prefix", "unpublished", "pruned"],
    )
    def test_returns_none_when_no_earlier_usable_release_exists(self, releases_table, setup):
        setup()
        assert get_previous_release(dataset="datacite-funders", before="2026-04-02") is None

    def test_queries_descending_with_a_consistent_read(self, mocker):
        query = mocker.patch.object(DatasetReleaseRecord, "query", return_value=iter([]))

        assert get_previous_release(dataset="datacite-funders", before="2026-04-02") is None

        args, kwargs = query.call_args
        assert args[0] == "datacite-funders"
        assert kwargs == {"consistent_read": True, "scan_index_forward": False}
        assert str(args[1]) == str(DatasetReleaseRecord.release_date < "2026-04-02")


class TestGetLatestPublishedRelease:
    def test_skips_newer_unpublished_releases(self, releases_table):
        persist_enrichment("2026-01-02")
        persist_enrichment("2026-02-02")
        persist_enrichment("2026-03-02", published=False)

        record = get_latest_published_release(dataset="datacite-funders")

        assert record.release_date == "2026-02-02"

    def test_returns_none_when_nothing_is_published(self, releases_table):
        persist_enrichment("2026-01-02", published=False)

        assert get_latest_published_release(dataset="datacite-funders") is None


class TestMarkReleasePruned:
    def test_sets_pruned_state_and_preserves_other_fields(self, releases_table):
        persist_discovered_release(
            dataset="ror", release=make_release("2026-01-01"), run_id="run-1", source_prefix="ror_ingest/run-1/"
        )

        mark_release_pruned(
            dataset="ror",
            release_date="2026-01-01",
            expected_source_prefix="ror_ingest/run-1/",
        )

        record = get_release(dataset="ror", release_date="2026-01-01")
        assert record.pruned_at is not None
        assert record.updated_at == record.pruned_at
        assert record.source_prefix == "ror_ingest/run-1/"

    def test_raises_when_release_missing(self, releases_table):
        with pytest.raises(UpdateError):
            mark_release_pruned(
                dataset="ror",
                release_date="2099-01-01",
                expected_source_prefix="ror_ingest/missing/",
            )

    def test_rejects_mark_when_release_prefix_changed(self, releases_table):
        persist_discovered_release(
            dataset="ror", release=make_release("2026-01-01"), run_id="run-2", source_prefix="ror_ingest/run-2/"
        )

        with pytest.raises(UpdateError):
            mark_release_pruned(
                dataset="ror",
                release_date="2026-01-01",
                expected_source_prefix="ror_ingest/run-1/",
            )

        record = get_release(dataset="ror", release_date="2026-01-01")
        assert record.pruned_at is None
        assert record.source_prefix == "ror_ingest/run-2/"
