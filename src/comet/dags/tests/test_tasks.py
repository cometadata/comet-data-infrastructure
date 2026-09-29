from __future__ import annotations

from contextlib import nullcontext
import datetime
from types import SimpleNamespace

from airflow import DAG
from airflow.sdk import Asset
from airflow.sdk.exceptions import AirflowException
import pytest

from comet.constants import DATACITE_FUNDERS_ENRICHMENT
import comet.dags.tasks as tasks
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease

START_DATE = datetime.datetime(2026, 1, 1)
RELEASE = DatasetRelease(release_date=datetime.date(2026, 1, 2))


class TestFetchRorRelease:
    def test_resolved_ror_release_includes_its_date_and_uri(self, mocker):
        mocker.patch.object(
            tasks,
            "get_current_context",
            return_value={"params": {"ror_dag_id": "ror_ingest", "ror_release_date": None}},
        )
        record = SimpleNamespace(release_date="2026-02-03", run_id="ror-run", file_name="ror.zip")
        mock_resolve = mocker.patch.object(tasks, "resolve_release_record", return_value=record)

        resolved = tasks.fetch_ror_release.function(bucket_name="test-bucket")

        mock_resolve.assert_called_once_with(dataset="ror", release_date=None)
        assert resolved == {"release_date": "2026-02-03", "uri": "s3://test-bucket/ror_ingest/ror-run/ror.zip"}


class TestCheckEnrichmentUnpublished:
    @pytest.mark.parametrize(
        ("published_at", "replace_published", "passes"),
        [
            (None, False, True),
            ("2026-01-03T00:00:00+00:00", False, False),
            ("2026-01-03T00:00:00+00:00", True, True),
        ],
        ids=["unpublished", "published", "published-with-replace"],
    )
    def test_rejects_a_published_release_unless_replacing_it(self, published_at, replace_published, passes, mocker):
        mocker.patch.object(
            tasks, "get_current_context", return_value={"params": {"replace_published": replace_published}}
        )
        mocker.patch.object(dataset_releases, "get_release", return_value=SimpleNamespace(published_at=published_at))

        expectation = nullcontext() if passes else pytest.raises(AirflowException, match="already published")
        with expectation:
            tasks.check_enrichment_unpublished.function(RELEASE.to_dict(), dataset="datacite-funders")


class TestResolvePreviousRelease:
    def test_looks_up_the_release_before_the_current_one(self, mocker):
        baseline = {"full_source_prefix": "dag/prev-run/full/", "release_date": "2025-12-02"}
        mock_previous = mocker.patch.object(tasks, "previous_release", return_value=baseline)

        previous = tasks.resolve_previous_release.function(RELEASE.to_dict(), dataset="datacite-funders")

        mock_previous.assert_called_once_with(dataset="datacite-funders", release_date="2026-01-02")
        assert previous == baseline


class TestDiffRelease:
    def test_batch_command_diffs_the_previous_full_release_against_the_new_one(self):
        with DAG("diff_test", start_date=START_DATE):
            previous = tasks.resolve_previous_release(RELEASE.to_dict(), dataset="datacite-funders")
            diff = tasks.diff_release(
                bucket_name="test-bucket",
                run_uri="s3://test-bucket/diff_test/{{ run_id }}/",
                enrichment=DATACITE_FUNDERS_ENRICHMENT,
                previous=previous,
                attempt_timeout=60,
            )

        assert diff.task_id == "diff"
        assert diff.job_name == "{{ batch_job_name(get_env(), 'diff-funders') }}"
        assert diff.job_queue == "{{ batch_job_queue_name(get_env(), 'enrich-funders') }}"
        assert diff.job_definition == "{{ batch_job_definition_name(get_env(), 'enrich') }}"
        assert diff.submit_job_timeout == 60
        command = diff.container_overrides["command"]
        assert command[:2] == ["comet", "diff"]
        args = dict(zip(command[2::2], command[3::2], strict=True))
        assert args["--old-uri"] == (
            "s3://test-bucket/"
            "{{ task_instance.xcom_pull(task_ids='resolve_previous_release', dag_id='diff_test', key='full_source_prefix') }}"
        )
        assert args["--new-uri"] == "s3://test-bucket/diff_test/{{ run_id }}/full/"
        assert args["--output-uri"] == "s3://test-bucket/diff_test/{{ run_id }}/diff/"


class TestPersistRelease:
    def test_stores_the_run_prefix(self, mocker):
        mock_persist = mocker.patch.object(dataset_releases, "persist_discovered_release")
        mocker.patch.object(tasks, "get_current_run_id", return_value="run-1")

        tasks.persist_release.function(RELEASE.to_dict(), dataset="ror", dag_id="prefix_test")

        mock_persist.assert_called_once_with(
            dataset="ror", release=RELEASE, run_id="run-1", source_prefix="prefix_test/run-1/"
        )


class TestPersistEnrichmentRelease:
    @pytest.fixture(autouse=True)
    def params(self, mocker):
        mocker.patch.object(tasks, "get_current_context", return_value={"params": {"replace_published": False}})

    @pytest.mark.parametrize(
        ("previous", "diff_source_prefix", "diff_base_release_date"),
        [
            pytest.param(
                {"full_source_prefix": "prefix_test/prev-run/full/", "release_date": "2025-12-02"},
                "prefix_test/run-1/diff/",
                "2025-12-02",
                id="with-diff",
            ),
            # A skipped resolve task leaves no XCom behind, so the diff was skipped too.
            pytest.param(None, None, None, id="first-release"),
        ],
    )
    def test_stores_the_directories_and_the_diff_baseline(
        self, previous, diff_source_prefix, diff_base_release_date, mocker
    ):
        mock_persist = mocker.patch.object(dataset_releases, "persist_discovered_release")
        mocker.patch.object(tasks, "get_current_run_id", return_value="run-1")

        tasks.persist_enrichment_release.function(
            RELEASE.to_dict(), dataset="datacite-funders", dag_id="prefix_test", previous=previous
        )

        mock_persist.assert_called_once_with(
            dataset="datacite-funders",
            release=RELEASE,
            run_id="run-1",
            source_prefix="prefix_test/run-1/",
            full_source_prefix="prefix_test/run-1/full/",
            diff_source_prefix=diff_source_prefix,
            diff_base_release_date=diff_base_release_date,
            replace_published=False,
        )

    def test_forwards_the_replace_published_param(self, mocker):
        mocker.patch.object(tasks, "get_current_context", return_value={"params": {"replace_published": True}})
        mock_persist = mocker.patch.object(dataset_releases, "persist_discovered_release")
        mocker.patch.object(tasks, "get_current_run_id", return_value="run-1")

        tasks.persist_enrichment_release.function(
            RELEASE.to_dict(), dataset="datacite-funders", dag_id="prefix_test", previous=None
        )

        assert mock_persist.call_args.kwargs["replace_published"] is True

    def test_keeps_the_persist_task_id_and_runs_when_the_diff_was_skipped(self):
        with DAG("trigger_test", start_date=START_DATE):
            persisted = tasks.persist_enrichment_release(
                RELEASE.to_dict(), dataset="datacite-funders", dag_id="trigger_test", previous=None
            )

        assert persisted.operator.task_id == "persist_release"
        assert persisted.operator.trigger_rule == "none_failed"


class TestPublishReleaseAsset:
    def test_publishes_the_release_key_on_the_asset(self):
        asset = Asset("datacite-funders")
        callback = object()
        publish = tasks.publish_release_asset(asset=asset, dataset="datacite-funders", on_success_callback=callback)
        with DAG("publish_test", start_date=START_DATE):
            published = publish(RELEASE.to_dict())

        assert published.operator.task_id == "publish_release_asset"
        assert published.operator.outlets == [asset]
        assert published.operator.on_success_callback == [callback]
        (metadata,) = publish.function(RELEASE.to_dict())
        assert metadata.asset == asset
        assert metadata.extra == {"dataset": "datacite-funders", "release_date": "2026-01-02"}
