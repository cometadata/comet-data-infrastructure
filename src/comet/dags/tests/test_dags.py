"""Tests for the real DAG factories in comet.dags and the shipped dags.yaml.example."""

from __future__ import annotations

from collections.abc import Callable
import datetime
import importlib
import inspect
from pathlib import Path
import pkgutil
import shutil
from types import SimpleNamespace
from typing import NamedTuple

from airflow import DAG
from airflow.sdk import AssetAny
from airflow.sdk.exceptions import AirflowException, AirflowSkipException
import pytest
from pydantic import ValidationError
import yaml

import comet
from comet.airflow import BaseDagParams
from comet.airflow.assets import (
    DATACITE_AFFILIATIONS_ASSET,
    DATACITE_FUNDERS_ASSET,
    DATACITE_RELEASE_ASSET,
    DATACITE_RESOURCE_TYPE_GENERAL_ASSET,
)
from comet.airflow.config import DagsConfig
from comet.airflow.loader import load_dags, resolve_factory
import comet.dags
from comet.dags.datacite_enrich_affiliations import create_datacite_enrich_affiliations_dag
from comet.dags.datacite_enrich_funders import create_datacite_enrich_funders_dag
from comet.dags.datacite_enrich_params import (
    DataCiteEnrichAffiliationsParams,
    DataCiteEnrichFundersParams,
    DataCiteEnrichParams,
)
from comet.dags.datacite_enrich_resource_type_general import create_datacite_enrich_resource_type_general_dag
from comet.dags.datacite_ingest import DataCiteIngestParams, create_datacite_ingest_dag
import comet.dags.publish_enrichments as publish_enrichments
from comet.dags.publish_enrichments import PublishEnrichmentsParams, create_publish_enrichments_dag
from comet.dags.ror_ingest import RorIngestParams, create_ror_ingest_dag
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease

REPO_ROOT = Path(comet.__file__).parents[2]
EXAMPLE_CONFIG = REPO_ROOT / "dags" / "dags.yaml.example"

START_DATE = datetime.datetime(2026, 1, 1)
HF_ENDPOINT_URL = "https://s3.hf.co/test-namespace"


def test_publish_params_require_hf_endpoint():
    with pytest.raises(ValidationError, match="hf_endpoint_url"):
        PublishEnrichmentsParams(
            start_date=START_DATE,
            source="datacite",
            bucket_name="test-bucket",
            hf_bucket_name="test-hf-bucket",
        )


class DagCase(NamedTuple):
    dag_id: str
    factory: Callable
    params: BaseDagParams
    schedule: str | list
    # Expected downstream task ids per task; the keys double as the expected task set.
    edges: dict[str, set[str]]


DAG_CASES = [
    DagCase(
        "ror_ingest",
        create_ror_ingest_dag,
        RorIngestParams(start_date=START_DATE, bucket_name="test-bucket"),
        "@daily",
        {
            "fetch_release": {"download", "persist_discovered_release", "publish_release_asset"},
            "download": {"persist_discovered_release"},
            "persist_discovered_release": {"publish_release_asset"},
            "publish_release_asset": set(),
        },
    ),
    DagCase(
        "datacite_ingest",
        create_datacite_ingest_dag,
        DataCiteIngestParams(
            start_date=START_DATE,
            bucket_name="test-bucket",
            datacite_bucket_name="test-datacite-bucket",
            datacite_bucket_region="eu-west-1",
        ),
        "@daily",
        {
            "fetch_release": {"download", "persist_discovered_release", "publish_release_asset"},
            "download": {"persist_discovered_release"},
            "persist_discovered_release": {"publish_release_asset"},
            "publish_release_asset": set(),
        },
    ),
    DagCase(
        "datacite_enrich_resource_type_general",
        create_datacite_enrich_resource_type_general_dag,
        DataCiteEnrichParams(start_date=START_DATE, bucket_name="test-bucket"),
        [DATACITE_RELEASE_ASSET],
        {
            "fetch_datacite_release": {"enrich", "persist_release", "publish_release_asset"},
            "enrich": {"persist_release"},
            "persist_release": {"publish_release_asset"},
            "publish_release_asset": set(),
        },
    ),
    DagCase(
        "datacite_enrich_funders",
        create_datacite_enrich_funders_dag,
        DataCiteEnrichFundersParams(start_date=START_DATE, bucket_name="test-bucket"),
        [DATACITE_RELEASE_ASSET],
        {
            "fetch_datacite_release": {"enrich", "persist_release", "publish_release_asset"},
            "fetch_ror_release": {"enrich"},
            "enrich": {"persist_release"},
            "persist_release": {"publish_release_asset"},
            "publish_release_asset": set(),
        },
    ),
    DagCase(
        "datacite_publish",
        create_publish_enrichments_dag,
        PublishEnrichmentsParams(
            start_date=START_DATE,
            source="datacite",
            bucket_name="test-bucket",
            hf_bucket_name="test-hf-bucket",
            hf_endpoint_url=HF_ENDPOINT_URL,
        ),
        AssetAny(DATACITE_RESOURCE_TYPE_GENERAL_ASSET, DATACITE_FUNDERS_ASSET, DATACITE_AFFILIATIONS_ASSET),
        {
            "resolve_releases": {"publish"},
            "publish": set(),
        },
    ),
    DagCase(
        "datacite_enrich_affiliations",
        create_datacite_enrich_affiliations_dag,
        DataCiteEnrichAffiliationsParams(start_date=START_DATE, bucket_name="test-bucket"),
        [DATACITE_RELEASE_ASSET],
        {
            "fetch_datacite_release": {"enrich", "persist_release", "publish_release_asset"},
            "fetch_ror_release": {"enrich"},
            "enrich": {"persist_release"},
            "persist_release": {"publish_release_asset"},
            "publish_release_asset": set(),
        },
    ),
]


class EnrichCase(NamedTuple):
    factory: Callable
    params: BaseDagParams
    dataset: str
    uses_ror: bool


ENRICH_CASES = [
    EnrichCase(
        create_datacite_enrich_funders_dag,
        DataCiteEnrichFundersParams(start_date=START_DATE, bucket_name="test-bucket"),
        "datacite-funders",
        uses_ror=True,
    ),
    EnrichCase(
        create_datacite_enrich_affiliations_dag,
        DataCiteEnrichAffiliationsParams(start_date=START_DATE, bucket_name="test-bucket"),
        "datacite-affiliations",
        uses_ror=True,
    ),
    EnrichCase(
        create_datacite_enrich_resource_type_general_dag,
        DataCiteEnrichParams(start_date=START_DATE, bucket_name="test-bucket"),
        "datacite-resource-type-general",
        uses_ror=False,
    ),
]

ROR_CASES = [case for case in ENRICH_CASES if case.uses_ror]


class TestDags:
    @pytest.mark.parametrize("case", DAG_CASES, ids=lambda c: c.dag_id)
    def test_dag_loads(self, case):
        dag = case.factory(case.dag_id, case.params)
        assert isinstance(dag, DAG)
        assert dag.dag_id == case.dag_id

    @pytest.mark.parametrize("case", DAG_CASES, ids=lambda c: c.dag_id)
    def test_dag_structure(self, case):
        dag = case.factory(case.dag_id, case.params)
        assert dag.schedule == case.schedule
        assert set(dag.task_ids) == set(case.edges)
        for task_id, downstream in case.edges.items():
            assert dag.get_task(task_id).downstream_task_ids == downstream

    def test_example_config_loads(self, tmp_path):
        # Verbatim: <your-s3-bucket> placeholders are valid strings for the params models.
        shutil.copy(EXAMPLE_CONFIG, tmp_path / "dags.yaml")
        globals_dict = {"__file__": str(tmp_path / "dags.py")}
        load_dags(globals_dict)
        config = DagsConfig.model_validate(yaml.safe_load(EXAMPLE_CONFIG.read_text()))
        enabled = [entry for entry in config.dags if entry.enabled]
        assert enabled
        for entry in enabled:
            assert isinstance(globals_dict[entry.dag_id], DAG)

    def test_example_config_complete(self):
        discovered = set()
        for module_info in pkgutil.iter_modules(comet.dags.__path__):
            if module_info.ispkg:
                continue
            module = importlib.import_module(f"comet.dags.{module_info.name}")
            for name, obj in vars(module).items():
                if not (inspect.isfunction(obj) and obj.__module__ == module.__name__):
                    continue
                dotted = f"{module.__name__}.{name}"
                try:
                    resolve_factory(dotted)
                except TypeError:
                    continue
                discovered.add(dotted)
        config = DagsConfig.model_validate(yaml.safe_load(EXAMPLE_CONFIG.read_text()))
        declared = {entry.factory for entry in config.dags}
        assert declared == discovered

    def test_example_config_uses_comet_hugging_face_destination(self):
        config = DagsConfig.model_validate(yaml.safe_load(EXAMPLE_CONFIG.read_text()))
        publish = next(entry for entry in config.dags if entry.dag_id == "datacite_publish")
        assert publish.params["hf_bucket_name"] == "comet-enrichments"
        assert publish.params["hf_endpoint_url"] == "https://s3.hf.co/cometadata"


class TestPublishEnrichmentsDag:
    @pytest.fixture
    def dag(self):
        params = PublishEnrichmentsParams(
            start_date=START_DATE,
            source="datacite",
            bucket_name="test-bucket",
            hf_bucket_name="test-hf-bucket",
            hf_endpoint_url=HF_ENDPOINT_URL,
        )
        return create_publish_enrichments_dag("datacite_publish", params)

    @staticmethod
    def patch_resolve(mocker, latest: dict[str, str]):
        """Resolve each dataset to its release date in ``latest`` with a stubbed record."""
        mocker.patch.object(
            publish_enrichments,
            "resolve_release_record",
            side_effect=lambda *, dataset, release_date: SimpleNamespace(
                run_id=f"run-{dataset}",
                release_date=latest[dataset],
                source_prefix=f"enrich_{dataset}/run-{dataset}/",
            ),
        )

    @staticmethod
    def patch_context(mocker, datasets: list[str], release_date: str | None = None):
        mocker.patch.object(
            publish_enrichments,
            "get_current_context",
            return_value={"params": {"bucket_name": "test-bucket", "release_date": release_date, "datasets": datasets}},
        )

    def test_publishes_record_source_uris_when_latest_releases_align(self, dag, mocker):
        latest = {
            "datacite-funders": "2026-01-02",
            "datacite-affiliations": "2026-01-02",
            "datacite-resource-type-general": "2026-01-02",
        }
        self.patch_resolve(mocker, latest)
        self.patch_context(mocker, datasets=list(latest))

        resolved = dag.get_task("resolve_releases").python_callable()

        assert resolved == {
            "release_date": "2026-01-02",
            "source_uris": {dataset: f"s3://test-bucket/enrich_{dataset}/run-{dataset}/" for dataset in latest},
        }

    @pytest.mark.parametrize(
        ("asset_triggered", "expected"),
        [(True, AirflowSkipException), (False, AirflowException)],
        ids=["asset-run-skips", "manual-run-fails"],
    )
    def test_misaligned_latest_releases_skip_asset_runs_and_fail_manual_runs(
        self, dag, mocker, asset_triggered, expected
    ):
        latest = {
            "datacite-funders": "2026-01-03",
            "datacite-affiliations": "2026-01-02",
            "datacite-resource-type-general": "2026-01-02",
        }
        self.patch_resolve(mocker, latest)
        self.patch_context(mocker, datasets=list(latest))
        mocker.patch("comet.airflow.utils.is_asset_triggered", return_value=asset_triggered)

        with pytest.raises(expected, match="not aligned") as excinfo:
            dag.get_task("resolve_releases").python_callable()
        assert type(excinfo.value) is expected

    def test_manual_run_publishes_only_selected_datasets(self, dag, mocker):
        self.patch_resolve(mocker, {"datacite-funders": "2026-01-02"})
        self.patch_context(mocker, datasets=["datacite-funders"], release_date="2026-01-02")

        resolved = dag.get_task("resolve_releases").python_callable()

        assert resolved["source_uris"] == {
            "datacite-funders": "s3://test-bucket/enrich_datacite-funders/run-datacite-funders/",
        }

    def test_fails_even_when_asset_triggered_if_record_has_no_source_prefix(self, dag, mocker):
        mocker.patch.object(
            publish_enrichments,
            "resolve_release_record",
            side_effect=lambda *, dataset, release_date: SimpleNamespace(
                run_id=f"run-{dataset}", release_date="2026-01-02", source_prefix=None
            ),
        )
        self.patch_context(mocker, datasets=["datacite-funders"])
        mocker.patch("comet.airflow.utils.is_asset_triggered", return_value=True)

        with pytest.raises(AirflowException, match="no source_prefix") as excinfo:
            dag.get_task("resolve_releases").python_callable()
        assert type(excinfo.value) is AirflowException

    def test_batch_command_invokes_generic_publish_cli(self, dag):
        command = dag.get_task("publish").container_overrides["command"]
        assert command[:4] == ["comet", "publish", "--source", "datacite"]


class TestEnrichFetchRorRelease:
    @pytest.mark.parametrize("case", ROR_CASES, ids=lambda c: c.dataset)
    def test_resolved_ror_release_includes_its_date_and_uri(self, case, mocker):
        dag = case.factory("enrich_test", case.params)
        module = inspect.getmodule(case.factory)
        mocker.patch.object(
            module,
            "get_current_context",
            return_value={
                "params": {"bucket_name": "test-bucket", "ror_dag_id": "ror_ingest", "ror_release_date": None}
            },
        )
        record = SimpleNamespace(release_date="2026-02-03", run_id="ror-run", file_name="ror.zip")
        mock_resolve = mocker.patch.object(module, "resolve_release_record", return_value=record)

        resolved = dag.get_task("fetch_ror_release").python_callable()

        mock_resolve.assert_called_once_with(dataset="ror", release_date=None)
        assert resolved == {"release_date": "2026-02-03", "uri": "s3://test-bucket/ror_ingest/ror-run/ror.zip"}


class TestEnrichPersistRelease:
    @pytest.mark.parametrize("case", ENRICH_CASES, ids=lambda c: c.dataset)
    def test_persist_release_stores_the_enrich_output_prefix(self, case, mocker):
        dag = case.factory("enrich_test", case.params)
        mock_persist = mocker.patch.object(dataset_releases, "persist_discovered_release")
        mocker.patch.object(inspect.getmodule(case.factory), "get_current_run_id", return_value="run-1")
        release = DatasetRelease(release_date=datetime.date(2026, 1, 2))

        dag.get_task("persist_release").python_callable(release.to_dict())

        mock_persist.assert_called_once_with(
            dataset=case.dataset,
            release=release,
            run_id="run-1",
            source_prefix="enrich_test/run-1/",
        )
