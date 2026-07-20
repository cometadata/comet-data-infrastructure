"""Tests for the real DAG factories in comet.dags and the shipped dags.yaml.example."""

from __future__ import annotations

from collections.abc import Callable
import datetime
import importlib
import inspect
from pathlib import Path
import pkgutil
import shutil
from typing import NamedTuple

from airflow import DAG
import pytest
import yaml

import comet
from comet.airflow import BaseDagParams
from comet.airflow.assets import DATACITE_RELEASE_ASSET
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
from comet.dags.ror_ingest import RorIngestParams, create_ror_ingest_dag

REPO_ROOT = Path(comet.__file__).parents[2]
EXAMPLE_CONFIG = REPO_ROOT / "dags" / "dags.yaml.example"

START_DATE = datetime.datetime(2026, 1, 1)


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
