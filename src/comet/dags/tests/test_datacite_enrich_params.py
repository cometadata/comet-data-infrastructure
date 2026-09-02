from __future__ import annotations

import datetime

from airflow.sdk.exceptions import ParamValidationError
from pydantic import ValidationError
import pytest

from comet.dags.datacite_enrich_params import (
    DOI_PATTERN,
    DataCiteEnrichFundersParams,
    DataCiteEnrichParams,
    enrich_trigger_params,
)

START = datetime.datetime(2026, 1, 1)
SOURCE_ID = "10.1234/example"


class TestDataCiteEnrichParams:
    @pytest.mark.parametrize(
        ("source_id", "is_valid"),
        [
            pytest.param("10.1234/example", True, id="lowercase-suffix"),
            pytest.param("10.500.100/UPPER CASE", True, id="multiple-prefix-parts-and-uppercase-suffix"),
            pytest.param("https://doi.org/10.1234/example", False, id="url-form"),
            pytest.param("10.x/example", False, id="non-numeric-prefix"),
            pytest.param("10.١٢٣/example", False, id="non-ascii-prefix-digits"),
            pytest.param("10.1234/", False, id="empty-suffix"),
        ],
    )
    def test_doi_pattern(self, source_id, is_valid):
        assert (DOI_PATTERN.fullmatch(source_id) is not None) is is_valid

    @pytest.mark.parametrize(
        "source_id",
        [
            pytest.param("https://doi.org/10.1234/example", id="url-form"),
            pytest.param("10.x/example", id="non-numeric-prefix"),
            pytest.param("10.١٢٣/example", id="non-ascii-prefix-digits"),
            pytest.param("10.1234/", id="empty-suffix"),
        ],
    )
    def test_source_id_must_be_a_doi(self, source_id):
        with pytest.raises(ValidationError):
            DataCiteEnrichParams(start_date=START, bucket_name="comet-dev-s3-data", source_id=source_id)


class TestEnrichTriggerParams:
    def test_params_default_from_yaml_and_keep_the_doi_rule(self):
        params = DataCiteEnrichFundersParams(
            bucket_name="comet-dev-s3-data", source_id=SOURCE_ID, datacite_dag_id="custom_ingest", start_date=START
        )
        trigger = enrich_trigger_params(params)

        # Each param defaults to the validated YAML value.
        assert set(trigger) == {"source_id", "datacite_dag_id", "release_date"}
        assert trigger["source_id"].value == SOURCE_ID
        assert trigger["datacite_dag_id"].value == "custom_ingest"
        assert trigger["release_date"].value is None
        # A Trigger-form override is held to the same rule as the YAML.
        with pytest.raises(ParamValidationError):
            trigger["source_id"].resolve("10.١٢٣/example")
