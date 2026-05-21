from __future__ import annotations

import datetime

from comet.dags.datacite_enrich_params import DataCiteEnrichFundersParams, enrich_trigger_params

START = datetime.datetime(2026, 1, 1)


class TestEnrichTriggerParams:
    def test_param_defaults_come_from_the_yaml_values(self):
        params = DataCiteEnrichFundersParams(
            bucket_name="comet-dev-s3-data", datacite_dag_id="datacite_ingest", start_date=START
        )
        trigger = enrich_trigger_params(params)

        # Each param defaults to the validated YAML value.
        assert set(trigger) == {"bucket_name", "datacite_dag_id", "release_date"}
        assert trigger["bucket_name"].value == "comet-dev-s3-data"
        assert trigger["datacite_dag_id"].value == "datacite_ingest"
        assert trigger["release_date"].value is None
