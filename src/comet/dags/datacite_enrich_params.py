"""Shared params + Trigger-form schema for the DataCite enrichment DAGs.

The Pydantic ``*Params`` validate the deploy-time config (``dags.yaml``); :func:`enrich_trigger_params`
builds the Airflow ``Param`` objects that drive the Trigger form, wiring each default from the
validated YAML value. The two concerns are kept separate — fields stay plain, form schema lives here.
"""

from __future__ import annotations

import re

from airflow.sdk import Param
from pydantic import field_validator

from comet.airflow import BaseDagParams

# DOI name syntax, such as 10.1234/example, required by comet-enrich's --source-id option.
DOI_PATTERN = re.compile(r"^10\.[0-9]+(?:\.[0-9]+)*/.+$")


class DataCiteEnrichParams(BaseDagParams):
    """Params common to the DataCite enrichment DAGs.

    Attributes:
        bucket_name: S3 bucket holding the staged snapshot and enrichment output.
        source_id: DOI name of the enrichment project, such as ``10.1234/example``; written to every
            record's ``sourceId``.
        datacite_dag_id: Upstream DataCite ingest DAG id whose run_id keys the input S3 prefix.
        release_date: Manual runs only — which DataCite release (YYYY-MM-DD) to enrich; empty uses the
            latest DataCite release, ignored on asset-triggered runs.
    """

    bucket_name: str
    source_id: str
    datacite_dag_id: str = "datacite_ingest"
    release_date: str | None = None

    @field_validator("source_id")
    @classmethod
    def source_id_is_doi(cls, value: str) -> str:
        """Require a DOI name, such as 10.1234/example."""
        if not DOI_PATTERN.fullmatch(value):
            raise ValueError("The value must be a DOI name, such as 10.1234/example.")
        return value


class DataCiteEnrichFundersParams(DataCiteEnrichParams):
    """Funders-specific params (adds the ROR snapshot inputs).

    Attributes:
        ror_dag_id: ROR ingest DAG id whose snapshot seeds Marple and reconciles matches.
        ror_release_date: Which ROR release (YYYY-MM-DD) to use; empty resolves the latest ROR release.
    """

    ror_dag_id: str = "ror_ingest"
    ror_release_date: str | None = None


class DataCiteEnrichAffiliationsParams(DataCiteEnrichParams):
    """Affiliations-specific params (adds the ROR snapshot inputs).

    Attributes:
        ror_dag_id: ROR ingest DAG id whose snapshot seeds Marple and reconciles matches.
        ror_release_date: Which ROR release (YYYY-MM-DD) to use; empty resolves the latest ROR release.
    """

    ror_dag_id: str = "ror_ingest"
    ror_release_date: str | None = None


def enrich_trigger_params(params: DataCiteEnrichParams) -> dict[str, Param]:
    """Build the Trigger-form Airflow Params shared by the enrichment DAGs (defaults from YAML).

    Args:
        params: The validated enrichment params; each field supplies its Param's default.

    Returns:
        Mapping of param name to ``airflow.sdk.Param`` to pass as ``@dag(params=...)``.
    """
    return {
        "source_id": Param(
            params.source_id,
            type="string",
            pattern=DOI_PATTERN.pattern,
            title="Enrichment project DOI name",
            description=(
                "The enrichment project's DOI name, such as 10.1234/example. "
                "The value is written to the sourceId field of every output record."
            ),
        ),
        "datacite_dag_id": Param(
            params.datacite_dag_id,
            type="string",
            title="DataCite ingest DAG ID",
            description="Upstream DataCite ingest DAG whose run keys the input S3 prefix.",
        ),
        "release_date": Param(
            params.release_date,
            type=["null", "string"],
            format="date",
            title="DataCite release date",
            description=(
                "Which release to enrich on a manual run (YYYY-MM-DD). Empty = latest DataCite release. "
                "Ignored on asset-triggered runs."
            ),
        ),
    }
