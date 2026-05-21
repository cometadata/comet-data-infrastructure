from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Param, dag, get_current_context, task

from comet.airflow.assets import DATACITE_RELEASE_ASSET, DATACITE_RESOURCE_TYPE_GENERAL_ASSET
from comet.airflow.utils import (
    build_release_asset_metadata,
    get_current_run_id,
    get_triggering_release_key_or_none,
    resolve_release_record,
)
from comet.aws import batch_job_definition_name, batch_job_name, batch_job_queue_name
from comet.constants import DATACITE_DATASET_NAME, DATACITE_RESOURCE_TYPE_GENERAL_DATASET_NAME
from comet.dags.datacite_enrich_params import DataCiteEnrichParams, enrich_trigger_params
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease
from comet.utils import get_env

ENRICH_VCPU = "8"
ENRICH_MEMORY = "15360"

BATCH_ATTEMPT_TIMEOUT = 3 * 60 * 60


def create_datacite_enrich_resource_type_general_dag(dag_id: str, params: DataCiteEnrichParams) -> DAG:
    """Build a DAG that enriches DataCite's resourceTypeGeneral, triggered by a new release.

    Args:
        dag_id: Airflow DAG ID.
        params: Validated enrichment parameters.

    Returns:
        The constructed DAG.
    """

    @dag(
        dag_id=dag_id,
        schedule=[DATACITE_RELEASE_ASSET],
        params={
            **enrich_trigger_params(params),
            "rules_path": Param(
                "enrichment-configs/resource-type-general-reclassification-rules.yaml",
                type="string",
                title="Reclassification rules path",
                description="S3 path (within the bucket) of the resourceTypeGeneral reclassification rules YAML.",
            ),
            "enrichment_metadata_path": Param(
                "enrichment-configs/resource-type-general-enrichment-metadata.yaml",
                type="string",
                title="Enrichment metadata path",
                description="S3 path (within the bucket) of the resourceTypeGeneral enrichment metadata YAML.",
            ),
        },
        user_defined_macros={
            "get_env": get_env,
            "batch_job_name": batch_job_name,
            "batch_job_queue_name": batch_job_queue_name,
            "batch_job_definition_name": batch_job_definition_name,
        },
        **params.dag_kwargs(),
    )
    def enrich_dag():
        @task
        def fetch_datacite_release() -> dict:
            key = get_triggering_release_key_or_none(DATACITE_RELEASE_ASSET)
            release_date = get_current_context()["params"]["release_date"]
            record = resolve_release_record(key, dataset=DATACITE_DATASET_NAME, release_date=release_date)

            return record.to_dataset_release().to_dict()

        enrich = BatchOperator(
            task_id="enrich",
            job_name="{{ batch_job_name(get_env(), 'enrich-resource-type-general') }}",
            job_queue="{{ batch_job_queue_name(get_env(), 'enrich-resource-type-general') }}",
            job_definition="{{ batch_job_definition_name(get_env(), 'enrich') }}",
            container_overrides={
                "resourceRequirements": [
                    {"type": "VCPU", "value": ENRICH_VCPU},
                    {"type": "MEMORY", "value": ENRICH_MEMORY},
                ],
                "command": [
                    "comet",
                    "datacite",
                    "enrich",
                    "resource-type-general",
                    "--input-uri",
                    "s3://{{ params.bucket_name }}/{{ params.datacite_dag_id }}"
                    "/{{ ti.xcom_pull(task_ids='fetch_datacite_release')['run_id'] }}/",
                    "--output-uri",
                    "s3://{{ params.bucket_name }}/" + dag_id + "/{{ run_id }}/",
                    "--rules-uri",
                    "s3://{{ params.bucket_name }}/{{ params.rules_path }}",
                    "--enrichment-uri",
                    "s3://{{ params.bucket_name }}/{{ params.enrichment_metadata_path }}",
                ],
            },
            submit_job_timeout=BATCH_ATTEMPT_TIMEOUT,
            awslogs_enabled=True,
            deferrable=True,
        )

        @task
        def persist_release(release: dict):
            dataset_releases.persist_discovered_release(
                dataset=DATACITE_RESOURCE_TYPE_GENERAL_DATASET_NAME,
                release=DatasetRelease.from_dict(release),
                run_id=get_current_run_id(),
            )

        @task(outlets=[DATACITE_RESOURCE_TYPE_GENERAL_ASSET])
        def publish_release_asset(release: dict):
            release = DatasetRelease.from_dict(release)
            yield build_release_asset_metadata(
                asset=DATACITE_RESOURCE_TYPE_GENERAL_ASSET,
                dataset=DATACITE_RESOURCE_TYPE_GENERAL_DATASET_NAME,
                release_date=release.release_date,
            )

        release_task = fetch_datacite_release()
        release_task >> enrich >> persist_release(release_task) >> publish_release_asset(release_task)

    return enrich_dag()
