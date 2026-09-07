from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Param, dag

from comet.airflow.assets import DATACITE_RELEASE_ASSET, DATACITE_RESOURCE_TYPE_GENERAL_ASSET
from comet.airflow.notifications import alert_kwargs
from comet.aws import (
    BATCH_JOB_TAGS,
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    run_prefix,
    s3_uri,
)
from comet.constants import DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT, FULL_DIR
from comet.dags.datacite_enrich_params import DataCiteEnrichParams, enrich_trigger_params
from comet.dags.tasks import (
    check_enrichment_unpublished,
    diff_release,
    fetch_datacite_release,
    persist_enrichment_release,
    publish_release_asset,
    resolve_previous_release,
)
from comet.utils import get_env

ENRICH_VCPU = "8"
ENRICH_MEMORY = "15360"
WRITER_LANES = "4"

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
        description="Reclassify DataCite resourceTypeGeneral values.",
        schedule=[DATACITE_RELEASE_ASSET],
        params={
            **enrich_trigger_params(params),
            "rules_path": Param(
                "enrichment-configs/resource-type-general-reclassification-rules.yaml",
                type="string",
                title="Reclassification rules path",
                description="S3 path (within the bucket) of the resourceTypeGeneral reclassification rules YAML.",
            ),
        },
        user_defined_macros={
            "get_env": get_env,
            "batch_job_name": batch_job_name,
            "batch_job_queue_name": batch_job_queue_name,
            "batch_job_definition_name": batch_job_definition_name,
        },
        **params.dag_kwargs(),
        **alert_kwargs(params.deadline_minutes),
    )
    def enrich_dag():
        datacite_input_uri = s3_uri(
            params.bucket_name,
            run_prefix(
                "{{ params.datacite_dag_id }}", "{{ ti.xcom_pull(task_ids='fetch_datacite_release')['run_id'] }}"
            ),
        )
        run_uri = s3_uri(params.bucket_name, run_prefix(dag_id, "{{ run_id }}"))

        enrich = BatchOperator(
            task_id="enrich",
            job_name="{{ batch_job_name(get_env(), 'enrich-resource-type-general') }}",
            job_queue="{{ batch_job_queue_name(get_env(), 'enrich-resource-type-general') }}",
            job_definition="{{ batch_job_definition_name(get_env(), 'enrich') }}",
            tags=BATCH_JOB_TAGS,
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
                    datacite_input_uri,
                    "--output-uri",
                    run_uri + FULL_DIR,
                    "--source-release-date",
                    "datacite={{ ti.xcom_pull(task_ids='fetch_datacite_release')['release_date'] }}",
                    "--rules-uri",
                    s3_uri(params.bucket_name, "{{ params.rules_path }}"),
                    "--source-id",
                    "{{ params.source_id }}",
                    "--output-writer-lanes",
                    WRITER_LANES,
                ],
            },
            submit_job_timeout=BATCH_ATTEMPT_TIMEOUT,
            awslogs_enabled=True,
            deferrable=True,
        )

        release = fetch_datacite_release()
        checked = check_enrichment_unpublished(release, dataset=DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier)
        previous = resolve_previous_release(release, dataset=DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier)
        diff = diff_release(
            bucket_name=params.bucket_name,
            run_uri=run_uri,
            enrichment=DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT,
            previous=previous,
            attempt_timeout=BATCH_ATTEMPT_TIMEOUT,
        )
        persisted = persist_enrichment_release(
            release,
            dataset=DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier,
            dag_id=dag_id,
            previous=previous,
        )
        publish = publish_release_asset(
            asset=DATACITE_RESOURCE_TYPE_GENERAL_ASSET, dataset=DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier
        )
        release >> [enrich, previous]
        checked >> [enrich, previous]
        # A skipped diff does not wait for enrich, so persist needs the direct edge.
        enrich >> persisted
        [enrich, previous] >> diff >> persisted >> publish(release)

    return enrich_dag()
