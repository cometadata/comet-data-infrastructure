from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Param, dag

from comet.airflow.assets import DATACITE_AFFILIATIONS_ASSET, DATACITE_RELEASE_ASSET
from comet.airflow.notifications import alert_kwargs
from comet.aws import (
    BATCH_JOB_TAGS,
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    run_prefix,
    s3_uri,
)
from comet.constants import DATACITE_AFFILIATIONS_ENRICHMENT, FULL_DIR
from comet.dags.datacite_enrich_params import DataCiteEnrichAffiliationsParams, enrich_trigger_params
from comet.dags.tasks import (
    check_enrichment_unpublished,
    diff_release,
    fetch_datacite_release,
    fetch_ror_release,
    persist_enrichment_release,
    publish_release_asset,
    resolve_previous_release,
)
from comet.utils import get_env

OPENSEARCH_VCPU = "10"
OPENSEARCH_MEMORY = "6144"
OPENSEARCH_JAVA_OPTS = "-Xms3g -Xmx3g"
MARPLE_VCPU = "10"
MARPLE_MEMORY = "27648"
MARPLE_WORKERS = "10"
MAIN_VCPU = "10"
MAIN_MEMORY = "27648"
WRITER_LANES = "32"

BATCH_ATTEMPT_TIMEOUT = 4 * 60 * 60


def create_datacite_enrich_affiliations_dag(dag_id: str, params: DataCiteEnrichAffiliationsParams) -> DAG:
    """Build a DAG that matches DataCite affiliations to ROR IDs, triggered by a new release.

    Args:
        dag_id: Airflow DAG ID.
        params: Validated enrichment parameters.

    Returns:
        The constructed DAG.
    """

    @dag(
        dag_id=dag_id,
        description="Match DataCite affiliations to ROR IDs.",
        schedule=[DATACITE_RELEASE_ASSET],
        params={
            **enrich_trigger_params(params),
            "ror_dag_id": Param(
                params.ror_dag_id,
                type="string",
                title="ROR ingest DAG ID",
                description="ROR ingest DAG whose snapshot seeds Marple.",
            ),
            "ror_release_date": Param(
                params.ror_release_date,
                type=["null", "string"],
                format="date",
                title="ROR release date",
                description="Which ROR release to use (YYYY-MM-DD). Empty = latest ROR release.",
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
        ror_data_uri = "{{ ti.xcom_pull(task_ids='fetch_ror_release')['uri'] }}"
        datacite_input_uri = s3_uri(
            params.bucket_name,
            run_prefix(
                "{{ params.datacite_dag_id }}", "{{ ti.xcom_pull(task_ids='fetch_datacite_release')['run_id'] }}"
            ),
        )
        run_uri = s3_uri(params.bucket_name, run_prefix(dag_id, "{{ run_id }}"))

        # Multi-container job: overrides go through ecs_properties_override.
        enrich = BatchOperator(
            task_id="enrich",
            job_name="{{ batch_job_name(get_env(), 'enrich-affiliations') }}",
            job_queue="{{ batch_job_queue_name(get_env(), 'enrich-affiliations') }}",
            job_definition="{{ batch_job_definition_name(get_env(), 'enrich-with-ror') }}",
            tags=BATCH_JOB_TAGS,
            ecs_properties_override={
                "taskProperties": [
                    {
                        "containers": [
                            {
                                "name": "opensearch",
                                "resourceRequirements": [
                                    {"type": "VCPU", "value": OPENSEARCH_VCPU},
                                    {"type": "MEMORY", "value": OPENSEARCH_MEMORY},
                                ],
                                "environment": [{"name": "OPENSEARCH_JAVA_OPTS", "value": OPENSEARCH_JAVA_OPTS}],
                            },
                            {
                                "name": "marple",
                                "resourceRequirements": [
                                    {"type": "VCPU", "value": MARPLE_VCPU},
                                    {"type": "MEMORY", "value": MARPLE_MEMORY},
                                ],
                                "environment": [
                                    {"name": "ROR_S3_URI", "value": ror_data_uri},
                                    {"name": "MARPLE_WORKERS", "value": MARPLE_WORKERS},
                                ],
                            },
                            {
                                "name": "main",
                                "resourceRequirements": [
                                    {"type": "VCPU", "value": MAIN_VCPU},
                                    {"type": "MEMORY", "value": MAIN_MEMORY},
                                ],
                                "command": [
                                    "comet",
                                    "datacite",
                                    "enrich",
                                    "affiliations",
                                    "--input-uri",
                                    datacite_input_uri,
                                    "--output-uri",
                                    run_uri + FULL_DIR,
                                    "--source-release-date",
                                    "datacite={{ ti.xcom_pull(task_ids='fetch_datacite_release')['release_date'] }}",
                                    "--source-release-date",
                                    "ror={{ ti.xcom_pull(task_ids='fetch_ror_release')['release_date'] }}",
                                    "--source-id",
                                    "{{ params.source_id }}",
                                    "--output-writer-lanes",
                                    WRITER_LANES,
                                ],
                            },
                        ]
                    }
                ]
            },
            submit_job_timeout=BATCH_ATTEMPT_TIMEOUT,
            deferrable=True,
        )

        release = fetch_datacite_release()
        checked = check_enrichment_unpublished(release, dataset=DATACITE_AFFILIATIONS_ENRICHMENT.identifier)
        ror = fetch_ror_release(bucket_name=params.bucket_name)
        previous = resolve_previous_release(release, dataset=DATACITE_AFFILIATIONS_ENRICHMENT.identifier)
        diff = diff_release(
            bucket_name=params.bucket_name,
            run_uri=run_uri,
            enrichment=DATACITE_AFFILIATIONS_ENRICHMENT,
            previous=previous,
            attempt_timeout=BATCH_ATTEMPT_TIMEOUT,
        )
        persisted = persist_enrichment_release(
            release,
            dataset=DATACITE_AFFILIATIONS_ENRICHMENT.identifier,
            dag_id=dag_id,
            previous=previous,
        )
        publish = publish_release_asset(
            asset=DATACITE_AFFILIATIONS_ASSET, dataset=DATACITE_AFFILIATIONS_ENRICHMENT.identifier
        )
        release >> [enrich, previous]
        checked >> [enrich, previous]
        # A skipped diff does not wait for enrich, so persist needs the direct edge.
        enrich >> persisted
        ror >> enrich
        [enrich, previous] >> diff >> persisted >> publish(release)

    return enrich_dag()
