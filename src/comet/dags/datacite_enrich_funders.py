from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Param, dag, get_current_context, task

from comet.airflow.assets import DATACITE_FUNDERS_ASSET, DATACITE_RELEASE_ASSET
from comet.airflow.utils import (
    build_release_asset_metadata,
    get_current_run_id,
    get_triggering_release_key_or_none,
    resolve_release_record,
)
from comet.aws import (
    BATCH_JOB_TAGS,
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    run_prefix,
    s3_uri,
)
from comet.constants import DATACITE_FUNDERS_ENRICHMENT, DATACITE_SOURCE, ROR_SOURCE
from comet.dags.datacite_enrich_params import DataCiteEnrichFundersParams, enrich_trigger_params
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease
from comet.utils import get_env

OPENSEARCH_VCPU = "1"
OPENSEARCH_MEMORY = "4096"
OPENSEARCH_JAVA_OPTS = "-Xms2g -Xmx2g"
MARPLE_VCPU = "10"
MARPLE_MEMORY = "16384"
MARPLE_WORKERS = "10"
MAIN_VCPU = "4"
MAIN_MEMORY = "8192"
WRITER_LANES = "4"

BATCH_ATTEMPT_TIMEOUT = 3 * 60 * 60


def create_datacite_enrich_funders_dag(dag_id: str, params: DataCiteEnrichFundersParams) -> DAG:
    """Build a DAG that matches DataCite funders to ROR IDs, triggered by a new release.

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
            "ror_dag_id": Param(
                params.ror_dag_id,
                type="string",
                title="ROR ingest DAG ID",
                description="ROR ingest DAG whose snapshot seeds Marple and reconciles matches.",
            ),
            "ror_release_date": Param(
                params.ror_release_date,
                type=["null", "string"],
                format="date",
                title="ROR release date",
                description="Which ROR release to use (YYYY-MM-DD). Empty = latest ROR release.",
            ),
            "provenance_path": Param(
                "enrichment-configs/funders-provenance.yaml",
                type="string",
                title="Provenance path",
                description="S3 path (within the bucket) of the funders provenance YAML.",
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
            record = resolve_release_record(
                dataset=DATACITE_SOURCE.identifier, release_date=release_date, triggering_key=key
            )

            return record.to_dataset_release().to_dict()

        @task
        def fetch_ror_release() -> dict:
            run_params = get_current_context()["params"]
            record = resolve_release_record(dataset=ROR_SOURCE.identifier, release_date=run_params["ror_release_date"])

            return {
                "release_date": record.release_date,
                "uri": s3_uri(params.bucket_name, run_params["ror_dag_id"], record.run_id, record.file_name),
            }

        ror_data_uri = "{{ ti.xcom_pull(task_ids='fetch_ror_release')['uri'] }}"
        datacite_input_uri = s3_uri(
            params.bucket_name,
            run_prefix(
                "{{ params.datacite_dag_id }}", "{{ ti.xcom_pull(task_ids='fetch_datacite_release')['run_id'] }}"
            ),
        )

        # Multi-container job: overrides go through ecs_properties_override.
        enrich = BatchOperator(
            task_id="enrich",
            job_name="{{ batch_job_name(get_env(), 'enrich-funders') }}",
            job_queue="{{ batch_job_queue_name(get_env(), 'enrich-funders') }}",
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
                                    "funders",
                                    "--input-uri",
                                    datacite_input_uri,
                                    "--output-uri",
                                    s3_uri(params.bucket_name, run_prefix(dag_id, "{{ run_id }}")),
                                    "--source-release-date",
                                    "datacite={{ ti.xcom_pull(task_ids='fetch_datacite_release')['release_date'] }}",
                                    "--source-release-date",
                                    "ror={{ ti.xcom_pull(task_ids='fetch_ror_release')['release_date'] }}",
                                    "--ror-data-uri",
                                    ror_data_uri,
                                    "--provenance-uri",
                                    s3_uri(params.bucket_name, "{{ params.provenance_path }}"),
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

        @task
        def persist_release(release: dict):
            run_id = get_current_run_id()
            dataset_releases.persist_discovered_release(
                dataset=DATACITE_FUNDERS_ENRICHMENT.identifier,
                release=DatasetRelease.from_dict(release),
                run_id=run_id,
                source_prefix=run_prefix(dag_id, run_id),
            )

        @task(outlets=[DATACITE_FUNDERS_ASSET])
        def publish_release_asset(release: dict):
            release = DatasetRelease.from_dict(release)
            yield build_release_asset_metadata(
                asset=DATACITE_FUNDERS_ASSET,
                dataset=DATACITE_FUNDERS_ENRICHMENT.identifier,
                release_date=release.release_date,
            )

        release_task = fetch_datacite_release()
        ror_task = fetch_ror_release()
        [release_task, ror_task] >> enrich >> persist_release(release_task) >> publish_release_asset(release_task)

    return enrich_dag()
