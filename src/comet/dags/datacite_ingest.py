from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
import pendulum

from comet.airflow import BaseDagParams
from comet.airflow.assets import DATACITE_RELEASE_ASSET
from comet.airflow.utils import (
    build_release_asset_metadata,
    get_airflow_connection,
    get_current_run_id,
)
from comet.aws import (
    BATCH_JOB_TAGS,
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    run_prefix,
    s3_uri,
)
from comet.constants import DATACITE_SOURCE
from comet.datacite.datacite import get_new_datacite_release, release_is_smaller, snapshot_stats
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease
from comet.utils import get_env

DOWNLOAD_VCPU = "4"
DOWNLOAD_MEMORY = "15360"


class DataCiteIngestParams(BaseDagParams):
    """Factory-specific parameters for the DataCite ingest DAG.

    Attributes:
        bucket_name: Destination S3 bucket for the DataCite snapshot.
        datacite_bucket_name: Source DataCite S3 bucket (per-run overridable).
        datacite_bucket_region: Region of the source DataCite bucket (per-run overridable).
        datacite_conn_name: Airflow connection id for DataCite credentials.
    """

    bucket_name: str
    datacite_bucket_name: str
    datacite_bucket_region: str
    datacite_conn_name: str = "datacite"


def create_datacite_ingest_dag(dag_id: str, params: DataCiteIngestParams) -> DAG:
    """Build a DAG that submits a single Batch job to ingest a new ROR release.

    Args:
        dag_id: Airflow DAG ID.
        params: Validated ROR ingest parameters.

    Returns:
        The constructed DAG.
    """

    @dag(
        dag_id=dag_id,
        schedule="@daily",
        params={
            "datacite_bucket_name": Param(
                params.datacite_bucket_name,
                type="string",
                title="DataCite source bucket",
                description="Source DataCite S3 bucket to copy the snapshot from.",
            ),
            "datacite_bucket_region": Param(
                params.datacite_bucket_region,
                type="string",
                title="DataCite source region",
                description="AWS region of the source DataCite bucket.",
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
    def datacite_dag():
        @task
        def fetch_release() -> dict:
            run_params = get_current_context()["params"]
            last_release_record = dataset_releases.get_latest_release(dataset=DATACITE_SOURCE.identifier)
            published_after = pendulum.parse(last_release_record.release_date) if last_release_record else None
            conn = get_airflow_connection(params.datacite_conn_name)
            release = get_new_datacite_release(
                datacite_bucket_name=run_params["datacite_bucket_name"],
                datacite_bucket_region=run_params["datacite_bucket_region"],
                published_after=published_after,
                account_id=conn.login,
                password=conn.password,
            )
            if release is None:
                raise AirflowSkipException("No new DataCite version available")

            last_release = last_release_record.to_dataset_release() if last_release_record else None
            if last_release is not None and release_is_smaller(release, last_release):
                new_count, new_bytes = snapshot_stats(release)
                last_count, last_bytes = snapshot_stats(last_release)
                raise AirflowException(
                    f"DataCite snapshot looks incomplete: {new_count} files / {new_bytes} bytes < previous {last_count} / {last_bytes}"
                )

            return release.to_dict()

        download = BatchOperator(
            task_id="download",
            job_name="{{ batch_job_name(get_env(), 'download-datacite') }}",
            job_queue="{{ batch_job_queue_name(get_env(), 'download') }}",
            job_definition="{{ batch_job_definition_name(get_env(), 'download-datacite') }}",
            tags=BATCH_JOB_TAGS,
            container_overrides={
                "resourceRequirements": [
                    {"type": "VCPU", "value": DOWNLOAD_VCPU},
                    {"type": "MEMORY", "value": DOWNLOAD_MEMORY},
                ],
                "command": [
                    "comet",
                    "datacite",
                    "download",
                    "--target-uri",
                    s3_uri(params.bucket_name, run_prefix(dag_id, "{{ run_id }}")),
                    "--datacite-bucket-name",
                    "{{ params.datacite_bucket_name }}",
                    "--datacite-bucket-region",
                    "{{ params.datacite_bucket_region }}",
                    "--expected-file-count",
                    "{{ ti.xcom_pull(task_ids='fetch_release')['metadata']['file_count'] }}",
                    "--expected-total-bytes",
                    "{{ ti.xcom_pull(task_ids='fetch_release')['metadata']['total_size_bytes'] }}",
                ],
            },
            awslogs_enabled=True,
            deferrable=True,
        )

        @task
        def persist_discovered_release(release: dict):
            release = DatasetRelease.from_dict(release)
            run_id = get_current_run_id()
            dataset_releases.persist_discovered_release(
                dataset=DATACITE_SOURCE.identifier,
                release=release,
                run_id=run_id,
                source_prefix=run_prefix(dag_id, run_id),
            )

        @task(outlets=[DATACITE_RELEASE_ASSET])
        def publish_release_asset(release: dict):
            release = DatasetRelease.from_dict(release)
            yield build_release_asset_metadata(
                asset=DATACITE_RELEASE_ASSET,
                dataset=DATACITE_SOURCE.identifier,
                release_date=release.release_date,
            )

        release_task = fetch_release()
        release_task >> download >> persist_discovered_release(release_task) >> publish_release_asset(release_task)

    return datacite_dag()
