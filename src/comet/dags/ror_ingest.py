from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
import pendulum

from comet.airflow import BaseDagParams
from comet.airflow.assets import ROR_RELEASE_ASSET
from comet.airflow.utils import build_release_asset_metadata, get_current_run_id
from comet.aws import s3_uri
from comet.constants import ROR_DATASET_NAME
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease
from comet.ror.ror import download_ror, get_new_ror_release


class RorIngestParams(BaseDagParams):
    """Factory-specific parameters for the ROR ingest DAG.

    Attributes:
        bucket_name: Destination S3 bucket for the ROR snapshot (per-run overridable).
    """

    bucket_name: str


def create_ror_ingest_dag(dag_id: str, params: RorIngestParams) -> DAG:
    """Build a DAG that ingests a new ROR release.

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
            "bucket_name": Param(
                params.bucket_name,
                type="string",
                title="S3 bucket",
                description=(
                    "The Comet S3 data bucket that holds ingested datasets and enrichment outputs. "
                    "Here it's the destination for the ROR snapshot."
                ),
            )
        },
        **params.dag_kwargs(),
    )
    def ror_dag():
        @task
        def fetch_release() -> dict:
            latest_release = dataset_releases.get_latest_release(dataset=ROR_DATASET_NAME)
            published_after = pendulum.parse(latest_release.release_date) if latest_release else None
            release = get_new_ror_release(published_after=published_after)

            if release is None:
                raise AirflowSkipException("No new ROR version available")

            return release.to_dict()

        @task
        def download(release: dict):
            release = DatasetRelease.from_dict(release)
            run_id = get_current_run_id()
            # Read at runtime so a manual run can override it.
            bucket_name = get_current_context()["params"]["bucket_name"]
            # s3://{bucket}/{dag_id}/{run_id}/
            target_uri = s3_uri(bucket_name, dag_id, run_id) + "/"
            download_ror(
                target_uri=target_uri,
                download_url=release.download_url,
                file_name=release.file_name,
                file_hash=release.file_hash,
            )

        @task
        def persist_discovered_release(release: dict):
            release = DatasetRelease.from_dict(release)
            run_id = get_current_run_id()
            dataset_releases.persist_discovered_release(
                dataset=ROR_DATASET_NAME,
                release=release,
                run_id=run_id,
            )

        @task(outlets=[ROR_RELEASE_ASSET])
        def publish_release_asset(release: dict):
            release = DatasetRelease.from_dict(release)
            yield build_release_asset_metadata(
                asset=ROR_RELEASE_ASSET, dataset=ROR_DATASET_NAME, release_date=release.release_date
            )

        release_task = fetch_release()
        download(release_task) >> persist_discovered_release(release_task) >> publish_release_asset(release_task)

    return ror_dag()
