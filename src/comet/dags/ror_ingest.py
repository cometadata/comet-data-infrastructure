from __future__ import annotations

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.exceptions import AirflowException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
import pendulum

from comet.airflow import BaseDagParams
from comet.airflow.assets import ROR_RELEASE_ASSET
from comet.airflow.notifications import alert_kwargs, optional_slack_date, slack_notifier
from comet.airflow.utils import get_current_run_id
from comet.aws import run_prefix, s3_uri
from comet.constants import ROR_SOURCE
from comet.dags.tasks import persist_release, publish_release_asset
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease
from comet.ror.ror import download_ror, get_ror_release


class RorIngestParams(BaseDagParams):
    """Factory-specific parameters for the ROR ingest DAG.

    Attributes:
        bucket_name: Destination S3 bucket for the ROR snapshot.
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
        description="Ingest new ROR releases.",
        schedule="@daily",
        params={
            "release_date": Param(
                None,
                type=["null", "string"],
                format="date",
                title="ROR release date",
                description=(
                    "Ingest the ROR release published on this date (YYYY-MM-DD). "
                    "Empty = the newest release not yet ingested."
                ),
            ),
        },
        **params.dag_kwargs(),
        **alert_kwargs(params.deadline_minutes),
    )
    def ror_dag():
        @task
        def fetch_release() -> dict:
            release_date = get_current_context()["params"]["release_date"]
            if release_date:
                day = pendulum.parse(release_date, exact=True)
                release = get_ror_release(start_date=day, end_date=day)
                if release is None:
                    raise AirflowException(f"No ROR release was published on {release_date}")
                return release.to_dict()

            latest_release = dataset_releases.get_latest_release(dataset=ROR_SOURCE.identifier)
            # Look for releases published after the latest ingested one.
            start_date = pendulum.parse(latest_release.release_date, exact=True).add(days=1) if latest_release else None
            release = get_ror_release(start_date=start_date)

            if release is None:
                raise AirflowSkipException("No new ROR version available")

            return release.to_dict()

        @task
        def download(release: dict):
            release = DatasetRelease.from_dict(release)
            run_id = get_current_run_id()
            target_uri = s3_uri(params.bucket_name, run_prefix(dag_id, run_id))
            download_ror(
                target_uri=target_uri,
                download_url=release.download_url,
                file_name=release.file_name,
                file_hash=release.file_hash,
            )

        publish = publish_release_asset(
            asset=ROR_RELEASE_ASSET,
            dataset=ROR_SOURCE.identifier,
            on_success_callback=slack_notifier(
                ":large_green_circle:",
                "ROR release ingested",
                "{% set release = ti.xcom_pull(task_ids='fetch_release') %}"
                "*Release date:* {{ release.release_date }}\n"
                "*File:* {{ release.file_name }}\n"
                f"*Completed:* {optional_slack_date('ti.end_date')}",
            ),
        )

        release = fetch_release()
        persisted = persist_release(release, dataset=ROR_SOURCE.identifier, dag_id=dag_id)
        download(release) >> persisted >> publish(release)

    return ror_dag()
