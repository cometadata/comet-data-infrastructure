from __future__ import annotations

from typing import Literal

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Asset, AssetAny, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowException
from pydantic import field_validator

from comet.airflow import BaseDagParams
from comet.airflow.notifications import alert_kwargs, optional_slack_date, slack_notifier
from comet.airflow.utils import resolve_release_record, skip_asset_fail_manual
from comet.aws import BATCH_JOB_TAGS, batch_job_definition_name, batch_job_name, batch_job_queue_name, s3_uri
from comet.constants import enrichments_for_source
from comet.utils import get_env

BATCH_ATTEMPT_TIMEOUT = 2 * 60 * 60
PUBLISH_VCPU = "8"
PUBLISH_MEMORY = "15360"


class PublishEnrichmentsParams(BaseDagParams):
    """Params for a publish enrichments DAG instance.

    Attributes:
        source: Source dataset whose enrichments this instance publishes.
        bucket_name: S3 bucket holding the enrichment run outputs.
        hf_bucket_name: Hugging Face bucket the releases are published to.
        hf_endpoint_url: Hugging Face S3-compatible endpoint URL.
        max_active_runs: Pinned to 1; the publish job assumes no concurrent copies or index uploads.
    """

    source: str
    bucket_name: str
    hf_bucket_name: str
    hf_endpoint_url: str
    max_active_runs: Literal[1] = 1

    @field_validator("source")
    @classmethod
    def source_has_enrichments(cls, value: str) -> str:
        """Require a source with at least one registered enrichment."""
        if not enrichments_for_source(value):
            raise ValueError(f"Source '{value}' has no enrichments to publish")
        return value


def create_publish_enrichments_dag(dag_id: str, params: PublishEnrichmentsParams) -> DAG:
    """Build a DAG that publishes aligned enrichment releases to Hugging Face.

    Args:
        dag_id: Airflow DAG ID.
        params: Validated publish parameters.

    Returns:
        The constructed DAG.
    """
    datasets = [enrichment.identifier for enrichment in enrichments_for_source(params.source)]

    @dag(
        dag_id=dag_id,
        description="Publish enrichment releases to Hugging Face.",
        schedule=AssetAny(*(Asset(dataset) for dataset in datasets)),
        params={
            "hf_bucket_name": Param(
                params.hf_bucket_name,
                type="string",
                title="Hugging Face bucket name",
                description="Hugging Face bucket the releases are published to.",
            ),
            "hf_endpoint_url": Param(
                params.hf_endpoint_url,
                type="string",
                title="Hugging Face endpoint URL",
                description="Hugging Face S3-compatible endpoint URL.",
            ),
            "release_date": Param(
                None,
                type=["null", "string"],
                format="date",
                title="Release date",
                description="Which snapshot (YYYY-MM-DD) to publish; "
                "empty uses each dataset's latest release and requires their dates to match.",
            ),
            "datasets": Param(
                datasets,
                type="array",
                uniqueItems=True,
                items={"type": "string", "enum": datasets},
                title="Datasets",
                description="Datasets to publish; defaults to all of them.",
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
    def publish_dag():
        @task
        def resolve_releases() -> dict:
            run_params = get_current_context()["params"]
            records = {}
            for dataset in run_params["datasets"]:
                try:
                    record = resolve_release_record(dataset=dataset, release_date=run_params["release_date"])
                except AirflowException as error:
                    skip_asset_fail_manual(f"Release for {dataset} is not ready", error)
                # Outside the try: a missing source_prefix is a data problem, never "not ready".
                if not record.source_prefix:
                    raise AirflowException(
                        f"Release record for {dataset}/{record.release_date} has no source_prefix; "
                        "re-run its enrich DAG to refresh the record"
                    )
                records[dataset] = record

            release_dates = {record.release_date for record in records.values()}
            if len(release_dates) != 1:
                skip_asset_fail_manual("Latest releases are not aligned: " + ", ".join(sorted(release_dates)))

            return {
                "release_date": release_dates.pop(),
                "source_uris": {
                    dataset: s3_uri(params.bucket_name, record.source_prefix) for dataset, record in records.items()
                },
            }

        resolved_xcom = "ti.xcom_pull(task_ids='resolve_releases')"
        publish = BatchOperator(
            task_id="publish",
            on_success_callback=slack_notifier(
                ":large_green_circle:",
                "Enrichments published to Hugging Face",
                "{% set resolved = " + resolved_xcom + " %}"
                "*Release date:* {{ resolved.release_date }}\n"
                "*Datasets:* {{ resolved.source_uris | sort | join(', ') }}\n"
                "*Bucket:* {{ params.hf_bucket_name }}\n"
                f"*Completed:* {optional_slack_date('ti.end_date')}",
            ),
            job_name="{{ batch_job_name(get_env(), 'publish') }}",
            job_queue="{{ batch_job_queue_name(get_env(), 'publish') }}",
            job_definition="{{ batch_job_definition_name(get_env(), 'publish') }}",
            tags=BATCH_JOB_TAGS,
            container_overrides={
                "resourceRequirements": [
                    {"type": "VCPU", "value": PUBLISH_VCPU},
                    {"type": "MEMORY", "value": PUBLISH_MEMORY},
                ],
                "command": [
                    "comet",
                    "publish",
                    "--source",
                    params.source,
                    "--release-date",
                    "{{ " + resolved_xcom + "['release_date'] }}",
                    "--source-uris",
                    "{{ " + resolved_xcom + "['source_uris'] | tojson }}",
                    "--hf-bucket",
                    "{{ params.hf_bucket_name }}",
                    "--hf-endpoint-url",
                    "{{ params.hf_endpoint_url }}",
                ],
            },
            submit_job_timeout=BATCH_ATTEMPT_TIMEOUT,
            deferrable=True,
        )

        resolve_releases() >> publish

    return publish_dag()
