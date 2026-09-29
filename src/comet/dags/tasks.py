"""Tasks for fetching, diffing, recording, and announcing dataset releases."""

from __future__ import annotations

from collections.abc import Callable  # noqa: TC003  # loader's get_type_hints() evaluates annotations at runtime

from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.sdk import Asset, XComArg, get_current_context, task
from airflow.sdk.bases.decorator import TaskDecorator  # noqa: TC002
from airflow.sdk.exceptions import AirflowException

from comet.airflow.assets import DATACITE_RELEASE_ASSET
from comet.airflow.utils import (
    build_release_asset_metadata,
    get_current_run_id,
    get_triggering_release_key_or_none,
    previous_release,
    resolve_release_record,
)
from comet.aws import BATCH_JOB_TAGS, run_prefix, s3_uri
from comet.constants import DATACITE_SOURCE, DIFF_DIR, FULL_DIR, ROR_SOURCE, Enrichment
import comet.dynamodb_store as dataset_releases
from comet.model.dataset_version_model import DatasetRelease

DIFF_VCPU = "4"
DIFF_MEMORY = "8192"


@task
def fetch_datacite_release() -> dict:
    """Resolve the DataCite release from the triggering asset or ``datacite_release_date`` param.

    Returns:
        The release as a ``DatasetRelease`` dict.
    """
    key = get_triggering_release_key_or_none(DATACITE_RELEASE_ASSET)
    datacite_release_date = get_current_context()["params"]["datacite_release_date"]
    record = resolve_release_record(
        dataset=DATACITE_SOURCE.identifier, release_date=datacite_release_date, triggering_key=key
    )

    return record.to_dataset_release().to_dict()


@task
def fetch_ror_release(bucket_name: str) -> dict:
    """Resolve the ROR release selected by the ``ror_dag_id`` and ``ror_release_date`` params.

    Args:
        bucket_name: S3 bucket holding the ROR snapshot.

    Returns:
        The release date and the S3 URI of the ROR snapshot file.
    """
    run_params = get_current_context()["params"]
    record = resolve_release_record(dataset=ROR_SOURCE.identifier, release_date=run_params["ror_release_date"])

    return {
        "release_date": record.release_date,
        "uri": s3_uri(bucket_name, run_params["ror_dag_id"], record.run_id, record.file_name),
    }


@task
def check_enrichment_unpublished(release: dict, *, dataset: str) -> None:
    """Fail before the Batch job is submitted when this release date is already published.

    The ``replace_published`` param allows the rerun; ``persist_enrichment_release`` then
    un-publishes the record so the publish DAG replaces the release.
    """
    if get_current_context()["params"]["replace_published"]:
        return
    release_date = DatasetRelease.from_dict(release).release_date.isoformat()
    record = dataset_releases.get_release(dataset=dataset, release_date=release_date)
    if record is not None and record.published_at:
        raise AirflowException(f"Release {dataset}/{release_date} is already published and cannot be rerun")


@task
def resolve_previous_release(release: dict, dataset: str) -> dict:
    """Find the previous usable published release to diff against, skipping when there is none.

    The dict return type makes Airflow push each key as its own XCom, which
    :func:`diff_release` relies on to render the prefix.

    Args:
        release: The current release as a ``DatasetRelease`` dict.
        dataset: The enrichment dataset identifier, e.g. "datacite-funders".

    Returns:
        A dict with the previous release's ``full_source_prefix`` and ``release_date``.
    """
    release_date = DatasetRelease.from_dict(release).release_date.isoformat()
    return previous_release(dataset=dataset, release_date=release_date)


def diff_release(
    *,
    bucket_name: str,
    run_uri: str,
    enrichment: Enrichment,
    previous: XComArg,
    attempt_timeout: int,
    vcpu: str = DIFF_VCPU,
    memory: str = DIFF_MEMORY,
) -> BatchOperator:
    """Build the Batch task that diffs this run's full release against the previous one.

    Args:
        bucket_name: S3 bucket holding the releases.
        run_uri: S3 run prefix containing the ``full/`` input and ``diff/`` output.
        enrichment: Enrichment method used to name the Batch job and queue.
        previous: XCom from :func:`resolve_previous_release`, containing ``full_source_prefix``.
        attempt_timeout: Seconds a single Batch attempt may run.
        vcpu: vCPUs for the Batch container.
        memory: Memory in MiB for the Batch container.
    """
    return BatchOperator(
        task_id="diff",
        job_name=f"{{{{ batch_job_name(get_env(), 'diff-{enrichment.method}') }}}}",
        job_queue=f"{{{{ batch_job_queue_name(get_env(), 'enrich-{enrichment.method}') }}}}",
        job_definition="{{ batch_job_definition_name(get_env(), 'enrich') }}",
        tags=BATCH_JOB_TAGS,
        container_overrides={
            "resourceRequirements": [
                {"type": "VCPU", "value": vcpu},
                {"type": "MEMORY", "value": memory},
            ],
            "command": [
                "comet",
                "diff",
                "--old-uri",
                s3_uri(bucket_name, str(previous["full_source_prefix"])),
                "--new-uri",
                run_uri + FULL_DIR,
                "--output-uri",
                run_uri + DIFF_DIR,
            ],
        },
        submit_job_timeout=attempt_timeout,
        awslogs_enabled=True,
        deferrable=True,
    )


@task
def persist_release(release: dict, *, dataset: str, dag_id: str):
    """Record an ingested release and the run prefix it was written under.

    Args:
        release: The release as a ``DatasetRelease`` dict.
        dataset: The dataset identifier the record is keyed by.
        dag_id: The DAG whose run prefix holds the output.
    """
    run_id = get_current_run_id()
    dataset_releases.persist_discovered_release(
        dataset=dataset,
        release=DatasetRelease.from_dict(release),
        run_id=run_id,
        source_prefix=run_prefix(dag_id, run_id),
    )


@task(task_id="persist_release", trigger_rule="none_failed")
def persist_enrichment_release(release: dict, *, dataset: str, dag_id: str, previous: dict | None):
    """Record an enrichment release, its full and diff directories, and the diff's baseline.

    Args:
        release: The release as a ``DatasetRelease`` dict.
        dataset: The enrichment dataset identifier the record is keyed by.
        dag_id: The DAG whose run prefix holds the output.
        previous: XCom of :func:`resolve_previous_release`; None when the diff was skipped.
    """
    run_id = get_current_run_id()
    source_prefix = run_prefix(dag_id, run_id)
    dataset_releases.persist_discovered_release(
        dataset=dataset,
        release=DatasetRelease.from_dict(release),
        run_id=run_id,
        source_prefix=source_prefix,
        full_source_prefix=source_prefix + FULL_DIR,
        diff_source_prefix=source_prefix + DIFF_DIR if previous else None,
        diff_base_release_date=previous["release_date"] if previous else None,
        replace_published=get_current_context()["params"]["replace_published"],
    )


def publish_release_asset(*, asset: Asset, dataset: str, on_success_callback: Callable | None = None) -> TaskDecorator:
    """Build the task that publishes the release on its asset.

    Outlets are fixed when the task is defined, so this returns a task rather than being one.

    Args:
        asset: The asset to publish on.
        dataset: The dataset identifier stored in the asset event.
        on_success_callback: Optional success notifier for the task.

    Returns:
        A task taking the release as a ``DatasetRelease`` dict.
    """

    @task(task_id="publish_release_asset", outlets=[asset], on_success_callback=on_success_callback)
    def publish(release: dict):
        release = DatasetRelease.from_dict(release)
        yield build_release_asset_metadata(asset=asset, dataset=dataset, release_date=release.release_date)

    return publish
