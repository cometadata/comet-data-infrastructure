from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging
from typing import Literal

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.sdk import Param, dag, get_current_context, task
import boto3
from pydantic import field_validator

from comet import pruning
from comet.airflow import BaseDagParams
from comet.airflow.notifications import alert_kwargs, optional_slack_date, slack_notifier
from comet.aws import delete_s3_prefix, first_object_timestamp, list_run_prefixes
from comet.dynamodb_store import list_all_releases, mark_release_pruned

log = logging.getLogger(__name__)


class PruneReleasesParams(BaseDagParams):
    """Configuration for release pruning.

    ``max_active_runs`` is fixed at 1 because concurrent runs can target the
    same prefixes.
    """

    bucket_name: str
    producer_dag_ids: list[str]
    max_active_runs: Literal[1] = 1

    @field_validator("producer_dag_ids")
    @classmethod
    def validate_producer_dag_ids(cls, dag_ids: list[str]) -> list[str]:
        """Require plain DAG IDs so scans remain within configured producer prefixes."""
        if not dag_ids:
            raise ValueError("producer_dag_ids must not be empty")
        if any(not dag_id.strip() or "/" in dag_id for dag_id in dag_ids):
            raise ValueError("producer_dag_ids must contain non-empty DAG IDs without slashes")
        return dag_ids


def create_prune_releases_dag(dag_id: str, params: PruneReleasesParams) -> DAG:
    """Build the monthly S3 run-retention DAG.

    Tracked prefixes follow release retention rules. Untracked prefixes become
    eligible after the configured grace period.
    """

    @dag(
        dag_id=dag_id,
        description="Prune expired and orphaned release data from S3.",
        schedule="0 0 15 * *",
        params={
            "orphan_grace_days": Param(
                30,
                type="integer",
                minimum=1,
                title="Orphan grace period",
                description="Minimum age in days before an untracked run prefix may be deleted.",
            ),
            "dry_run": Param(
                False,
                type="boolean",
                title="Dry run",
                description="Log what would be deleted without deleting anything.",
            ),
        },
        **params.dag_kwargs(),
        **alert_kwargs(params.deadline_minutes),
    )
    def prune_dag():
        @task(
            on_success_callback=slack_notifier(
                ":large_green_circle:",
                "{{ 'Release prune dry run' if params.dry_run else 'Releases pruned' }}",
                "{% set summary = ti.xcom_pull() %}"
                "{% if params.dry_run %}"
                "*Prefixes that would be deleted:* {{ summary.prefixes }}\n"
                "*Records that would be marked:* {{ summary.records }}\n"
                "{% else %}"
                "*Prefixes deleted:* {{ summary.prefixes }}\n"
                "*Records marked:* {{ summary.records }}\n"
                "{% endif %}"
                "*Dry run:* {{ 'yes' if params.dry_run else 'no' }}\n"
                f"*Completed:* {optional_slack_date('ti.end_date')}",
            )
        )
        def prune() -> dict:
            run_params = get_current_context()["params"]

            s3_client = boto3.client("s3")
            # Load every release record so prefixes belonging to unknown or legacy
            # datasets are not mistaken for orphaned runs.
            records = list_all_releases()
            run_prefixes = list_run_prefixes(
                params.bucket_name,
                params.producer_dag_ids,
                s3_client=s3_client,
            )
            run_timestamps = {
                prefix: first_object_timestamp(params.bucket_name, prefix, s3_client=s3_client)
                for prefix in run_prefixes
            }
            candidates = pruning.select_prune_candidates(
                run_timestamps,
                records,
                orphan_cutoff=datetime.now(UTC) - timedelta(days=run_params["orphan_grace_days"]),
            )
            for candidate in candidates:
                log.info(f"Pruning {candidate.prefix}: {candidate.reason}")
                if not run_params["dry_run"]:
                    for record in candidate.records:
                        mark_release_pruned(
                            dataset=record.dataset,
                            release_date=record.release_date,
                            expected_source_prefix=candidate.prefix,
                        )
                delete_s3_prefix(
                    params.bucket_name,
                    candidate.prefix,
                    s3_client=s3_client,
                    dry_run=run_params["dry_run"],
                )
            return {
                "prefixes": len(candidates),
                "records": sum(len(candidate.records) for candidate in candidates),
            }

        prune()

    return prune_dag()
