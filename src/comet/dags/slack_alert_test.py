from __future__ import annotations

import time

from airflow import DAG  # noqa: TC002  # loader's get_type_hints() evaluates the `-> DAG` return at runtime
from airflow.sdk import dag, task

from comet.airflow import BaseDagParams  # noqa: TC001  # loader's get_type_hints() evaluates params at runtime
from comet.airflow.notifications import alert_kwargs, optional_slack_date, slack_notifier


def create_slack_alert_test_dag(dag_id: str, params: BaseDagParams) -> DAG:
    """Build a manual DAG for testing failure, deadline, and success notifications."""

    @dag(
        dag_id=dag_id,
        description="Test DAG failure, deadline, and success Slack messages.",
        schedule=None,
        is_paused_upon_creation=True,
        **params.dag_kwargs(),
        **alert_kwargs(params.deadline_minutes),
    )
    def slack_alert_test():
        @task
        def wait_then_fail():
            time.sleep(150)
            raise RuntimeError("Intentional failure for Slack alert test")

        @task(
            on_success_callback=slack_notifier(
                ":large_green_circle:",
                "Slack alert test succeeded",
                "*Result:* Intentional success for Slack alert test\n"
                f"*Completed:* {optional_slack_date('ti.end_date')}",
            )
        )
        def succeed():
            pass

        wait_then_fail()
        succeed()

    return slack_alert_test()
