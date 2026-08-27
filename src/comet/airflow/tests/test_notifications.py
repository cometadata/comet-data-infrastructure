from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from airflow.sdk import DAG, DeadlineReference
from airflow.utils.types import DagRunType

from comet.airflow.notifications import DeadlineSlackNotifier, alert_kwargs, build_deadline_alert

STARTED = datetime(2026, 8, 30, 23, 20, tzinfo=UTC)
COMPLETED = datetime(2026, 8, 30, 23, 20, 42, tzinfo=UTC)
STARTED_TOKEN = "<!date^1788132000^{date_short_pretty} at {time_secs}|2026-08-30 23:20:00+00:00>"
COMPLETED_TOKEN = "<!date^1788132042^{date_short_pretty} at {time_secs}|2026-08-30 23:20:42+00:00>"
DEADLINE_QUEUED_TOKEN = "<!date^1788132000^{date_short_pretty} at {time_secs}|2026-08-30T23:20:00Z>"
DEADLINE_STARTED_TOKEN = "<!date^1788132037^{date_short_pretty} at {time_secs}|2026-08-30T23:20:37Z>"
LOG_URL = "http://localhost:8080/dags/a_dag/runs/r1/tasks/a_task?try_number=1"


@pytest.mark.parametrize(
    "context, body",
    [
        pytest.param(
            {
                "run_id": "manual__2026-08-30T23:19:38+00:00",
                "dag_run": SimpleNamespace(run_type=DagRunType.MANUAL, triggering_user_name="user"),
                "ti": SimpleNamespace(task_id="a_task", start_date=STARTED, end_date=COMPLETED, log_url=LOG_URL),
                "exception": RuntimeError("x" * 600),
            },
            "*Error:* " + "x" * 497 + "...\n"
            f"*Started:* {STARTED_TOKEN}\n"
            f"*Failed:* {COMPLETED_TOKEN}\n"
            "*Run:* manual__2026-08-30T23:19:38+00:00 (manual, user)",
            id="worker",
        ),
        pytest.param(
            {
                "run_id": "scheduled__2026-08-30T23:19:38+00:00",
                "dag_run": SimpleNamespace(run_type="scheduled", triggering_user_name=None),
                "ti": SimpleNamespace(task_id="a_task", log_url=LOG_URL),
            },
            "*Error:* No exception was reported; Airflow marked the task failed.\n"
            "*Started:* not available\n"
            "*Failed:* not available\n"
            "*Run:* scheduled__2026-08-30T23:19:38+00:00 (scheduled)",
            id="scheduler",
        ),
    ],
)
def test_failure_template_renders_for_worker_and_scheduler_contexts(context, body):
    notifier = alert_kwargs(90)["default_args"]["on_failure_callback"]

    notifier.render_template_fields({"dag": DAG(dag_id="a_dag"), **context})

    expected_text = (
        f":red_circle: [test] *Airflow DAG task failure*\n\n`a_dag.a_task`\n\n{body}\n\n<{LOG_URL}|View logs>"
    )
    assert notifier.slack_webhook_conn_id == "slack_default"
    assert notifier.text == expected_text
    assert notifier.blocks == [
        {"type": "section", "text": {"type": "mrkdwn", "text": expected_text}},
        {"type": "divider"},
    ]


@pytest.mark.parametrize(
    "start_date, started",
    [("2026-08-30T23:20:37Z", DEADLINE_STARTED_TOKEN), (None, "not started")],
    ids=["running", "queued"],
)
def test_deadline_template_renders_with_dag_run_response(start_date, started):
    alert = build_deadline_alert(90)
    notifier = DeadlineSlackNotifier(**alert.callback.kwargs)
    context = {
        "dag_run": {
            "dag_run_id": "asset__2026-08-30T02:14:11+00:00",
            "dag_id": "a_dag",
            "state": "running",
            "run_type": "asset_triggered",
            "triggering_user_name": None,
            "queued_at": "2026-08-30T23:20:00Z",
            "start_date": start_date,
        },
    }

    notifier.render_template_fields(context)

    expected_text = (
        ":red_circle: [test] *Airflow DAG deadline exceeded*\n\n"
        "`a_dag`\n\n"
        "*Run:* asset__2026-08-30T02:14:11+00:00 (asset_triggered)\n"
        "*State:* running\n"
        "*Limit:* 90 minutes since queued\n"
        f"*Queued:* {DEADLINE_QUEUED_TOKEN}\n"
        f"*Started:* {started}\n\n"
        "<http://localhost:8080/dags/a_dag/runs/asset__2026-08-30T02%3A14%3A11%2B00%3A00|View run>"
    )

    assert alert.reference is DeadlineReference.DAGRUN_QUEUED_AT
    assert notifier.text == expected_text
    assert notifier.blocks == [
        {"type": "section", "text": {"type": "mrkdwn", "text": expected_text}},
        {"type": "divider"},
    ]
