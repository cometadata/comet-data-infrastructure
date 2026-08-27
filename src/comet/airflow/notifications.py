"""Slack notifications for DAG failures, deadlines, and progress."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.slack.notifications.slack_webhook import (
    SlackWebhookNotifier,
    send_slack_webhook_notification,
)
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback
import pendulum

from comet.utils import get_env

if TYPE_CHECKING:
    from airflow.sdk import DAG
    import jinja2

SLACK_CONNECTION_ID = "slack_default"
SLACK_TIMEOUT_SECONDS = 10
ERROR_MAX_CHARS = 500


def slack_date(expression: str, *, parse: bool = False) -> str:
    """Format a Jinja datetime expression as a Slack date; parse ISO strings when requested."""
    seconds = f"{expression} | epoch" if parse else f"{expression}.timestamp() | int"
    return f"<!date^{{{{ {seconds} }}}}^{{date_short_pretty}} at {{time_secs}}|{{{{ {expression} }}}}>"


def optional_slack_date(expression: str, *, fallback: str = "not available") -> str:
    """Return a Slack date snippet that tolerates an absent or empty value."""
    return (
        f"{{% if {expression} is defined and {expression} %}}"
        f"{slack_date(expression)}"
        f"{{% else %}}{fallback}{{% endif %}}"
    )


def slack_notifier(icon: str, title: str, body: str) -> SlackWebhookNotifier:
    """Build a task callback using the shared Slack connection."""
    sections = [
        f"{icon} [{get_env()}] *{title}*",
        "`{{ dag.dag_id }}.{{ ti.task_id }}`",
    ]
    if body:
        sections.append(body.rstrip())
    sections.append("<{{ ti.log_url }}|View logs>")

    text = "\n\n".join(sections)
    return send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_CONNECTION_ID,
        text=text,
        blocks=[
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
            {"type": "divider"},
        ],
        timeout=SLACK_TIMEOUT_SECONDS,
    )


FAILURE_BODY = (
    "*Error:* {{ exception"
    " | default('No exception was reported; Airflow marked the task failed.', true)"
    f" | string | truncate({ERROR_MAX_CHARS}) }}}}\n"
    f"*Started:* {optional_slack_date('ti.start_date')}\n"
    f"*Failed:* {optional_slack_date('ti.end_date')}\n"
    "*Run:* {{ run_id }} ({{ dag_run.run_type.value | default(dag_run.run_type) }}"
    "{{ ', ' ~ dag_run.triggering_user_name if dag_run.triggering_user_name }})"
)


def epoch(value: str) -> int:
    """Return the Unix timestamp for an ISO 8601 string; a naive value is read as UTC."""
    return pendulum.parse(value).int_timestamp


class DeadlineSlackNotifier(SlackWebhookNotifier):
    """Add deadline-specific template values to a Slack notifier.

    Deadline callbacks receive JSON string dates and no task instance, so they cannot use
    ``ti.log_url``.
    """

    def get_template_env(self, dag: DAG | None = None) -> jinja2.Environment:
        """Add the epoch filter and UI base URL."""
        env = super().get_template_env(dag)
        env.filters["epoch"] = epoch
        env.globals["base_url"] = conf.get("api", "base_url", fallback="http://localhost:8080").rstrip("/")
        return env


def build_deadline_alert(minutes: int) -> DeadlineAlert:
    """Build a queued-time deadline that sends a Slack message from a worker."""
    text = (
        f":red_circle: [{get_env()}] *Airflow DAG deadline exceeded*\n\n"
        "`{{ dag_run.dag_id }}`\n\n"
        "*Run:* {{ dag_run.dag_run_id }} ({{ dag_run.run_type.value | default(dag_run.run_type) }}"
        "{{ ', ' ~ dag_run.triggering_user_name if dag_run.triggering_user_name }})\n"
        "*State:* {{ dag_run.state }}\n"
        f"*Limit:* {minutes} minutes since queued\n"
        f"*Queued:* {slack_date('dag_run.queued_at', parse=True)}\n"
        f"*Started:* {{% if dag_run.start_date %}}{slack_date('dag_run.start_date', parse=True)}"
        "{% else %}not started{% endif %}\n\n"
        "<{{ base_url }}/dags/{{ dag_run.dag_id }}/runs/{{ dag_run.dag_run_id | urlencode }}|View run>"
    )
    return DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=minutes),
        callback=SyncCallback(
            DeadlineSlackNotifier,
            kwargs={
                "slack_webhook_conn_id": SLACK_CONNECTION_ID,
                "text": text,
                "blocks": [
                    {"type": "section", "text": {"type": "mrkdwn", "text": text}},
                    {"type": "divider"},
                ],
                "timeout": SLACK_TIMEOUT_SECONDS,
            },
        ),
    )


def alert_kwargs(deadline_minutes: int) -> dict[str, Any]:
    """Build DAG arguments for task-failure and queued-time deadline notifications."""
    return {
        "default_args": {
            "on_failure_callback": slack_notifier(":red_circle:", "Airflow DAG task failure", FAILURE_BODY)
        },
        "deadline": build_deadline_alert(deadline_minutes),
    }
