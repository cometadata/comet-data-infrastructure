"""Base params model for config-driven DAG factory functions."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BaseDagParams(BaseModel):
    """Common DAG parameters shared by every factory's Params subclass.

    The schedule is intentionally NOT a parameter — each DAG hardcodes its own schedule (a cron
    preset or an asset trigger) at the ``@dag`` call, since the schedule is structural to the DAG,
    not per-deployment config.

    Attributes:
        start_date: First date the DAG is eligible to run.
        end_date: Last date the DAG is eligible to run; ``None`` for indefinite.
        catchup: Whether to backfill from ``start_date`` when the scheduler first sees the DAG.
        tags: Tags shown in the Airflow UI.
        max_active_runs: Maximum concurrent DAG runs; ``None`` for Airflow's default.
        deadline_minutes: Minutes from queueing before the DAG sends a deadline alert.
    """

    model_config = ConfigDict(extra="forbid")

    start_date: datetime
    end_date: datetime | None = None
    catchup: bool = False
    tags: list[str] = Field(default_factory=list)
    max_active_runs: int | None = None
    deadline_minutes: int = Field(default=90, gt=0)

    def dag_kwargs(self) -> dict[str, Any]:
        """Build the common kwargs to forward into the airflow.DAG constructor (excluding schedule).

        ``schedule`` is set explicitly by each factory (a cron preset or an asset list), so it is
        deliberately not included here.

        Returns:
            Dict of common DAG constructor kwargs, ready to spread with ``**``.
        """
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "catchup": self.catchup,
            "tags": list(self.tags),
            "max_active_runs": self.max_active_runs,
        }
