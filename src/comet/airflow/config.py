"""Pydantic models for the DAG config YAML."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DagEntry(BaseModel):
    """A single DAG declaration in dags.yaml.

    Attributes:
        dag_id: Airflow DAG ID for this entry.
        factory: Dotted path to the factory function, e.g. ``"comet.dags.ror_ingest.create_ror_ingest_dag"``.
        enabled: If ``False``, the entry is skipped at load time.
        params: Per-factory params dict; validated against the factory's ``BaseDagParams`` subclass.
    """

    model_config = ConfigDict(extra="forbid")

    dag_id: str
    factory: str
    enabled: bool = True
    params: dict[str, Any] = Field(default_factory=dict)


class DagsConfig(BaseModel):
    """Top-level envelope for dags.yaml.

    Attributes:
        dags: List of DAG entries to load.
    """

    model_config = ConfigDict(extra="forbid")

    dags: list[DagEntry] = Field(default_factory=list)
