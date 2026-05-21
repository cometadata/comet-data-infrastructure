"""Resolve factory functions from dotted paths and register DAGs into globals."""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
import pkgutil
from typing import TYPE_CHECKING, get_type_hints

from airflow.sdk import get_parsing_context
import yaml

from comet.airflow.config import DagsConfig
from comet.airflow.factory import BaseDagParams

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

MIN_FACTORY_PARAMS = 2


class DagLoadError(ExceptionGroup):
    """ExceptionGroup of every dags.yaml entry that failed to load."""


def resolve_factory(dotted_path: str) -> tuple[Callable, type[BaseDagParams]]:
    """Resolve a dotted-path factory and infer its Params type from its signature.

    Args:
        dotted_path: Dotted path to the factory function, e.g. ``"module.attr"``
            or ``"module:attr"`` (both forms accepted by ``pkgutil.resolve_name``).

    Returns:
        Tuple of ``(factory function, BaseDagParams subclass)``.

    Raises:
        ImportError: Top-level module cannot be imported.
        AttributeError: Module imports but the attribute is missing.
        TypeError: Factory has the wrong signature, or its second-arg annotation
            is not a ``BaseDagParams`` subclass.
    """
    func = pkgutil.resolve_name(dotted_path)
    sig_params = list(inspect.signature(func).parameters)
    if len(sig_params) < MIN_FACTORY_PARAMS:
        raise TypeError(f"{dotted_path}: factory must accept (dag_id, params)")
    params_type = get_type_hints(func).get(sig_params[1])
    if not (isinstance(params_type, type) and issubclass(params_type, BaseDagParams)):
        raise TypeError(f"{dotted_path}: second parameter must be annotated with a BaseDagParams subclass")
    return func, params_type


def load_dags(globals_dict: dict) -> None:
    """Load DAGs declared in the sibling ``dags.yaml`` into ``globals_dict``.

    Missing ``dags.yaml`` is logged and skipped. Malformed YAML or envelope
    validation errors propagate so the operator sees a bad config. Per-entry
    errors (bad factory path, params validation, exception in the factory,
    dag_id collision) do not stop the loop — every entry is attempted and its
    exception collected — but once any entry fails the loader raises a single
    :class:`DagLoadError` (an ``ExceptionGroup``) at the end wrapping every
    failure, each sub-exception keeping its own traceback and a ``dag_id``/
    ``factory`` note. That surfaces the whole set as one dag-processor import
    error, rather than only the first exception. Honors Airflow's
    ``get_parsing_context()`` so a single-DAG parse only materializes the
    requested DAG.

    Args:
        globals_dict: The caller's ``globals()`` dict; each DAG is assigned
            under its ``dag_id``.
    """
    path = Path(globals_dict["__file__"]).parent / "dags.yaml"
    if not path.exists():
        logger.warning("no dags.yaml next to %s; skipping", path)
        return

    config = DagsConfig.model_validate(yaml.safe_load(path.read_text()) or {})
    current_dag_id = get_parsing_context().dag_id

    def ensure_absent(dag_id: str) -> None:
        if dag_id in globals_dict:
            raise ValueError(f"dag_id {dag_id!r} already present in globals (duplicate entry or shadowed import)")

    failures: list[Exception] = []
    failed_ids: list[str] = []
    attempted = 0
    for entry in (e for e in config.dags if e.enabled):
        if current_dag_id is not None and current_dag_id != entry.dag_id:
            continue
        attempted += 1
        try:
            ensure_absent(entry.dag_id)
            func, params_type = resolve_factory(entry.factory)
            globals_dict[entry.dag_id] = func(entry.dag_id, params_type(**entry.params))
        except Exception as exc:
            exc.add_note(f"dag_id={entry.dag_id} factory={entry.factory}")
            failures.append(exc)
            failed_ids.append(entry.dag_id)

    if failures:
        raise DagLoadError(
            f"{len(failures)} of {attempted} DAG entries failed to load: {', '.join(failed_ids)}",
            failures,
        )
