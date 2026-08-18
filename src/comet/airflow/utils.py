"""Shared utilities for comet Airflow DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sdk import Asset, BaseHook, Connection, Metadata, get_current_context

import comet.dynamodb_store as dataset_releases

if TYPE_CHECKING:
    import datetime

    from comet.dynamodb_store import DatasetReleaseRecord


def get_current_run_id() -> str:
    """Return the run_id of the currently executing task instance."""
    return get_current_context()["run_id"]


def get_airflow_connection(
    conn_id: str,
    *,
    require_host: bool = False,
    require_login: bool = True,
    require_password: bool = True,
    require_schema: bool = False,
    require_port: bool = False,
) -> Connection:
    """Get and validate an Airflow connection.

    Raises an AirflowException if the connection does not exist or if any required
    connection fields are missing.

    Args:
        conn_id: The connection to look up.
        require_host: Require a host.
        require_login: Require a login.
        require_password: Require a password.
        require_schema: Require a schema.
        require_port: Require a port.

    Returns:
        The connection.

    Raises:
        AirflowException: The connection is missing, or a required field is blank.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as e:
        raise AirflowException(f"Missing Airflow connection: '{conn_id}'") from e

    required_fields = {
        "host": require_host and not conn.host,
        "login": require_login and not conn.login,
        "password": require_password and not conn.password,
        "schema": require_schema and not conn.schema,
        "port": require_port and not conn.port,
    }
    missing = [field for field, is_missing in required_fields.items() if is_missing]

    if missing:
        raise AirflowException(
            f"Airflow connection '{conn_id}' is invalid. Missing required field(s): {', '.join(missing)}"
        )

    return conn


def is_asset_triggered() -> bool:
    """Return True when the current run was triggered by at least one asset event."""
    return bool(get_current_context()["triggering_asset_events"])


def skip_asset_fail_manual(message: str, error: AirflowException | None = None) -> NoReturn:
    """Skip an asset-triggered run; fail a manual one.

    Args:
        message: The skip or failure message.
        error: An underlying error; a manual run re-raises it instead of ``message``.
    """
    if is_asset_triggered():
        raise AirflowSkipException(message) from error
    raise error or AirflowException(message)


def get_triggering_release_key_or_none(asset: Asset) -> tuple[str, str] | None:
    """Return the ``(dataset, release_date)`` key from the triggering asset event, or ``None``.

    Reads the metadata that the producing DAG attached via :func:`build_release_asset_metadata`.
    Returns ``None`` when the run was not triggered by an event for ``asset`` (e.g. a manual run),
    so callers can fall back. Still raises if there *is* a triggering event but it's missing the
    expected metadata — that's a real bug, not a manual run.

    Args:
        asset: The inlet asset whose triggering event to read.

    Returns:
        The ``(dataset, release_date)`` primary key, or ``None`` if not asset-triggered.

    Raises:
        AirflowException: If a triggering event exists but is missing the expected metadata keys.
    """
    asset_events = get_current_context()["triggering_asset_events"]
    # Fall back to name/uri across SDK versions.
    events = None
    for key in (asset, asset.name, asset.uri):
        try:
            events = asset_events[key]
        except (KeyError, TypeError):
            continue
        if events:
            break

    if not events:
        return None

    extra = events[-1].extra or {}
    try:
        return extra["dataset"], extra["release_date"]
    except KeyError as e:
        raise AirflowException(f"Triggering asset event for '{asset.uri}' is missing release metadata: {extra}") from e


def get_triggering_release_key(asset: Asset) -> tuple[str, str]:
    """Return the ``(dataset, release_date)`` key from the asset event that triggered this run.

    Args:
        asset: The inlet asset whose triggering event to read.

    Returns:
        The ``(dataset, release_date)`` primary key.

    Raises:
        AirflowException: If the run was not triggered by an event for ``asset``, or the event
            is missing the expected metadata keys.
    """
    key = get_triggering_release_key_or_none(asset)
    if key is None:
        raise AirflowException(f"No triggering asset events found for asset '{asset.uri}'")
    return key


def resolve_release_record(
    *, dataset: str, release_date: str | None, triggering_key: tuple[str, str] | None = None
) -> DatasetReleaseRecord:
    """Resolve the release record to process for a run.

    A ``triggering_key`` pins to that exact release. Otherwise an explicit ``release_date``
    pins by date, and no date at all resolves the latest release for ``dataset``.

    Args:
        dataset: The dataset identifier.
        release_date: The explicit release date (e.g. from ``params``); empty resolves the
            latest release.
        triggering_key: The triggering asset event's ``(dataset, release_date)`` key, if any.

    Returns:
        The resolved ``DatasetReleaseRecord``.

    Raises:
        AirflowException: If no matching release record exists.
    """
    if triggering_key is not None:
        dataset, release_date = triggering_key
        record = dataset_releases.get_release(dataset=dataset, release_date=release_date)
    elif release_date:
        record = dataset_releases.get_release(dataset=dataset, release_date=release_date)
    else:
        release_date = "latest"
        record = dataset_releases.get_latest_release(dataset=dataset)
    if record is None:
        raise AirflowException(f"No release found for {dataset}/{release_date}")
    return record


def build_release_asset_metadata(*, asset: Asset, dataset: str, release_date: datetime.date) -> Metadata:
    """Build Airflow asset metadata for a dataset release.

    The metadata stores the DynamoDB DatasetReleaseRecord primary key
    (`dataset`, `release_date`) so downstream runs can resolve the full release
    record from the asset event.

    Args:
        asset: The asset the metadata is attached to.
        dataset: The dataset key.
        release_date: The release date being published.

    Returns:
        The Metadata event.
    """
    return Metadata(
        asset,
        {
            "dataset": dataset,
            "release_date": release_date.isoformat(),
        },
    )
