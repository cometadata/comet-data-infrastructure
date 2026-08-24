"""DynamoDB persistence and discovery service for dataset releases."""

from __future__ import annotations

from datetime import UTC, date, datetime
import logging
import os

from pynamodb.attributes import MapAttribute, UnicodeAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.models import Model

from comet.model.dataset_version_model import DatasetRelease
from comet.utils import get_env, get_region

log = logging.getLogger(__name__)


class DatasetReleaseMeta(type):
    """Metaclass that resolves the PynamoDB table config at runtime, not import.

    PynamoDB reads ``table_name`` only when an operation runs, so exposing it as a class
    property defers ``AWS_ENV``/``AWS_REGION`` validation to runtime — a missing env var then
    fails loudly on the first operation instead of producing a wrong table name at import.
    ``region`` returns the raw ``AWS_REGION`` without raising (PynamoDB reads it at class
    creation, which must not blow up); the loud check lives in ``table_name``.
    """

    @property
    def region(cls) -> str | None:
        """Return the raw AWS_REGION for PynamoDB (no raise; validated in table_name)."""
        return os.environ.get("AWS_REGION")

    @property
    def table_name(cls) -> str:
        """Return the dataset-releases table name, validating AWS_ENV/AWS_REGION at runtime."""
        get_region()
        return f"comet-{get_env()}-dataset-releases"


class DatasetReleaseRecord(Model):
    """PynamoDB model for a discovered dataset release.

    Attributes:
        dataset: The dataset identifier (hash key).
        release_date: ISO date string "YYYY-MM-DD" (range key, sortable).
        file_name: File to download, if applicable.
        download_url: Direct download URL, if applicable.
        file_hash: MD5 checksum for the file, if applicable.
        run_id: Identifier of the run that ingested this release
            (matches the S3 prefix segment under ``{dataset}-download/``).
        metadata: Arbitrary extra key/value pairs.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta(metaclass=DatasetReleaseMeta):
        """PynamoDB table configuration."""

        billing_mode = "PAY_PER_REQUEST"

    dataset = UnicodeAttribute(hash_key=True)
    release_date = UnicodeAttribute(range_key=True)
    file_name = UnicodeAttribute(null=True)
    download_url = UnicodeAttribute(null=True)
    file_hash = UnicodeAttribute(null=True)
    run_id = UnicodeAttribute(null=True)
    metadata = MapAttribute(default=dict)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()

    def to_dataset_release(self) -> DatasetRelease:
        """Convert this persisted record into a DatasetRelease (carrying run_id)."""
        return DatasetRelease(
            release_date=date.fromisoformat(self.release_date),
            file_name=self.file_name,
            download_url=self.download_url,
            file_hash=self.file_hash,
            run_id=self.run_id,
            metadata=self.metadata.as_dict() if self.metadata is not None else {},
        )


def get_latest(model_cls: type[Model], hash_key: str) -> Model | None:
    """Return the most recent record for a hash key (reverse range-key scan, limit 1).

    Args:
        model_cls: PynamoDB model class to query.
        hash_key: Hash key value.

    Returns:
        The most recent record, or None if no records exist.
    """
    return next(model_cls.query(hash_key, scan_index_forward=False, limit=1), None)


def get_latest_release(*, dataset: str) -> DatasetReleaseRecord | None:
    """Return the most recently published release record for a dataset.

    Args:
        dataset: The dataset identifier.

    Returns:
        The most recent DatasetReleaseRecord, or None if no records exist.
    """
    return get_latest(DatasetReleaseRecord, dataset)


def get_release(*, dataset: str, release_date: str) -> DatasetReleaseRecord | None:
    """Return the release record for a (dataset, release_date) primary key.

    Args:
        dataset: The dataset identifier (hash key).
        release_date: ISO date string "YYYY-MM-DD" (range key).

    Returns:
        The matching DatasetReleaseRecord, or None if no such record exists.
    """
    try:
        return DatasetReleaseRecord.get(dataset, release_date)
    except DoesNotExist:
        return None


def persist_discovered_release(*, dataset: str, release: DatasetRelease, run_id: str) -> DatasetReleaseRecord:
    """Upsert a discovered release, keyed by (dataset, release_date).

    A re-run for an existing release_date refreshes the record in place — notably the
    ``run_id`` (and the file fields, in case a re-download produced a different artifact) —
    so downstream consumers that resolve the S3 prefix from ``run_id`` pick up the new run
    rather than the stale one. ``created_at`` is preserved as "first seen"; ``updated_at``
    tracks the last refresh.

    Args:
        dataset: The dataset identifier.
        release: The DatasetRelease returned by a detector function.
        run_id: Identifier of the run that ingested this release; stored on the
            record so callers can reconstruct the S3 prefix the bytes live under.

    Returns:
        The persisted (created or updated) DatasetReleaseRecord.
    """
    release_date_str = release.release_date.isoformat()
    now = datetime.now(UTC).isoformat()

    try:
        record = DatasetReleaseRecord.get(dataset, release_date_str)
        action = "Updated existing"
    except DoesNotExist:
        record = DatasetReleaseRecord(dataset=dataset, release_date=release_date_str, created_at=now)
        action = "Persisted new"

    record.file_name = release.file_name
    record.download_url = release.download_url
    record.file_hash = release.file_hash
    record.run_id = run_id
    record.metadata = release.metadata
    record.updated_at = now
    record.save()
    log.info(f"{action} release: dataset={dataset} release_date={release_date_str} run_id={run_id}")
    return record
