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
    property defers ``COMET_ENV``/``AWS_REGION`` validation to runtime — a missing env var then
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
        """Return the dataset-releases table name, validating COMET_ENV/AWS_REGION at runtime."""
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
        source_prefix: Key prefix on the data bucket of the run output that produced this release.
        metadata: Arbitrary extra key/value pairs.
        release_type: Kind of published release ("full"); None until published.
        published_at: ISO datetime string of the last publish; None until published.
        export_path: Key prefix of the published copy on the export bucket.
        pruned_at: ISO timestamp set before the source prefix is removed; None until
            the release is selected for pruning.
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
    source_prefix = UnicodeAttribute(null=True)
    metadata = MapAttribute(default=dict)
    release_type = UnicodeAttribute(null=True)
    published_at = UnicodeAttribute(null=True)
    export_path = UnicodeAttribute(null=True)
    pruned_at = UnicodeAttribute(null=True)
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
    return next(model_cls.query(hash_key, consistent_read=True, scan_index_forward=False, limit=1), None)


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
        return DatasetReleaseRecord.get(dataset, release_date, consistent_read=True)
    except DoesNotExist:
        return None


def list_releases(*, dataset: str) -> list[DatasetReleaseRecord]:
    """Return all release records for a dataset using a strongly consistent read.

    Args:
        dataset: The dataset identifier.

    Returns:
        List of DatasetReleaseRecord, oldest release first.
    """
    return list(DatasetReleaseRecord.query(dataset, consistent_read=True))


def list_all_releases() -> list[DatasetReleaseRecord]:
    """Return all release records in unspecified order using a strongly consistent scan."""
    return list(DatasetReleaseRecord.scan(consistent_read=True))


def mark_published(*, dataset: str, release_date: str, export_path: str, release_type: str) -> None:
    """Record that a release has been published to the export bucket.

    Uses an UpdateItem with set actions rather than get+save. Discovery writes also update
    only their owned fields, so concurrent operations cannot drop publication state. The
    condition requires the record to exist — publishing an unknown release raises.

    Args:
        dataset: The dataset identifier (hash key).
        release_date: ISO date string "YYYY-MM-DD" (range key).
        export_path: Key prefix of the published copy on the export bucket.
        release_type: Kind of published release.
    """
    now = datetime.now(UTC).isoformat()
    record = DatasetReleaseRecord(dataset, release_date)
    record.update(
        actions=[
            DatasetReleaseRecord.release_type.set(release_type),
            DatasetReleaseRecord.published_at.set(now),
            DatasetReleaseRecord.export_path.set(export_path),
            DatasetReleaseRecord.updated_at.set(now),
        ],
        condition=DatasetReleaseRecord.created_at.exists(),
    )
    log.info(f"Marked published: dataset={dataset} release_date={release_date} export_path={export_path}")


def mark_release_pruned(*, dataset: str, release_date: str, expected_source_prefix: str) -> None:
    """Mark a release as pruned before removing its S3 data.

    The update fails if the record is missing or no longer references
    ``expected_source_prefix``, protecting a concurrent refresh.
    """
    now = datetime.now(UTC).isoformat()
    record = DatasetReleaseRecord(dataset, release_date)
    record.update(
        actions=[
            DatasetReleaseRecord.pruned_at.set(now),
            DatasetReleaseRecord.updated_at.set(now),
        ],
        condition=(
            DatasetReleaseRecord.created_at.exists() & (DatasetReleaseRecord.source_prefix == expected_source_prefix)
        ),
    )
    log.info(f"Marked pruned: dataset={dataset} release_date={release_date}")


def persist_discovered_release(
    *, dataset: str, release: DatasetRelease, run_id: str, source_prefix: str
) -> DatasetReleaseRecord:
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
        source_prefix: Key prefix on the data bucket of the run output that produced this release.

    Returns:
        The persisted (created or updated) DatasetReleaseRecord.
    """
    release_date_str = release.release_date.isoformat()
    now = datetime.now(UTC).isoformat()

    record = DatasetReleaseRecord(dataset, release_date_str)
    actions = [
        DatasetReleaseRecord.created_at.set(DatasetReleaseRecord.created_at | now),
        DatasetReleaseRecord.run_id.set(run_id),
        DatasetReleaseRecord.source_prefix.set(source_prefix),
        DatasetReleaseRecord.metadata.set(release.metadata),
        DatasetReleaseRecord.pruned_at.remove(),
        DatasetReleaseRecord.updated_at.set(now),
    ]
    for attribute, value in (
        (DatasetReleaseRecord.file_name, release.file_name),
        (DatasetReleaseRecord.download_url, release.download_url),
        (DatasetReleaseRecord.file_hash, release.file_hash),
    ):
        actions.append(attribute.set(value) if value is not None else attribute.remove())

    record.update(actions=actions)
    log.info(f"Persisted release: dataset={dataset} release_date={release_date_str} run_id={run_id}")
    return record
