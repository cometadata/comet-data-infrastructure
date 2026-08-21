from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from comet.constants import SOURCE_REGISTRY

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    from comet.dynamodb_store import DatasetReleaseRecord


@dataclass(frozen=True)
class PruneCandidate:
    """S3 prefix selected for removal and the release records to mark first."""

    prefix: str
    reason: str
    records: tuple[DatasetReleaseRecord, ...]


def prunable_releases(
    records_by_dataset: Mapping[str, Sequence[DatasetReleaseRecord]],
) -> dict[tuple[str, str], str]:
    """Return registered releases outside retention, keyed by dataset and release date.

    Unregistered datasets are omitted so their prefixes remain protected.
    """
    prunable: dict[tuple[str, str], str] = {}
    for source, enrichments in SOURCE_REGISTRY.items():
        for enrichment in enrichments:
            records = sorted(records_by_dataset.get(enrichment.identifier, []), key=lambda record: record.release_date)
            published_records = [record for record in records if record.published_at]
            newest = published_records[-1].release_date if published_records else None

            # Published enrichment releases outside retention.
            for record in published_records[: -enrichment.releases_to_keep]:
                prunable[record.dataset, record.release_date] = "old published enrichment"

            # Unpublished runs superseded by a newer publication.
            for record in records:
                if not record.published_at and newest and record.release_date < newest:
                    prunable[record.dataset, record.release_date] = "superseded unpublished enrichment"

        # Source releases outside retention.
        records = sorted(records_by_dataset.get(source.identifier, []), key=lambda record: record.release_date)
        for record in records[: -source.releases_to_keep]:
            prunable[record.dataset, record.release_date] = "old source release"
    return prunable


def select_prune_candidates(
    run_prefixes: Mapping[str, datetime | None],
    records: Sequence[DatasetReleaseRecord],
    *,
    orphan_cutoff: datetime,
) -> list[PruneCandidate]:
    """Return run prefixes eligible for removal.

    A tracked prefix qualifies only when all unpruned records are outside retention.
    Previously marked prefixes are retried, and untracked prefixes wait until
    ``orphan_cutoff``.
    """
    records_by_dataset: dict[str, list[DatasetReleaseRecord]] = defaultdict(list)
    for record in records:
        records_by_dataset[record.dataset].append(record)
    prunable = prunable_releases(records_by_dataset)

    records_by_prefix: dict[str, list[DatasetReleaseRecord]] = defaultdict(list)
    for record in records:
        records_by_prefix[record.source_prefix].append(record)

    selected: list[PruneCandidate] = []
    for prefix, timestamp in run_prefixes.items():
        referencing = records_by_prefix.get(prefix, [])
        if not referencing:
            if timestamp is not None and timestamp <= orphan_cutoff:
                selected.append(PruneCandidate(prefix, "orphaned run", ()))
            continue
        unmarked = [record for record in referencing if not record.pruned_at]
        if not unmarked:
            selected.append(PruneCandidate(prefix, "previously marked for pruning", ()))
        elif all((record.dataset, record.release_date) in prunable for record in unmarked):
            selected.append(
                PruneCandidate(prefix, prunable[unmarked[0].dataset, unmarked[0].release_date], tuple(unmarked))
            )
    return selected
