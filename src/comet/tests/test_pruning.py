from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from comet.constants import (
    DATACITE_FUNDERS_ENRICHMENT,
    DATACITE_SOURCE,
    ROR_SOURCE,
)
import comet.pruning as pruning


@dataclass
class StubRecord:
    """Stands in for DatasetReleaseRecord."""

    dataset: str
    source_prefix: str
    release_date: str
    published_at: str | None
    pruned_at: str | None


def record(dataset: str, prefix: str, release_date: str, *, published: bool = False, pruned: bool = False):
    return StubRecord(
        dataset=dataset,
        source_prefix=prefix,
        release_date=release_date,
        published_at="2026-01-01T00:00:00+00:00" if published else None,
        pruned_at="2026-04-01T00:00:00+00:00" if pruned else None,
    )


def select(runs, records, **overrides):
    kwargs = dict(orphan_cutoff=datetime(2026, 4, 1, tzinfo=UTC))
    kwargs.update(overrides)
    return pruning.select_prune_candidates(runs, records, **kwargs)


def reasons_by_prefix(candidates):
    return {candidate.prefix: candidate.reason for candidate in candidates}


class TestSelectPruneCandidates:
    def test_deletes_only_orphans_older_than_the_grace_period(self):
        runs = {
            "datacite_ingest/scheduled__2026-01-01T00:00:00+00:00/": datetime(2026, 1, 1, tzinfo=UTC),
            "datacite_ingest/scheduled__2026-03-20T00:00:00+00:00/": datetime(2026, 3, 20, tzinfo=UTC),
            "datacite_ingest/custom-run/": None,
        }

        candidates = select(runs, [], orphan_cutoff=datetime(2026, 3, 1, tzinfo=UTC))

        assert reasons_by_prefix(candidates) == {
            "datacite_ingest/scheduled__2026-01-01T00:00:00+00:00/": "orphaned run",
        }

    def test_retains_twelve_ror_source_releases(self):
        records = [
            record(ROR_SOURCE.identifier, f"ror_ingest/run-{month}/", f"2025-{month:02}-01") for month in range(1, 14)
        ]
        runs = {item.source_prefix: datetime(2025, 1, 1, tzinfo=UTC) for item in records}

        candidates = select(runs, records)

        assert reasons_by_prefix(candidates) == {"ror_ingest/run-1/": "old source release"}
        assert candidates[0].records == (records[0],)

    def test_retains_three_datacite_source_releases_without_waiting_for_enrichments(self):
        records = [
            record(DATACITE_SOURCE.identifier, f"datacite_ingest/run-{month}/", f"2026-{month:02}-01")
            for month in range(1, 5)
        ]
        runs = {item.source_prefix: datetime(2026, 1, 1, tzinfo=UTC) for item in records}

        candidates = select(runs, records)

        assert reasons_by_prefix(candidates) == {"datacite_ingest/run-1/": "old source release"}

    def test_prunes_unpublished_output_only_after_a_newer_publication(self):
        january = record(DATACITE_FUNDERS_ENRICHMENT.identifier, "funders/jan/", "2026-01-01")
        runs = {january.source_prefix: datetime(2026, 1, 1, tzinfo=UTC)}
        records = [january]

        protected = select(runs, records)
        records.append(record(DATACITE_FUNDERS_ENRICHMENT.identifier, "funders/feb/", "2026-02-01", published=True))
        superseded = select(runs, records)

        assert protected == []
        assert reasons_by_prefix(superseded) == {"funders/jan/": "superseded unpublished enrichment"}

    def test_retains_the_three_newest_published_enrichments(self):
        records = [
            record(DATACITE_FUNDERS_ENRICHMENT.identifier, f"funders/{month}/", f"2026-{month:02}-01", published=True)
            for month in range(1, 5)
        ]
        runs = {item.source_prefix: datetime(2026, 1, 1, tzinfo=UTC) for item in records}

        candidates = select(runs, records)

        assert reasons_by_prefix(candidates) == {"funders/1/": "old published enrichment"}

    def test_protects_a_shared_prefix_while_any_referencing_release_is_retained(self):
        shared = "ror_ingest/shared/"
        records = [record(ROR_SOURCE.identifier, shared, "2026-01-01")] + [
            record(ROR_SOURCE.identifier, f"ror_ingest/run-{month}/", f"2026-{month:02}-01") for month in range(2, 14)
        ]
        records.append(record(DATACITE_FUNDERS_ENRICHMENT.identifier, shared, "2026-01-01", published=True))

        candidates = select({shared: datetime(2026, 1, 1, tzinfo=UTC)}, records)

        assert candidates == []

    def test_protects_prefixes_of_unregistered_datasets(self):
        legacy = record("openalex", "openalex_ingest/run-1/", "2025-01-01")
        runs = {legacy.source_prefix: datetime(2025, 1, 1, tzinfo=UTC)}

        candidates = select(runs, [legacy])

        assert candidates == []

    def test_retries_deletion_for_a_prefix_already_marked_pruned(self):
        pruned = record(ROR_SOURCE.identifier, "ror_ingest/run-1/", "2025-01-01", pruned=True)
        runs = {pruned.source_prefix: datetime(2026, 3, 20, tzinfo=UTC)}

        candidates = select(runs, [pruned])

        assert reasons_by_prefix(candidates) == {"ror_ingest/run-1/": "previously marked for pruning"}
        assert candidates[0].records == ()
