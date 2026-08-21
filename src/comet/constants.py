"""Shared constants used across comet workflows and DAGs."""

from dataclasses import dataclass


def validate_releases_to_keep(releases_to_keep: int) -> None:
    """Require a positive retention count; zero would make the pruning slice empty."""
    if releases_to_keep < 1:
        raise ValueError(f"releases_to_keep must be at least 1, got {releases_to_keep}")


@dataclass(frozen=True)
class Source:
    """Source dataset and minimum number of recent releases to retain."""

    identifier: str
    releases_to_keep: int

    def __post_init__(self):
        """Validate the retention count."""
        validate_releases_to_keep(self.releases_to_keep)


@dataclass(frozen=True)
class Enrichment:
    """Enrichment method and minimum number of published releases to retain."""

    source: Source
    method: str
    releases_to_keep: int

    def __post_init__(self):
        """Validate the retention count."""
        validate_releases_to_keep(self.releases_to_keep)

    @property
    def identifier(self) -> str:
        """Identifier for this enrichment's releases.

        Used as the DynamoDB hash key, the Airflow asset name, and the source_uris
        keys passed to comet publish. Never part of an S3 path.
        """
        return f"{self.source.identifier}-{self.method}"


ROR_SOURCE = Source("ror", 12)
DATACITE_SOURCE = Source("datacite", 3)

DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT = Enrichment(DATACITE_SOURCE, "resource-type-general", 3)
DATACITE_FUNDERS_ENRICHMENT = Enrichment(DATACITE_SOURCE, "funders", 3)
DATACITE_AFFILIATIONS_ENRICHMENT = Enrichment(DATACITE_SOURCE, "affiliations", 3)

# Registry used by release indexing and pruning.
SOURCE_REGISTRY: dict[Source, tuple[Enrichment, ...]] = {
    ROR_SOURCE: (),
    DATACITE_SOURCE: (
        DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT,
        DATACITE_FUNDERS_ENRICHMENT,
        DATACITE_AFFILIATIONS_ENRICHMENT,
    ),
}


def enrichments_for_source(source: str) -> list[Enrichment]:
    """Look up a registered source's enrichments.

    Args:
        source: The source dataset identifier.

    Returns:
        The source's enrichments in registry order; empty when it has none.

    Raises:
        ValueError: If the source is not registered.
    """
    for registered_source, enrichments in SOURCE_REGISTRY.items():
        if registered_source.identifier == source:
            return list(enrichments)
    valid_sources = ", ".join(sorted(registered.identifier for registered in SOURCE_REGISTRY))
    raise ValueError(f"Unknown source '{source}'; valid sources: {valid_sources}")
