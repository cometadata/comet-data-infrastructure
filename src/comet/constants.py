"""Shared constants used across comet workflows and DAGs."""

from dataclasses import dataclass

# Used as S3 prefixes, DynamoDB hash keys, and Airflow asset names.
ROR_DATASET_NAME = "ror"
DATACITE_DATASET_NAME = "datacite"


@dataclass(frozen=True)
class Enrichment:
    """An enrichment method applied to a source dataset."""

    source: str
    method: str

    @property
    def identifier(self) -> str:
        """Identifier for this enrichment's releases.

        Used as the DynamoDB hash key, the Airflow asset name, and the source_uris
        keys passed to comet publish. Never part of an S3 path.
        """
        return f"{self.source}-{self.method}"


DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT = Enrichment(DATACITE_DATASET_NAME, "resource-type-general")
DATACITE_FUNDERS_ENRICHMENT = Enrichment(DATACITE_DATASET_NAME, "funders")
DATACITE_AFFILIATIONS_ENRICHMENT = Enrichment(DATACITE_DATASET_NAME, "affiliations")

# The release index is rendered from this registry, so an enrichment missing here
# disappears from its source's index.json.
ENRICHMENTS_BY_SOURCE = {
    DATACITE_DATASET_NAME: [
        DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT,
        DATACITE_FUNDERS_ENRICHMENT,
        DATACITE_AFFILIATIONS_ENRICHMENT,
    ],
}


def enrichments_for_source(source: str) -> list[Enrichment]:
    """Return the registered enrichments for a source.

    Args:
        source: The source dataset name, e.g. "datacite".

    Returns:
        The source's enrichments in registry order.

    Raises:
        ValueError: If the source has no registry entry.
    """
    if source not in ENRICHMENTS_BY_SOURCE:
        raise ValueError(f"Unknown source '{source}'; valid sources: {', '.join(sorted(ENRICHMENTS_BY_SOURCE))}")
    return ENRICHMENTS_BY_SOURCE[source]
