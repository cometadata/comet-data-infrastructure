"""Shared constants used across comet workflows and DAGs."""

# Used as S3 prefixes, DynamoDB hash keys, and Airflow asset names.
ROR_DATASET_NAME = "ror"
DATACITE_DATASET_NAME = "datacite"

# One per enrichment.
DATACITE_RESOURCE_TYPE_GENERAL_DATASET_NAME = "datacite-resource-type-general"
DATACITE_FUNDERS_DATASET_NAME = "datacite-funders"
DATACITE_AFFILIATIONS_DATASET_NAME = "datacite-affiliations"
