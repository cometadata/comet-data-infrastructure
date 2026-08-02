"""Cyclopts CLI for the DataCite dataset."""

import cyclopts

from comet.datacite.enrich import DEFAULT_OUTPUT_WRITER_LANES, DEFAULT_ROR_SERVICE_URL

datacite_app = cyclopts.App(name="datacite", help="DataCite dataset commands.")

enrich_app = cyclopts.App(name="enrich", help="DataCite enrichment commands.")
datacite_app.command(enrich_app)


@datacite_app.command
def download(
    *,
    target_uri: str,
    datacite_bucket_name: str,
    datacite_bucket_region: str,
    expected_file_count: int,
    expected_total_bytes: int,
) -> None:
    """Download the DataCite snapshot and upload it to S3.

    Args:
        target_uri: Destination S3 URI for the DataCite snapshot (trailing slash).
        datacite_bucket_name: Source DataCite S3 bucket.
        datacite_bucket_region: AWS region of the source DataCite bucket.
        expected_file_count: Expected number of .jsonl.gz files (from MANIFEST.json).
        expected_total_bytes: Expected total size of the .jsonl.gz files in bytes.
    """
    from comet.cli import setup_logging
    from comet.datacite import datacite

    setup_logging()
    datacite.download_datacite(
        target_uri=target_uri,
        datacite_bucket_name=datacite_bucket_name,
        datacite_bucket_region=datacite_bucket_region,
        expected_file_count=expected_file_count,
        expected_total_bytes=expected_total_bytes,
    )


@enrich_app.command(name="resource-type-general")
def resource_type_general(
    *,
    input_uri: str,
    output_uri: str,
    rules_uri: str,
    provenance_uri: str,
    output_writer_lanes: int = DEFAULT_OUTPUT_WRITER_LANES,
) -> None:
    """Reclassify ``types.resourceTypeGeneral`` over the DataCite snapshot.

    Args:
        input_uri: S3 URI of the staged DataCite snapshot (trailing slash).
        output_uri: S3 URI to upload the enrichment output to (trailing slash).
        rules_uri: S3 URI of the reclassification rules YAML.
        provenance_uri: S3 URI of the enrichment provenance YAML.
        output_writer_lanes: Parallel writer lanes for the enrichment output.
    """
    from comet.cli import setup_logging
    from comet.datacite import enrich

    setup_logging()
    enrich.enrich_resource_type_general(
        input_uri=input_uri,
        output_uri=output_uri,
        rules_uri=rules_uri,
        provenance_uri=provenance_uri,
        output_writer_lanes=output_writer_lanes,
    )


@enrich_app.command(name="funders")
def funders(
    *,
    input_uri: str,
    output_uri: str,
    ror_data_uri: str,
    provenance_uri: str,
    ror_service_url: str = DEFAULT_ROR_SERVICE_URL,
    output_writer_lanes: int = DEFAULT_OUTPUT_WRITER_LANES,
) -> None:
    """Match DataCite funder names to ROR IDs over the DataCite snapshot.

    Args:
        input_uri: S3 URI of the staged DataCite snapshot (trailing slash).
        output_uri: S3 URI to upload the enrichment output to (trailing slash).
        ror_data_uri: S3 URI of the ROR release zip used to reconcile matches to ROR records.
        provenance_uri: S3 URI of the funders provenance YAML.
        ror_service_url: Base URL of the ROR match service.
        output_writer_lanes: Parallel writer lanes for the enrichment output.
    """
    from comet.cli import setup_logging
    from comet.datacite import enrich

    setup_logging()
    enrich.enrich_funders(
        input_uri=input_uri,
        output_uri=output_uri,
        ror_data_uri=ror_data_uri,
        provenance_uri=provenance_uri,
        ror_service_url=ror_service_url,
        output_writer_lanes=output_writer_lanes,
    )


@enrich_app.command(name="affiliations")
def affiliations(
    *,
    input_uri: str,
    output_uri: str,
    provenance_uri: str,
    ror_service_url: str = DEFAULT_ROR_SERVICE_URL,
    output_writer_lanes: int = DEFAULT_OUTPUT_WRITER_LANES,
) -> None:
    """Match DataCite affiliation strings to ROR IDs over the DataCite snapshot.

    Args:
        input_uri: S3 URI of the staged DataCite snapshot (trailing slash).
        output_uri: S3 URI to upload the enrichment output to (trailing slash).
        provenance_uri: S3 URI of the affiliations provenance YAML.
        ror_service_url: Base URL of the ROR match service.
        output_writer_lanes: Parallel writer lanes for the enrichment output.
    """
    from comet.cli import setup_logging
    from comet.datacite import enrich

    setup_logging()
    enrich.enrich_affiliations(
        input_uri=input_uri,
        output_uri=output_uri,
        provenance_uri=provenance_uri,
        ror_service_url=ror_service_url,
        output_writer_lanes=output_writer_lanes,
    )
