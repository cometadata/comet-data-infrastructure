"""DataCite enrichment runners.

Each enrichment downloads the staged DataCite snapshot, runs its Rust binary, and uploads
the result. The per-enrichment differences (binary name, args, output, configs) live here;
the shared download -> run -> upload plumbing is :func:`comet.aws.transform_task`.
"""

import logging
import pathlib
import shutil
import zipfile

from comet.aws import download_file_from_s3, parse_s3_uri, transform_task
from comet.utils import local_path, run_process

logger = logging.getLogger(__name__)

RESOURCE_TYPE_GENERAL_OUTPUT_FILE = "enrichments.jsonl"

FUNDERS_BINARY = "comet-enrich-datacite-funders"
FUNDERS_OUTPUT_FILE = "enrichments.jsonl"

# Built from the same crate as funders.
AFFILIATIONS_BINARY = "comet-enrich-datacite-affiliations"
AFFILIATIONS_OUTPUT_FILE = "enrichments.jsonl"


def download_config(config_uri: str) -> pathlib.Path:
    """Download a single enrichment-config file from its full S3 URI; return the local path.

    Configs live on S3 (uploaded from ``configs/`` at deploy — see the Makefile); the DAG
    passes each config's full URI, and this fetches it to a local file named after the object key.

    Args:
        config_uri: Full S3 URI of the config file.

    Returns:
        Local path to the downloaded config file.
    """
    work_dir = local_path("enrichment_configs")
    work_dir.mkdir(parents=True, exist_ok=True)
    _, key = parse_s3_uri(config_uri)
    local_file = work_dir / pathlib.Path(key).name
    download_file_from_s3(config_uri, local_file)
    return local_file


def enrich_resource_type_general(*, input_uri: str, output_uri: str, rules_uri: str, enrichment_uri: str):
    """Reclassify ``types.resourceTypeGeneral`` over the DataCite snapshot.

    Downloads the snapshot from ``input_uri`` and the two configs, runs the resource-type-general
    reclassifier, and uploads the single ``enrichments.jsonl`` to ``output_uri``.

    Args:
        input_uri: S3 URI of the staged DataCite snapshot (trailing slash).
        output_uri: S3 URI to upload the enrichment output to (trailing slash).
        rules_uri: S3 URI of the reclassification rules YAML (``--rules``).
        enrichment_uri: S3 URI of the enrichment metadata YAML (``--enrichment``).
    """
    rules = download_config(rules_uri)
    enrichment = download_config(enrichment_uri)
    with transform_task(input_uri, output_uri, upload_glob=RESOURCE_TYPE_GENERAL_OUTPUT_FILE) as ctx:
        run_process(
            [
                "comet-enrich-datacite-resource-type-general",
                "--input",
                str(ctx.download_dir),
                "--output",
                str(ctx.transform_dir / RESOURCE_TYPE_GENERAL_OUTPUT_FILE),
                "--rules",
                str(rules),
                "--enrichment",
                str(enrichment),
            ]
        )


def select_ror_data_member(members: list[str]) -> str:
    """Pick the ROR v2-schema JSON dump from a ROR release zip's member names.

    The funders ``reconcile`` step parses ROR v2 records (``names[].value/types`` and
    ``external_ids[].type/all``), which live in the ``*_schema_v2.json`` member. Older
    releases predating that file fall back to the plain ``*-ror-data.json`` dump.

    Args:
        members: The archive member names (from ``zipfile.namelist()``).

    Returns:
        The member name of the ROR data dump to feed to ``reconcile --ror-data``.

    Raises:
        ValueError: If no ROR data JSON is present in the archive.
    """
    schema_v2 = [m for m in members if m.endswith("_schema_v2.json")]
    if schema_v2:
        return schema_v2[0]
    ror_data = [m for m in members if m.endswith("-ror-data.json")]
    if ror_data:
        return ror_data[0]
    raise ValueError(f"No ROR data JSON found in archive members: {members}")


def prepare_ror_data(ror_data_uri: str) -> pathlib.Path:
    """Download the ROR release zip from S3 and unpack its v2-schema JSON dump.

    ``reconcile --ror-data`` expects a raw (decompressed) ROR v2 JSON file, whereas the
    snapshot staged by the ROR ingest is the Zenodo zip — so this downloads and extracts it.

    Args:
        ror_data_uri: S3 URI of the ROR release zip (as staged by the ror_ingest DAG).

    Returns:
        Local path to the extracted ROR v2 JSON dump.
    """
    _, prefix = parse_s3_uri(ror_data_uri)
    work_dir = local_path("ror_data_funders")
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    zip_path = work_dir / pathlib.Path(prefix).name
    download_file_from_s3(ror_data_uri, zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        member = select_ror_data_member(zf.namelist())
        zf.extract(member, work_dir)

    logger.info(f"Using ROR data dump: {member}")
    return work_dir / member


def enrich_funders(
    *,
    input_uri: str,
    output_uri: str,
    ror_data_uri: str,
    enrichment_config_uri: str,
    marple_url: str = "http://localhost:8000",
):
    """Match DataCite funder names to ROR IDs over the DataCite snapshot.

    Runs the funders ``extract -> query -> reconcile`` pipeline: extract unique funder names
    from the snapshot at ``input_uri``, match them against the Marple ROR matcher at
    ``marple_url``, reconcile the matches against the ROR dump (``ror_data_uri``), and upload
    the single ``enrichments.jsonl`` (DataCite enrichment format) output to ``output_uri``.

    Args:
        input_uri: S3 URI of the staged DataCite snapshot (trailing slash).
        output_uri: S3 URI to upload the enrichment output to (trailing slash).
        ror_data_uri: S3 URI of the ROR release zip used to reconcile matches to ROR records.
        enrichment_config_uri: S3 URI of the funders enrichment config YAML (``--enrichment-config``).
        marple_url: Base URL of the Marple ROR matcher (the sidecar on localhost in Batch).
    """
    config = download_config(enrichment_config_uri)
    ror_data = prepare_ror_data(ror_data_uri)
    try:
        with transform_task(input_uri, output_uri, upload_glob=FUNDERS_OUTPUT_FILE) as ctx:
            work_dir = ctx.transform_dir

            run_process(
                [
                    FUNDERS_BINARY,
                    "extract",
                    "--input",
                    str(ctx.download_dir),
                    "--output",
                    str(work_dir),
                ]
            )

            run_process(
                [
                    FUNDERS_BINARY,
                    "query",
                    "--input",
                    str(work_dir),
                    "--output",
                    str(work_dir),
                    "--base-url",
                    marple_url,
                    "--task",
                    "funder",
                ]
            )

            run_process(
                [
                    FUNDERS_BINARY,
                    "reconcile",
                    "--input",
                    str(work_dir),
                    "--output",
                    str(work_dir / FUNDERS_OUTPUT_FILE),
                    "--ror-data",
                    str(ror_data),
                    "--enrichment-format",
                    "--enrichment-config",
                    str(config),
                ]
            )
    finally:
        shutil.rmtree(ror_data.parent, ignore_errors=True)


def enrich_affiliations(
    *,
    input_uri: str,
    output_uri: str,
    ror_data_uri: str,
    enrichment_config_uri: str,
    marple_url: str = "http://localhost:8000",
):
    """Match DataCite affiliation strings to ROR IDs over the DataCite snapshot.

    Runs the affiliations ``extract -> query -> reconcile`` pipeline: extract unique affiliation
    strings from the snapshot at ``input_uri``, match them against the Marple ROR matcher at
    ``marple_url``, reconcile the matches against the ROR dump (``ror_data_uri``), and upload
    the single ``enrichments.jsonl`` (DataCite enrichment format) output to ``output_uri``.

    Args:
        input_uri: S3 URI of the staged DataCite snapshot (trailing slash).
        output_uri: S3 URI to upload the enrichment output to (trailing slash).
        ror_data_uri: S3 URI of the ROR release zip used to reconcile matches to ROR records.
        enrichment_config_uri: S3 URI of the affiliations enrichment config YAML (``--enrichment-config``).
        marple_url: Base URL of the Marple ROR matcher (the sidecar on localhost in Batch).
    """
    config = download_config(enrichment_config_uri)
    ror_data = prepare_ror_data(ror_data_uri)
    try:
        with transform_task(input_uri, output_uri, upload_glob=AFFILIATIONS_OUTPUT_FILE) as ctx:
            work_dir = ctx.transform_dir

            run_process(
                [
                    AFFILIATIONS_BINARY,
                    "extract",
                    "--input",
                    str(ctx.download_dir),
                    "--output",
                    str(work_dir),
                ]
            )

            run_process(
                [
                    AFFILIATIONS_BINARY,
                    "query",
                    "--input",
                    str(work_dir),
                    "--output",
                    str(work_dir),
                    "--base-url",
                    marple_url,
                    "--task",
                    "affiliation",
                ]
            )

            run_process(
                [
                    AFFILIATIONS_BINARY,
                    "reconcile",
                    "--input",
                    str(work_dir),
                    "--output",
                    str(work_dir / AFFILIATIONS_OUTPUT_FILE),
                    "--ror-data",
                    str(ror_data),
                    "--enrichment-format",
                    "--enrichment-config",
                    str(config),
                ]
            )
    finally:
        shutil.rmtree(ror_data.parent, ignore_errors=True)
