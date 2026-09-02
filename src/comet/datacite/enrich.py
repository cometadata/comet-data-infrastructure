"""DataCite enrichment runners."""

import logging
import pathlib
import shutil
import zipfile

from comet.aws import local_file_for_uri, s5cmd_download_file, transform_task
from comet.utils import local_path, run_process

logger = logging.getLogger(__name__)

UPLOAD_GLOB = "*"
UPLOAD_EXCLUDE_PATTERNS = (".work/*",)
DEFAULT_ROR_SERVICE_URL = "http://localhost:8000"
DEFAULT_OUTPUT_WRITER_LANES = 1


def download_config(config_uri: str) -> pathlib.Path:
    """Download a configuration file from S3 and return its local path."""
    work_dir = local_path("enrichment_configs")
    work_dir.mkdir(parents=True, exist_ok=True)
    local_file = local_file_for_uri(config_uri, work_dir)
    s5cmd_download_file(config_uri, local_file)
    return local_file


def enrich_resource_type_general(
    *,
    input_uri: str,
    output_uri: str,
    source_release_date: list[str],
    rules_uri: str,
    source_id: str,
    output_writer_lanes: int = DEFAULT_OUTPUT_WRITER_LANES,
):
    """Reclassify ``types.resourceTypeGeneral`` over the DataCite snapshot."""
    rules = download_config(rules_uri)
    with transform_task(
        input_uri,
        output_uri,
        upload_glob=UPLOAD_GLOB,
        upload_exclude_patterns=UPLOAD_EXCLUDE_PATTERNS,
    ) as ctx:
        run_process(
            [
                "comet-enrich",
                "resource-type-general",
                "--input",
                str(ctx.download_dir),
                "--output",
                str(ctx.transform_dir),
                "--rules",
                str(rules),
                "--source-id",
                source_id,
                *(arg for value in source_release_date for arg in ("--source-release-date", value)),
                "--output-writer-lanes",
                str(output_writer_lanes),
            ]
        )


def select_ror_data_member(members: list[str]) -> str:
    """Pick the ROR v2-schema JSON dump from a release archive."""
    schema_v2 = [member for member in members if member.endswith("_schema_v2.json")]
    if schema_v2:
        return schema_v2[0]
    ror_data = [member for member in members if member.endswith("-ror-data.json")]
    if ror_data:
        return ror_data[0]
    raise ValueError(f"No ROR data JSON found in archive members: {members}")


def prepare_ror_data(ror_data_uri: str) -> pathlib.Path:
    """Download a ROR release archive and extract its v2-schema JSON dump."""
    work_dir = local_path("ror_data_funders")
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    zip_path = local_file_for_uri(ror_data_uri, work_dir)
    s5cmd_download_file(ror_data_uri, zip_path)

    with zipfile.ZipFile(zip_path) as archive:
        member = select_ror_data_member(archive.namelist())
        archive.extract(member, work_dir)

    logger.info(f"Using ROR data dump: {member}")
    return work_dir / member


def enrich_funders(
    *,
    input_uri: str,
    output_uri: str,
    source_release_date: list[str],
    ror_data_uri: str,
    source_id: str,
    ror_service_url: str = DEFAULT_ROR_SERVICE_URL,
    output_writer_lanes: int = DEFAULT_OUTPUT_WRITER_LANES,
):
    """Match DataCite funder names to ROR IDs."""
    ror_data = prepare_ror_data(ror_data_uri)
    try:
        with transform_task(
            input_uri,
            output_uri,
            upload_glob=UPLOAD_GLOB,
            upload_exclude_patterns=UPLOAD_EXCLUDE_PATTERNS,
        ) as ctx:
            run_process(
                [
                    "comet-enrich",
                    "funders",
                    "--input",
                    str(ctx.download_dir),
                    "--output",
                    str(ctx.transform_dir),
                    "--source-id",
                    source_id,
                    *(arg for value in source_release_date for arg in ("--source-release-date", value)),
                    "--ror-file",
                    str(ror_data),
                    "--ror-service-url",
                    ror_service_url,
                    "--output-writer-lanes",
                    str(output_writer_lanes),
                ]
            )
    finally:
        shutil.rmtree(ror_data.parent, ignore_errors=True)


def enrich_affiliations(
    *,
    input_uri: str,
    output_uri: str,
    source_release_date: list[str],
    source_id: str,
    ror_service_url: str = DEFAULT_ROR_SERVICE_URL,
    output_writer_lanes: int = DEFAULT_OUTPUT_WRITER_LANES,
):
    """Match DataCite affiliation strings to ROR IDs."""
    with transform_task(
        input_uri,
        output_uri,
        upload_glob=UPLOAD_GLOB,
        upload_exclude_patterns=UPLOAD_EXCLUDE_PATTERNS,
    ) as ctx:
        run_process(
            [
                "comet-enrich",
                "affiliations",
                "--input",
                str(ctx.download_dir),
                "--output",
                str(ctx.transform_dir),
                "--source-id",
                source_id,
                *(arg for value in source_release_date for arg in ("--source-release-date", value)),
                "--ror-service-url",
                ror_service_url,
                "--output-writer-lanes",
                str(output_writer_lanes),
            ]
        )
