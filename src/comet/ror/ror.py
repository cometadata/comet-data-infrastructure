import logging

import pendulum
import pooch

from comet.aws import download_source_task
from comet.model.dataset_version_model import DatasetRelease
from comet.zenodo import list_zenodo_records

logger = logging.getLogger(__name__)

ROR_ZENODO_CONCEPT_ID = 6347574


def get_new_ror_release(*, published_after: pendulum.DateTime | None = None) -> DatasetRelease | None:
    """Return the newest ROR release published strictly after ``published_after``.

    Args:
        published_after: Release datetime of the most recent known version. Only
            releases published strictly after this date are considered. If None, no
            lower bound is applied.

    Returns:
        DatasetRelease with release_date, download_url, file_name, and file_hash set,
        or None if there is no newer release.
    """
    end_date = pendulum.now("UTC")
    records = list_zenodo_records(conceptrecid=ROR_ZENODO_CONCEPT_ID, start_date=published_after, end_date=end_date)
    logger.info(f"ROR Zenodo records found: {len(records)}")
    if not records:
        return None

    logger.info(f"ROR latest record: publication_date={records[0].publication_date}")
    latest = records[0]
    if not latest.files:
        return None

    file = latest.files[0]
    return DatasetRelease(
        release_date=latest.publication_date,
        download_url=file.link,
        file_name=file.file_name,
        file_hash=file.file_hash,
    )


def download_ror(*, target_uri: str, download_url: str, file_name: str, file_hash: str | None = None):
    """Download the ROR release zip from Zenodo and upload it to the DMP Tool S3 bucket.

    The zip is saved as-is; the download_source_task context manager uploads everything
    left in the download directory to S3 on exit.

    Args:
        target_uri: S3 URI to upload the ROR release to (trailing slash).
        download_url: the Zenodo download URL for the ROR zip file.
        file_name: name to save the downloaded zip as (becomes the S3 object name).
        file_hash: optional expected MD5 checksum (md5:... format).
    """
    with download_source_task(target_uri) as ctx:
        pooch.retrieve(
            url=download_url,
            known_hash=file_hash,
            fname=file_name,
            path=ctx.download_dir,
            progressbar=True,
        )
