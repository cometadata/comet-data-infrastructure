from __future__ import annotations

import logging
from urllib.parse import urlencode

import pendulum

from comet.model.zenodo_model import ZenodoFile, ZenodoRecord
from comet.utils import retry_session

logger = logging.getLogger(__name__)


def list_zenodo_records(
    *,
    conceptrecid: int,
    start_date: pendulum.Date | None = None,
    end_date: pendulum.Date | None = None,
    page_size: int = 10,
    timeout: float = 30.0,
) -> list[ZenodoRecord]:
    """Fetch the most recent Zenodo record versions for a concept, filtered by publication date.

    Zenodo applies the date filter, and only the first page of results is fetched.

    Args:
        conceptrecid: Zenodo concept record ID shared across all versions.
        start_date: Earliest publication date to include; no lower bound if None.
        end_date: Latest publication date to include; no upper bound if None.
        page_size: Maximum number of records to return.
        timeout: HTTP request timeout in seconds.

    Returns:
        Up to ``page_size`` matching ZenodoRecord objects, newest publication date first.
    """
    clauses = [f"conceptrecid:{conceptrecid}"]
    if start_date or end_date:
        # Zenodo only accepts plain dates in a range query.
        start = start_date.strftime("%Y-%m-%d") if start_date else "*"
        end = end_date.strftime("%Y-%m-%d") if end_date else "*"
        clauses.append(f"metadata.publication_date:[{start} TO {end}]")

    params = urlencode(
        {
            "q": " AND ".join(clauses),
            "all_versions": "true",
            "sort": "mostrecent",
            "size": page_size,
        }
    )
    url = f"https://zenodo.org/api/records?{params}"
    logger.debug(f"Fetching Zenodo records: {url}")

    resp = retry_session().get(
        url,
        timeout=timeout,
        headers={
            "Accept-Encoding": "gzip",
        },
    )
    resp.raise_for_status()
    hits = resp.json().get("hits", {}).get("hits", [])

    records: list[ZenodoRecord] = []
    for hit in hits:
        pub_date_str = hit.get("metadata", {}).get("publication_date")
        if not pub_date_str:
            continue

        try:
            pub_date = pendulum.from_format(pub_date_str, "YYYY-MM-DD", tz="UTC")
        except ValueError:
            logger.warning("Could not parse Zenodo publication_date: %s", pub_date_str)
            continue

        files: list[ZenodoFile] = [
            ZenodoFile(
                link=f.get("links", {}).get("self"),
                file_hash=f.get("checksum"),
                file_name=f.get("key"),
                file_type=f.get("type"),
            )
            for f in hit.get("files", [])
        ]

        records.append(
            ZenodoRecord(
                publication_date=pub_date.date(),
                files=files,
            )
        )

    records.sort(key=lambda r: r.publication_date, reverse=True)
    return records
