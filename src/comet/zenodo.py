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
    start_date: pendulum.DateTime | None = None,
    end_date: pendulum.DateTime,
    page_size: int = 10,
    timeout: float = 30.0,
) -> list[ZenodoRecord]:
    """Fetch all Zenodo record versions for a concept within an optional date range.

    Args:
        conceptrecid: Zenodo concept record ID shared across all versions.
        start_date: Exclusive lower bound on publication date (records strictly after
            this date); no lower bound if None.
        end_date: Inclusive upper bound on publication date.
        page_size: Number of records to request per API page.
        timeout: HTTP request timeout in seconds.

    Returns:
        List of ZenodoRecord objects whose publication dates fall within the given range.
    """
    records: list[ZenodoRecord] = []
    page = 1

    start_bound = start_date.start_of("day") if start_date else None
    end_bound = end_date.end_of("day")

    while True:
        params = urlencode(
            {
                "q": f"conceptrecid:{conceptrecid}",
                "all_versions": "true",
                "sort": "mostrecent",
                "page": page,
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
        data = resp.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        stop_paging = False

        for hit in hits:
            pub_date_str = hit.get("metadata", {}).get("publication_date")
            if not pub_date_str:
                continue

            try:
                pub_date = pendulum.from_format(pub_date_str, "YYYY-MM-DD", tz="UTC")
            except ValueError:
                logger.warning("Could not parse Zenodo publication_date: %s", pub_date_str)
                continue

            if pub_date > end_bound:
                continue

            if start_bound and pub_date <= start_bound:
                stop_paging = True
                break

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

        if stop_paging or len(hits) < page_size:
            break

        page += 1

    records.sort(key=lambda r: r.publication_date, reverse=True)
    return records
