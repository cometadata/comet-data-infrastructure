from __future__ import annotations

from pathlib import Path

import pendulum
import vcr

from comet.zenodo import list_zenodo_records

FIXTURES_DIR = Path(__file__).parent / "fixtures"
ROR_ZENODO_CASSETTE = FIXTURES_DIR / "ror_zenodo.yaml"


class TestListZenodoRecords:
    def test_sorted_descending(self):
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE), decode_compressed_response=False):
            records = list_zenodo_records(conceptrecid=6347574, end_date=pendulum.now("UTC"))

        assert len(records) > 0
        dates = [r.publication_date for r in records]
        assert dates == sorted(dates, reverse=True)

    def test_start_date_excludes_boundary_and_older(self):
        # 2025-08-07 is a real record date in the cassette.
        start_date = pendulum.datetime(2025, 8, 7, tz="UTC")
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE), decode_compressed_response=False):
            filtered = list_zenodo_records(
                conceptrecid=6347574,
                start_date=start_date,
                end_date=pendulum.now("UTC"),
            )

        assert filtered
        assert all(r.publication_date > start_date.date() for r in filtered)
