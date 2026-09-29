from __future__ import annotations

from pathlib import Path

import pendulum
import pytest
import vcr

from comet.zenodo import list_zenodo_records

FIXTURES_DIR = Path(__file__).parent / "fixtures"
ROR_ZENODO_CASSETTE = FIXTURES_DIR / "ror_zenodo.yaml"


class TestListZenodoRecords:
    @pytest.mark.parametrize(
        ("start_date", "end_date", "expected_dates"),
        [
            (pendulum.date(2025, 8, 7), pendulum.date(2025, 8, 7), ["2025-08-07"]),
            (pendulum.date(2025, 8, 8), pendulum.date(2025, 8, 8), []),
            (pendulum.date(2026, 8, 3), None, ["2026-09-22", "2026-08-25", "2026-08-03"]),
        ],
        ids=["single-day", "single-day-no-release", "open-end"],
    )
    def test_filters_by_publication_date(self, start_date, end_date, expected_dates):
        with vcr.use_cassette(str(ROR_ZENODO_CASSETTE), decode_compressed_response=False):
            records = list_zenodo_records(conceptrecid=6347574, start_date=start_date, end_date=end_date)

        assert [record.publication_date.isoformat() for record in records] == expected_dates
