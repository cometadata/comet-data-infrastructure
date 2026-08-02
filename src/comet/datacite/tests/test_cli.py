"""Tests for the comet datacite CLI."""

from __future__ import annotations

import pytest

from comet.datacite.cli import datacite_app


class TestDataciteDownloadCommand:
    def test_invokes_download_datacite_with_parsed_args(self, mocker):
        mock_download = mocker.patch("comet.datacite.datacite.download_datacite")
        mocker.patch("comet.cli.setup_logging")

        # cyclopts calls sys.exit(0) once the command returns.
        with pytest.raises(SystemExit) as exc_info:
            datacite_app(
                [
                    "download",
                    "--target-uri",
                    "s3://my-bucket/datacite_ingest/run-1/",
                    "--datacite-bucket-name",
                    "datacite-source",
                    "--datacite-bucket-region",
                    "eu-west-1",
                    "--expected-file-count",
                    "350",
                    "--expected-total-bytes",
                    "1200",
                ]
            )
        assert exc_info.value.code == 0

        mock_download.assert_called_once_with(
            target_uri="s3://my-bucket/datacite_ingest/run-1/",
            datacite_bucket_name="datacite-source",
            datacite_bucket_region="eu-west-1",
            expected_file_count=350,
            expected_total_bytes=1200,
        )


class TestDataciteEnrichResourceTypeGeneralCommand:
    def test_invokes_enrich_with_parsed_args(self, mocker):
        mock_enrich = mocker.patch("comet.datacite.enrich.enrich_resource_type_general")
        mocker.patch("comet.cli.setup_logging")

        with pytest.raises(SystemExit) as exc_info:
            datacite_app(
                [
                    "enrich",
                    "resource-type-general",
                    "--input-uri",
                    "s3://my-bucket/datacite_ingest/src-run/",
                    "--output-uri",
                    "s3://my-bucket/datacite_enrich_resource_type_general/run-1/",
                    "--rules-uri",
                    "s3://my-bucket/enrichment-configs/resource-type-general-reclassification-rules.yaml",
                    "--provenance-uri",
                    "s3://my-bucket/enrichment-configs/resource-type-general-provenance.yaml",
                ]
            )
        assert exc_info.value.code == 0

        mock_enrich.assert_called_once_with(
            input_uri="s3://my-bucket/datacite_ingest/src-run/",
            output_uri="s3://my-bucket/datacite_enrich_resource_type_general/run-1/",
            rules_uri="s3://my-bucket/enrichment-configs/resource-type-general-reclassification-rules.yaml",
            provenance_uri="s3://my-bucket/enrichment-configs/resource-type-general-provenance.yaml",
            output_writer_lanes=1,
        )


class TestDataciteEnrichFundersCommand:
    def test_invokes_enrich_with_parsed_args(self, mocker):
        mock_enrich = mocker.patch("comet.datacite.enrich.enrich_funders")
        mocker.patch("comet.cli.setup_logging")

        with pytest.raises(SystemExit) as exc_info:
            datacite_app(
                [
                    "enrich",
                    "funders",
                    "--input-uri",
                    "s3://my-bucket/datacite_ingest/src-run/",
                    "--output-uri",
                    "s3://my-bucket/datacite_enrich_funders/run-1/",
                    "--ror-data-uri",
                    "s3://my-bucket/ror_ingest/ror-run/ror.zip",
                    "--provenance-uri",
                    "s3://my-bucket/enrichment-configs/funders-provenance.yaml",
                ]
            )
        assert exc_info.value.code == 0

        mock_enrich.assert_called_once_with(
            input_uri="s3://my-bucket/datacite_ingest/src-run/",
            output_uri="s3://my-bucket/datacite_enrich_funders/run-1/",
            ror_data_uri="s3://my-bucket/ror_ingest/ror-run/ror.zip",
            provenance_uri="s3://my-bucket/enrichment-configs/funders-provenance.yaml",
            ror_service_url="http://localhost:8000",
            output_writer_lanes=1,
        )


class TestDataciteEnrichAffiliationsCommand:
    def test_invokes_enrich_with_parsed_args(self, mocker):
        mock_enrich = mocker.patch("comet.datacite.enrich.enrich_affiliations")
        mocker.patch("comet.cli.setup_logging")

        with pytest.raises(SystemExit) as exc_info:
            datacite_app(
                [
                    "enrich",
                    "affiliations",
                    "--input-uri",
                    "s3://my-bucket/datacite_ingest/src-run/",
                    "--output-uri",
                    "s3://my-bucket/datacite_enrich_affiliations/run-1/",
                    "--provenance-uri",
                    "s3://my-bucket/enrichment-configs/affiliations-provenance.yaml",
                    "--output-writer-lanes",
                    "32",
                ]
            )
        assert exc_info.value.code == 0

        mock_enrich.assert_called_once_with(
            input_uri="s3://my-bucket/datacite_ingest/src-run/",
            output_uri="s3://my-bucket/datacite_enrich_affiliations/run-1/",
            provenance_uri="s3://my-bucket/enrichment-configs/affiliations-provenance.yaml",
            ror_service_url="http://localhost:8000",
            output_writer_lanes=32,
        )
