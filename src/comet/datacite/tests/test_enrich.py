from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pytest

from comet.aws import TransformTaskContext
from comet.datacite.enrich import (
    UPLOAD_EXCLUDE_PATTERNS,
    UPLOAD_GLOB,
    enrich_affiliations,
    enrich_funders,
    enrich_resource_type_general,
    select_ror_data_member,
)


def transform_context(tmp_path, dag_name: str) -> TransformTaskContext:
    download_dir = tmp_path / "in"
    transform_dir = tmp_path / "out"
    download_dir.mkdir()
    transform_dir.mkdir()
    return TransformTaskContext(
        download_dir=download_dir,
        transform_dir=transform_dir,
        target_uri=f"s3://bucket/{dag_name}/run-1/",
    )


def fake_transform_task(ctx: TransformTaskContext, captured: dict):
    @contextmanager
    def fake_task(source_uri, target_uri, upload_glob, upload_exclude_patterns=()):
        captured.update(
            source_uri=source_uri,
            target_uri=target_uri,
            upload_glob=upload_glob,
            upload_exclude_patterns=upload_exclude_patterns,
        )
        yield ctx

    return fake_task


class TestEnrichResourceTypeGeneral:
    def test_runs_unified_binary_once(self, mocker, tmp_path):
        ctx = transform_context(tmp_path, "datacite_enrich_resource_type_general")
        captured = {}
        rules_local = tmp_path / "cfg" / "rules.yaml"
        provenance_local = tmp_path / "cfg" / "provenance.yaml"
        mock_download = mocker.patch(
            "comet.datacite.enrich.download_config", side_effect=[rules_local, provenance_local]
        )
        mock_run_process = mocker.patch("comet.datacite.enrich.run_process")

        with patch("comet.datacite.enrich.transform_task", fake_transform_task(ctx, captured)):
            enrich_resource_type_general(
                input_uri="s3://bucket/datacite_ingest/src-run/",
                output_uri=ctx.target_uri,
                source_release_date=["datacite=2026-01-02"],
                rules_uri="s3://bucket/enrichment-configs/resource-type-general-reclassification-rules.yaml",
                provenance_uri="s3://bucket/enrichment-configs/resource-type-general-provenance.yaml",
            )

        assert captured == {
            "source_uri": "s3://bucket/datacite_ingest/src-run/",
            "target_uri": ctx.target_uri,
            "upload_glob": UPLOAD_GLOB,
            "upload_exclude_patterns": UPLOAD_EXCLUDE_PATTERNS,
        }
        assert [call.args[0] for call in mock_download.call_args_list] == [
            "s3://bucket/enrichment-configs/resource-type-general-reclassification-rules.yaml",
            "s3://bucket/enrichment-configs/resource-type-general-provenance.yaml",
        ]
        mock_run_process.assert_called_once_with(
            [
                "comet-enrich",
                "resource-type-general",
                "--input",
                str(ctx.download_dir),
                "--output",
                str(ctx.transform_dir),
                "--rules",
                str(rules_local),
                "--provenance",
                str(provenance_local),
                "--source-release-date",
                "datacite=2026-01-02",
                "--output-writer-lanes",
                "1",
            ]
        )


class TestRorEnrichers:
    @pytest.mark.parametrize(
        "enrich, subcommand, uses_ror_file, writer_lanes",
        [
            (enrich_affiliations, "affiliations", False, 32),
            (enrich_funders, "funders", True, 1),
        ],
        ids=["affiliations", "funders"],
    )
    def test_runs_unified_binary_and_cleans_up_ror_data(
        self, mocker, tmp_path, enrich, subcommand, uses_ror_file, writer_lanes
    ):
        ctx = transform_context(tmp_path, f"datacite_enrich_{subcommand}")
        captured = {}
        provenance_local = tmp_path / "cfg" / "provenance.yaml"
        provenance_uri = f"s3://bucket/enrichment-configs/{subcommand}-provenance.yaml"
        mock_download = mocker.patch("comet.datacite.enrich.download_config", return_value=provenance_local)
        mock_run_process = mocker.patch("comet.datacite.enrich.run_process")
        kwargs = dict(
            input_uri="s3://bucket/datacite_ingest/src-run/",
            output_uri=ctx.target_uri,
            source_release_date=["datacite=2026-01-02", "ror=2026-01-15"],
            provenance_uri=provenance_uri,
            ror_service_url="http://ror-service:8000",
            output_writer_lanes=writer_lanes,
        )
        ror_file_args = []
        if uses_ror_file:
            ror_data = tmp_path / "ror" / "v1.58-2026-ror-data_schema_v2.json"
            ror_data.parent.mkdir()
            ror_data.write_text("[]")
            mocker.patch("comet.datacite.enrich.prepare_ror_data", return_value=ror_data)
            kwargs["ror_data_uri"] = "s3://bucket/ror_ingest/ror-run/ror.zip"
            ror_file_args = ["--ror-file", str(ror_data)]

        with patch("comet.datacite.enrich.transform_task", fake_transform_task(ctx, captured)):
            enrich(**kwargs)

        assert captured["upload_glob"] == UPLOAD_GLOB
        assert captured["upload_exclude_patterns"] == UPLOAD_EXCLUDE_PATTERNS
        mock_download.assert_called_once_with(provenance_uri)
        mock_run_process.assert_called_once_with(
            [
                "comet-enrich",
                subcommand,
                "--input",
                str(ctx.download_dir),
                "--output",
                str(ctx.transform_dir),
                "--provenance",
                str(provenance_local),
                "--source-release-date",
                "datacite=2026-01-02",
                "--source-release-date",
                "ror=2026-01-15",
                *ror_file_args,
                "--ror-service-url",
                "http://ror-service:8000",
                "--output-writer-lanes",
                str(writer_lanes),
            ]
        )
        if uses_ror_file:
            assert not ror_data.parent.exists()


class TestSelectRorDataMember:
    @pytest.mark.parametrize(
        "members, expected",
        [
            (["v1.58-2026-ror-data.json", "v1.58-2026-ror-data_schema_v2.json"], "v1.58-2026-ror-data_schema_v2.json"),
            (["v1.50-2024-ror-data.json"], "v1.50-2024-ror-data.json"),
        ],
    )
    def test_selects_expected_member(self, members, expected):
        assert select_ror_data_member(members) == expected

    def test_raises_when_no_ror_dump_present(self):
        with pytest.raises(ValueError, match="No ROR data JSON"):
            select_ror_data_member(["README.md", "relationships.json"])
