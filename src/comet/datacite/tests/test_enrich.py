from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pytest

from comet.aws import TransformTaskContext
from comet.datacite.enrich import (
    enrich_affiliations,
    enrich_funders,
    enrich_resource_type_general,
    select_ror_data_member,
)


class TestEnrichResourceTypeGeneral:
    def test_runs_binary_with_expected_args(self, mocker, tmp_path):
        download_dir = tmp_path / "in"
        transform_dir = tmp_path / "out"
        download_dir.mkdir()
        transform_dir.mkdir()
        ctx = TransformTaskContext(
            download_dir=download_dir,
            transform_dir=transform_dir,
            target_uri="s3://bucket/datacite_enrich_resource_type_general/run-1/",
        )

        captured = {}

        @contextmanager
        def fake_task(source_uri, target_uri, upload_glob):
            captured.update(source_uri=source_uri, target_uri=target_uri, upload_glob=upload_glob)
            yield ctx

        rules_local = tmp_path / "cfg" / "rules.yaml"
        enrichment_local = tmp_path / "cfg" / "metadata.yaml"
        mock_download = mocker.patch(
            "comet.datacite.enrich.download_config", side_effect=[rules_local, enrichment_local]
        )
        mock_run_process = mocker.patch("comet.datacite.enrich.run_process")

        with patch("comet.datacite.enrich.transform_task", fake_task):
            enrich_resource_type_general(
                input_uri="s3://bucket/datacite_ingest/src-run/",
                output_uri="s3://bucket/datacite_enrich_resource_type_general/run-1/",
                rules_uri="s3://bucket/enrichment-configs/resource-type-general-reclassification-rules.yaml",
                enrichment_uri="s3://bucket/enrichment-configs/resource-type-general-enrichment-metadata.yaml",
            )

        assert captured["source_uri"] == "s3://bucket/datacite_ingest/src-run/"
        assert captured["target_uri"] == "s3://bucket/datacite_enrich_resource_type_general/run-1/"
        assert captured["upload_glob"] == "enrichments.jsonl"

        assert [c.args[0] for c in mock_download.call_args_list] == [
            "s3://bucket/enrichment-configs/resource-type-general-reclassification-rules.yaml",
            "s3://bucket/enrichment-configs/resource-type-general-enrichment-metadata.yaml",
        ]
        mock_run_process.assert_called_once()
        (cmd,), _ = mock_run_process.call_args
        assert cmd == [
            "comet-enrich-datacite-resource-type-general",
            "--input",
            str(download_dir),
            "--output",
            str(transform_dir / "enrichments.jsonl"),
            "--rules",
            str(rules_local),
            "--enrichment",
            str(enrichment_local),
        ]


class TestRorEnrichers:
    """The funders and affiliations enrichers share the same datacite-ror extract/query/reconcile
    pipeline; only the binary name and Marple task differ."""

    @pytest.mark.parametrize(
        "enrich_fn, dag_name, binary, task",
        [
            (enrich_funders, "datacite_enrich_funders", "comet-enrich-datacite-funders", "funder"),
            (
                enrich_affiliations,
                "datacite_enrich_affiliations",
                "comet-enrich-datacite-affiliations",
                "affiliation",
            ),
        ],
    )
    def test_runs_extract_query_reconcile_in_order(self, mocker, tmp_path, enrich_fn, dag_name, binary, task):
        download_dir = tmp_path / "in"
        transform_dir = tmp_path / "out"
        download_dir.mkdir()
        transform_dir.mkdir()
        ctx = TransformTaskContext(
            download_dir=download_dir,
            transform_dir=transform_dir,
            target_uri=f"s3://bucket/{dag_name}/run-1/",
        )

        captured = {}

        @contextmanager
        def fake_task(source_uri, target_uri, upload_glob):
            captured.update(source_uri=source_uri, target_uri=target_uri, upload_glob=upload_glob)
            yield ctx

        ror_data = tmp_path / "ror" / "v1.58-2026-ror-data_schema_v2.json"
        config_local = tmp_path / "cfg" / "enrichment-config.yaml"
        mocker.patch("comet.datacite.enrich.prepare_ror_data", return_value=ror_data)
        mock_download = mocker.patch("comet.datacite.enrich.download_config", return_value=config_local)
        mock_run_process = mocker.patch("comet.datacite.enrich.run_process")

        with patch("comet.datacite.enrich.transform_task", fake_task):
            enrich_fn(
                input_uri="s3://bucket/datacite_ingest/src-run/",
                output_uri=f"s3://bucket/{dag_name}/run-1/",
                ror_data_uri="s3://bucket/ror_ingest/ror-run/ror.zip",
                enrichment_config_uri="s3://bucket/enrichment-configs/enrichment-config.yaml",
            )

        assert captured["source_uri"] == "s3://bucket/datacite_ingest/src-run/"
        assert captured["target_uri"] == f"s3://bucket/{dag_name}/run-1/"
        assert captured["upload_glob"] == "enrichments.jsonl"
        mock_download.assert_called_once_with("s3://bucket/enrichment-configs/enrichment-config.yaml")

        commands = [call.args[0] for call in mock_run_process.call_args_list]
        assert commands == [
            [binary, "extract", "--input", str(download_dir), "--output", str(transform_dir)],
            [
                binary,
                "query",
                "--input",
                str(transform_dir),
                "--output",
                str(transform_dir),
                "--base-url",
                "http://localhost:8000",
                "--task",
                task,
            ],
            [
                binary,
                "reconcile",
                "--input",
                str(transform_dir),
                "--output",
                str(transform_dir / "enrichments.jsonl"),
                "--ror-data",
                str(ror_data),
                "--enrichment-format",
                "--enrichment-config",
                str(config_local),
            ],
        ]


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
