from __future__ import annotations

import pathlib

import pytest

from comet.aws import (
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    download_source_task,
    transform_task,
)


class TestBatchNameBuilders:
    @pytest.mark.parametrize(
        "func, args, expected",
        [
            (batch_job_name, ("dev", "download-datacite"), "comet-dev-download-datacite"),
            (batch_job_queue_name, ("dev", "download"), "comet-dev-batch-download-job-queue"),
            (batch_job_definition_name, ("dev", "download-datacite"), "comet-dev-download-datacite-job"),
        ],
    )
    def test_builds_name_matching_cloudformation_convention(self, func, args, expected):
        assert func(*args) == expected


class TestDownloadSourceTask:
    def test_yields_context_and_uploads_then_cleans(self, mocker, tmp_path):
        def fake_local_path(*parts):
            return pathlib.Path(tmp_path, *parts)

        mocker.patch("comet.aws.local_path", side_effect=fake_local_path)
        mock_clean = mocker.patch("comet.aws.clean_s3_prefix")
        mock_upload = mocker.patch("comet.aws.upload_files_to_s3")

        target_uri = "s3://bucket/datacite_ingest/run-1/"
        expected_dir = tmp_path / "datacite_ingest" / "run-1"

        with download_source_task(target_uri) as ctx:
            assert ctx.download_dir == expected_dir
            assert ctx.target_uri == target_uri
            assert ctx.download_dir.exists()
            mock_clean.assert_called_once_with(target_uri)
            mock_upload.assert_not_called()
            (ctx.download_dir / "x.json").write_text("{}")

        mock_upload.assert_called_once_with(expected_dir, target_uri)
        assert not expected_dir.exists()


class TestTransformTask:
    def test_downloads_runs_uploads_named_output_then_cleans(self, mocker, tmp_path):
        def fake_local_path(*parts):
            return pathlib.Path(tmp_path, *parts)

        mocker.patch("comet.aws.local_path", side_effect=fake_local_path)
        mock_clean = mocker.patch("comet.aws.clean_s3_prefix")
        mock_download = mocker.patch("comet.aws.download_files_from_s3")
        mock_upload = mocker.patch("comet.aws.upload_files_to_s3")

        source_uri = "s3://bucket/datacite_ingest/src-run/"
        target_uri = "s3://bucket/datacite_enrich_resource_type_general/run-1/"
        stage_dir = tmp_path / "datacite_enrich_resource_type_general" / "run-1"
        expected_download = stage_dir / "download"
        expected_transform = stage_dir / "transform"

        with transform_task(source_uri, target_uri, upload_glob="enrichments.jsonl") as ctx:
            assert ctx.download_dir == expected_download
            assert ctx.transform_dir == expected_transform
            assert ctx.target_uri == target_uri
            assert ctx.download_dir.exists()
            assert ctx.transform_dir.exists()
            mock_clean.assert_called_once_with(target_uri)
            mock_download.assert_called_once_with(f"{source_uri}*", expected_download)
            mock_upload.assert_not_called()
            (ctx.transform_dir / "enrichments.jsonl").write_text("{}")

        mock_upload.assert_called_once_with(expected_transform, target_uri, "enrichments.jsonl")
        assert not expected_download.exists()
        assert not expected_transform.exists()
