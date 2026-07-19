from __future__ import annotations

import pytest

from comet.aws import (
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    download_source_task,
    local_dir_for_uri,
    local_file_for_uri,
    transform_task,
    upload_files_to_s3,
)


@pytest.fixture
def scratch_root(mocker, tmp_path):
    root = tmp_path / "data"
    mocker.patch("comet.aws.local_path", side_effect=lambda *parts: root.joinpath(*parts))
    return root


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


class TestLocalDirForUri:
    def test_maps_nested_prefix_below_scratch_root(self, scratch_root):
        result = local_dir_for_uri("s3://bucket/datacite_ingest/run-1/")

        assert result == scratch_root / "datacite_ingest" / "run-1"

    @pytest.mark.parametrize(
        "uri",
        [
            "s3://bucket",
            "s3://bucket/",
            "s3://bucket/./",
            "s3://bucket/../stage/",
            "s3://bucket/jobs/../stage/",
            "s3://bucket/jobs/../",
        ],
    )
    def test_rejects_empty_root_or_traversing_prefix(self, scratch_root, uri):
        with pytest.raises(ValueError, match="Unsafe scratch path"):
            local_dir_for_uri(uri)

    def test_allows_dots_inside_a_path_component(self, scratch_root):
        result = local_dir_for_uri("s3://bucket/jobs/run..1/")

        assert result == scratch_root / "jobs" / "run..1"

    def test_rejects_path_through_symlink_outside_scratch_root(self, scratch_root, tmp_path):
        scratch_root.mkdir()
        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        (scratch_root / "escape").symlink_to(outside_dir, target_is_directory=True)

        with pytest.raises(ValueError, match="Unsafe scratch path"):
            local_dir_for_uri("s3://bucket/escape/stage/")


class TestLocalFileForUri:
    def test_maps_key_filename_into_work_dir(self, tmp_path):
        result = local_file_for_uri("s3://bucket/configs/rules.json", tmp_path)

        assert result == tmp_path / "rules.json"

    @pytest.mark.parametrize(
        "uri",
        [
            "s3://bucket",
            "s3://bucket/",
            "s3://bucket/..",
            "s3://bucket/configs/..",
        ],
    )
    def test_rejects_uri_without_a_filename(self, tmp_path, uri):
        with pytest.raises(ValueError, match="Cannot derive a local filename"):
            local_file_for_uri(uri, tmp_path)


class TestDownloadSourceTask:
    def test_yields_context_and_uploads_then_cleans(self, mocker, scratch_root):
        mock_clean = mocker.patch("comet.aws.clean_s3_prefix")
        mock_upload = mocker.patch("comet.aws.upload_files_to_s3")

        target_uri = "s3://bucket/datacite_ingest/run-1/"
        expected_dir = scratch_root / "datacite_ingest" / "run-1"

        with download_source_task(target_uri) as ctx:
            assert ctx.download_dir == expected_dir
            assert ctx.target_uri == target_uri
            assert ctx.download_dir.exists()
            mock_clean.assert_called_once_with(target_uri)
            mock_upload.assert_not_called()
            (ctx.download_dir / "x.json").write_text("{}")

        mock_upload.assert_called_once_with(expected_dir, target_uri)
        assert not expected_dir.exists()

    @pytest.mark.parametrize("failure", ["body", "upload"])
    def test_cleans_without_partial_upload_when_task_fails(self, mocker, scratch_root, failure):
        mocker.patch("comet.aws.clean_s3_prefix")
        mock_upload = mocker.patch(
            "comet.aws.upload_files_to_s3",
            side_effect=RuntimeError("upload failed") if failure == "upload" else None,
        )
        target_uri = "s3://bucket/datacite_ingest/run-1/"
        expected_dir = scratch_root / "datacite_ingest" / "run-1"

        with pytest.raises(RuntimeError, match=failure):
            with download_source_task(target_uri) as ctx:
                (ctx.download_dir / "partial.json").write_text("{}")
                if failure == "body":
                    raise RuntimeError("body failed")

        if failure == "body":
            mock_upload.assert_not_called()
        assert not expected_dir.exists()


class TestTransformTask:
    def test_downloads_uploads_public_output_tree_then_cleans(self, mocker, scratch_root):
        mock_clean = mocker.patch("comet.aws.clean_s3_prefix")
        mock_download = mocker.patch("comet.aws.download_files_from_s3")
        mock_upload = mocker.patch("comet.aws.upload_files_to_s3")

        source_uri = "s3://bucket/datacite_ingest/src-run/"
        target_uri = "s3://bucket/datacite_enrich_resource_type_general/run-1/"
        stage_dir = scratch_root / "datacite_enrich_resource_type_general" / "run-1"
        expected_download = stage_dir / "download"
        expected_transform = stage_dir / "transform"

        with transform_task(
            source_uri,
            target_uri,
            upload_glob="*",
            upload_exclude_patterns=(".work/*",),
        ) as ctx:
            assert ctx.download_dir == expected_download
            assert ctx.transform_dir == expected_transform
            assert ctx.target_uri == target_uri
            assert ctx.download_dir.exists()
            assert ctx.transform_dir.exists()
            mock_clean.assert_called_once_with(target_uri)
            mock_download.assert_called_once_with(f"{source_uri}*", expected_download)
            mock_upload.assert_not_called()
            (ctx.transform_dir / "manifest.json").write_text("{}")

        mock_upload.assert_called_once_with(expected_transform, target_uri, "*", (".work/*",))
        assert not expected_download.exists()
        assert not expected_transform.exists()

    @pytest.mark.parametrize("failure", ["download", "body", "upload"])
    def test_cleans_without_partial_upload_when_task_fails(self, mocker, scratch_root, failure):
        mocker.patch("comet.aws.clean_s3_prefix")

        def fail_after_partial_download(source_uri, target_dir):
            (target_dir / "partial.jsonl").write_text("{}")
            raise RuntimeError("download failed")

        mocker.patch(
            "comet.aws.download_files_from_s3",
            side_effect=fail_after_partial_download if failure == "download" else None,
        )
        mock_upload = mocker.patch(
            "comet.aws.upload_files_to_s3",
            side_effect=RuntimeError("upload failed") if failure == "upload" else None,
        )
        source_uri = "s3://bucket/datacite_ingest/src-run/"
        target_uri = "s3://bucket/datacite_enrich/run-1/"
        stage_dir = scratch_root / "datacite_enrich" / "run-1"

        with pytest.raises(RuntimeError, match=failure):
            with transform_task(source_uri, target_uri, upload_glob="*") as ctx:
                (ctx.transform_dir / "partial.jsonl").write_text("{}")
                if failure == "body":
                    raise RuntimeError("body failed")

        if failure != "upload":
            mock_upload.assert_not_called()
        assert not stage_dir.exists()


class TestUploadFilesToS3:
    def test_adds_each_exclusion_before_source_and_destination(self, mocker, tmp_path):
        mock_run_process = mocker.patch("comet.aws.run_process")

        upload_files_to_s3(
            tmp_path,
            "s3://bucket/output/",
            "*",
            (".work/*", "*.tmp"),
        )

        mock_run_process.assert_called_once_with(
            [
                "s5cmd",
                "--log",
                "error",
                "--stat",
                "cp",
                "--exclude",
                ".work/*",
                "--exclude",
                "*.tmp",
                f"{tmp_path}/*",
                "s3://bucket/output/",
            ]
        )
