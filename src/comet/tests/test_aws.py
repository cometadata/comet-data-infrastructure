from __future__ import annotations

from datetime import UTC, datetime

import boto3
from moto import mock_aws
import pytest

from comet.aws import (
    batch_job_definition_name,
    batch_job_name,
    batch_job_queue_name,
    delete_s3_prefix,
    download_source_task,
    first_object_timestamp,
    list_run_prefixes,
    s5cmd_clean_prefix,
    local_dir_for_uri,
    local_file_for_uri,
    s5cmd_command,
    transform_task,
    s5cmd_upload_files,
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
        mock_clean = mocker.patch("comet.aws.s5cmd_clean_prefix")
        mock_upload = mocker.patch("comet.aws.s5cmd_upload_files")

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
        mocker.patch("comet.aws.s5cmd_clean_prefix")
        mock_upload = mocker.patch(
            "comet.aws.s5cmd_upload_files",
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
        mock_clean = mocker.patch("comet.aws.s5cmd_clean_prefix")
        mock_download = mocker.patch("comet.aws.s5cmd_download_files")
        mock_upload = mocker.patch("comet.aws.s5cmd_upload_files")

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
        mocker.patch("comet.aws.s5cmd_clean_prefix")

        def fail_after_partial_download(source_uri, target_dir):
            (target_dir / "partial.jsonl").write_text("{}")
            raise RuntimeError("download failed")

        mocker.patch(
            "comet.aws.s5cmd_download_files",
            side_effect=fail_after_partial_download if failure == "download" else None,
        )
        mock_upload = mocker.patch(
            "comet.aws.s5cmd_upload_files",
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


class TestDeleteS3Prefix:
    @pytest.fixture
    def s3(self):
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    def test_deletes_only_objects_under_the_prefix(self, s3):
        for key in ["datacite_ingest/run-1/a", "datacite_ingest/run-1/b", "datacite_ingest/run-2/keep"]:
            s3.put_object(Bucket="test-bucket", Key=key, Body=b"")

        deleted = delete_s3_prefix("test-bucket", "datacite_ingest/run-1/", s3_client=s3)

        assert deleted == 2
        remaining = s3.list_objects_v2(Bucket="test-bucket", Prefix="datacite_ingest/")
        assert [obj["Key"] for obj in remaining["Contents"]] == ["datacite_ingest/run-2/keep"]

    def test_requires_directory_like_prefix_before_deleting(self, s3):
        keys = ["datacite_ingest/run-1/data.json", "datacite_ingest/run-11/data.json"]
        for key in keys:
            s3.put_object(Bucket="test-bucket", Key=key, Body=b"")

        with pytest.raises(ValueError, match="trailing slash"):
            delete_s3_prefix("test-bucket", "datacite_ingest/run-1", s3_client=s3)

        remaining = s3.list_objects_v2(Bucket="test-bucket", Prefix="datacite_ingest/")
        assert [obj["Key"] for obj in remaining["Contents"]] == keys

        deleted = delete_s3_prefix("test-bucket", "datacite_ingest/run-1/", s3_client=s3)

        assert deleted == 1
        remaining = s3.list_objects_v2(Bucket="test-bucket", Prefix="datacite_ingest/")
        assert [obj["Key"] for obj in remaining["Contents"]] == ["datacite_ingest/run-11/data.json"]

    def test_issues_one_delete_call_per_list_page(self, mocker):
        pages = [
            {"Contents": [{"Key": f"run-1/part_{i}"} for i in range(3)]},
            {"Contents": [{"Key": f"run-1/part_{i}"} for i in range(3, 5)]},
        ]
        client = mocker.Mock()
        client.get_paginator.return_value.paginate.return_value = pages
        client.delete_objects.return_value = {}

        deleted = delete_s3_prefix("test-bucket", "run-1/", s3_client=client)

        assert deleted == 5
        batches = [len(call.kwargs["Delete"]["Objects"]) for call in client.delete_objects.call_args_list]
        assert batches == [3, 2]

    def test_dry_run_counts_without_deleting(self, s3, mocker):
        s3.put_object(Bucket="test-bucket", Key="ror_ingest/run-1/ror.zip", Body=b"")
        spy = mocker.spy(s3, "delete_objects")

        deleted = delete_s3_prefix("test-bucket", "ror_ingest/run-1/", s3_client=s3, dry_run=True)

        assert deleted == 1
        spy.assert_not_called()
        assert s3.list_objects_v2(Bucket="test-bucket", Prefix="ror_ingest/")["KeyCount"] == 1

    def test_returns_zero_for_prefix_without_objects(self, s3, mocker):
        spy = mocker.spy(s3, "delete_objects")

        assert delete_s3_prefix("test-bucket", "datacite_ingest/gone/", s3_client=s3) == 0
        spy.assert_not_called()

    @pytest.mark.parametrize("prefix", ["", "/", "  ", "//"])
    def test_rejects_empty_prefix(self, prefix):
        with pytest.raises(ValueError, match="empty prefix"):
            delete_s3_prefix("test-bucket", prefix)


class TestListRunPrefixes:
    def test_discovers_run_prefixes_only_below_configured_dags(self, mocker):
        paginator = mocker.Mock()
        paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "datacite_ingest/scheduled__2026-01-01T00:00:00+00:00/"}]},
            {"CommonPrefixes": [{"Prefix": "datacite_ingest/manual__2026-02-01T00:00:00+00:00_xyz/"}]},
        ]
        client = mocker.Mock()
        client.get_paginator.return_value = paginator

        discovered = list_run_prefixes("data-bucket", ["datacite_ingest"], s3_client=client)

        assert discovered == {
            "datacite_ingest/scheduled__2026-01-01T00:00:00+00:00/",
            "datacite_ingest/manual__2026-02-01T00:00:00+00:00_xyz/",
        }
        paginator.paginate.assert_called_once_with(
            Bucket="data-bucket",
            Prefix="datacite_ingest/",
            Delimiter="/",
        )


class TestFirstObjectTimestamp:
    def test_uses_the_first_object_date_for_an_untracked_run(self, mocker):
        last_modified = datetime(2026, 2, 1, tzinfo=UTC)
        client = mocker.Mock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": "datacite_ingest/run-1/part-0001", "LastModified": last_modified}]
        }

        timestamp = first_object_timestamp(
            "data-bucket",
            "datacite_ingest/run-1/",
            s3_client=client,
        )

        assert timestamp == last_modified
        client.list_objects_v2.assert_called_once_with(
            Bucket="data-bucket",
            Prefix="datacite_ingest/run-1/",
            MaxKeys=1,
        )


class TestS5cmdCommand:
    @pytest.mark.parametrize(
        "endpoint_url,expected",
        [
            (None, ["s5cmd", "--log", "error", "--stat", "cp", "src", "dst"]),
            (
                "https://s3.hf.co",
                ["s5cmd", "--log", "error", "--stat", "--endpoint-url", "https://s3.hf.co", "cp", "src", "dst"],
            ),
        ],
    )
    def test_places_endpoint_flag_before_subcommand(self, endpoint_url, expected):
        assert s5cmd_command("cp", "src", "dst", endpoint_url=endpoint_url) == expected


class TestS5cmdCleanPrefix:
    @pytest.mark.parametrize("uri", ["s3://test-bucket", "s3://test-bucket/", "s3://test-bucket/prefix"])
    def test_refuses_unsafe_prefixes(self, uri, mocker):
        run = mocker.patch("comet.aws.run_process")

        with pytest.raises(ValueError, match="Refusing to delete"):
            s5cmd_clean_prefix(uri)

        run.assert_not_called()


class TestUploadFilesToS3:
    def test_adds_each_exclusion_before_source_and_destination(self, mocker, tmp_path):
        mock_run_process = mocker.patch("comet.aws.run_process")

        s5cmd_upload_files(
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
            ],
            env=None,
        )
