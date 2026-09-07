import contextlib

from comet import diff


class TestDiffEnrichments:
    def test_downloads_both_full_directories_runs_diff_and_uploads(self, mocker, tmp_path):
        stage = tmp_path / "stage"

        @contextlib.contextmanager
        def fake_scratch(target_uri):
            assert target_uri == "s3://bucket/dag/run-2/diff/"
            stage.mkdir(parents=True, exist_ok=True)
            yield stage

        mocker.patch("comet.diff.staged_scratch_dir", fake_scratch)
        mock_download = mocker.patch("comet.diff.s5cmd_download_files")
        mock_run = mocker.patch("comet.diff.run_process")
        mock_upload = mocker.patch("comet.diff.s5cmd_upload_files")

        diff.diff_enrichments(
            old_uri="s3://bucket/dag/run-1/full/",
            new_uri="s3://bucket/dag/run-2/full/",
            output_uri="s3://bucket/dag/run-2/diff/",
        )

        assert mock_download.call_args_list == [
            mocker.call("s3://bucket/dag/run-1/full/*", stage / "old"),
            mocker.call("s3://bucket/dag/run-2/full/*", stage / "new"),
        ]
        mock_run.assert_called_once_with(
            [
                "comet-enrich",
                "diff",
                "--old",
                str(stage / "old"),
                "--new",
                str(stage / "new"),
                "--output",
                str(stage / "diff"),
            ]
        )
        mock_upload.assert_called_once_with(stage / "diff", "s3://bucket/dag/run-2/diff/")
