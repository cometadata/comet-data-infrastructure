from __future__ import annotations

import gzip
import json
import logging
import subprocess
import zipfile

import pytest

from comet.utils import extract_zip_to_gzip, get_env, get_region, run_process


class TestEnvHelpers:
    @pytest.mark.parametrize(
        "func, var",
        [
            (get_env, "AWS_ENV"),
            (get_region, "AWS_REGION"),
        ],
    )
    def test_returns_value_when_set(self, monkeypatch, func, var):
        monkeypatch.setenv(var, "value")
        assert func() == "value"

    @pytest.mark.parametrize(
        "func, var",
        [
            (get_env, "AWS_ENV"),
            (get_region, "AWS_REGION"),
        ],
    )
    def test_raises_when_unset_or_empty(self, monkeypatch, func, var):
        monkeypatch.setenv(var, "")
        with pytest.raises(RuntimeError, match=var):
            func()


class TestRunProcess:
    def test_streams_stdout_to_logger(self, caplog):
        caplog.set_level(logging.INFO, logger="comet.utils")
        run_process(["echo", "hello world"])
        assert "hello world" in caplog.text
        assert "run_process command: `echo 'hello world'`" in caplog.text

    def test_raises_on_nonzero_exit(self):
        with pytest.raises(subprocess.CalledProcessError):
            run_process(["false"])


class TestExtractZipToGzip:
    def test_extracts_json_files_and_skips_others(self, tmp_path):
        zip_path = tmp_path / "archive.zip"
        payload = {"id": "ror-1", "name": "Example"}
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("ror.json", json.dumps(payload))
            zf.writestr("README.txt", "ignore me")

        out_paths = extract_zip_to_gzip(zip_path)

        assert len(out_paths) == 1
        assert out_paths[0].name == "ror.json.gz"
        with gzip.open(out_paths[0], "rt") as f:
            assert json.load(f) == payload

    def test_returns_empty_when_no_json(self, tmp_path):
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("README.txt", "no json here")

        assert extract_zip_to_gzip(zip_path) == []
