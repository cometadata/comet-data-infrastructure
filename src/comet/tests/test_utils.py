from __future__ import annotations

import logging
import subprocess

import pytest

from comet.utils import get_env, get_region, run_process


class TestEnvHelpers:
    @pytest.mark.parametrize(
        "func, var",
        [
            (get_env, "COMET_ENV"),
            (get_region, "AWS_REGION"),
        ],
    )
    def test_returns_value_when_set(self, monkeypatch, func, var):
        monkeypatch.setenv(var, "value")
        assert func() == "value"

    @pytest.mark.parametrize(
        "func, var",
        [
            (get_env, "COMET_ENV"),
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
