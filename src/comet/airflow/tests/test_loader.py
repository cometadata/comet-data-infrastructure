from __future__ import annotations

import logging
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest
import yaml

from comet.airflow.loader import DagLoadError, load_dags, resolve_factory

FIXTURES = Path(__file__).parent / "fixtures"
VALID = FIXTURES / "valid.yaml"
MIXED = FIXTURES / "mixed_good_and_broken.yaml"
MALFORMED = FIXTURES / "malformed.yaml"


def make_globals(tmp_path: Path, fixture: Path | None) -> dict:
    """Stage a fixture as dags.yaml next to a fake __file__ for the loader."""
    if fixture is not None:
        shutil.copy(fixture, tmp_path / "dags.yaml")
    return {"__file__": str(tmp_path / "dags.py")}


class TestLoadDags:
    def test_registers_enabled_dags_into_globals(self, tmp_path):
        g = make_globals(tmp_path, VALID)
        load_dags(g)
        assert g["alpha"] == {"dag_id": "alpha", "foo": "hello"}

    def test_skips_disabled_entries(self, tmp_path):
        g = make_globals(tmp_path, VALID)
        load_dags(g)
        assert "beta" not in g

    def test_skips_silently_when_yaml_missing(self, tmp_path, caplog):
        g = make_globals(tmp_path, fixture=None)
        with caplog.at_level(logging.WARNING, logger="comet.airflow.loader"):
            load_dags(g)
        assert any("no dags.yaml" in r.getMessage() for r in caplog.records)

    def test_malformed_yaml_propagates(self, tmp_path):
        g = make_globals(tmp_path, MALFORMED)
        with pytest.raises(yaml.YAMLError):
            load_dags(g)

    def test_failures_aggregate_into_one_exception_group(self, tmp_path):
        g = make_globals(tmp_path, MIXED)
        with pytest.raises(DagLoadError) as exc_info:
            load_dags(g)

        assert g["good_one"] == {"dag_id": "good_one", "foo": "ok"}
        assert g["good_two"] == {"dag_id": "good_two", "foo": "also-ok"}
        for bad in ("bad_import", "bad_params", "bad_build"):
            assert bad not in g

        eg = exc_info.value
        assert len(eg.exceptions) == 4
        for failed in ("bad_import", "bad_params", "bad_build"):
            assert failed in eg.message
        notes = [n for sub in eg.exceptions for n in getattr(sub, "__notes__", [])]
        assert any("dag_id=bad_import" in n for n in notes)

    def test_parsing_context_filters_to_single_dag(self, tmp_path, monkeypatch, caplog):
        # Pretend Airflow is parsing a single DAG.
        g = make_globals(tmp_path, MIXED)
        monkeypatch.setattr(
            "comet.airflow.loader.get_parsing_context",
            lambda: SimpleNamespace(dag_id="good_two"),
        )
        with caplog.at_level(logging.ERROR, logger="comet.airflow.loader"):
            load_dags(g)

        assert g["good_two"] == {"dag_id": "good_two", "foo": "also-ok"}
        for filtered in ("good_one", "bad_import", "bad_params", "bad_build"):
            assert filtered not in g
        assert not any(r.levelno == logging.ERROR for r in caplog.records)


class TestResolveFactory:
    @pytest.mark.parametrize(
        "dotted_path, expected_exception",
        [
            ("definitely_not_a_real_package_xyz.foo", ImportError),
            ("comet.does_not_exist.foo", AttributeError),
            (
                "comet.airflow.tests.fixtures.factories.nonexistent_attr",
                AttributeError,
            ),
            (
                "comet.airflow.tests.fixtures.factories.not_callable",
                TypeError,
            ),
            (
                "comet.airflow.tests.fixtures.factories.wrong_signature",
                TypeError,
            ),
            (
                "comet.airflow.tests.fixtures.factories.unannotated",
                TypeError,
            ),
            (
                "comet.airflow.tests.fixtures.factories.wrong_annotation",
                TypeError,
            ),
        ],
    )
    def test_rejects_bad_factory(self, dotted_path, expected_exception):
        with pytest.raises(expected_exception):
            resolve_factory(dotted_path)

    def test_resolves_valid_factory(self):
        from comet.airflow.tests.fixtures.factories import (
            FakeDagParams,
            fake_factory,
        )

        func, params_type = resolve_factory("comet.airflow.tests.fixtures.factories.fake_factory")
        assert func is fake_factory
        assert params_type is FakeDagParams
