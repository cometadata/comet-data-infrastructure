from __future__ import annotations

from pydantic import ValidationError
import pytest
import yaml

from comet.airflow.config import DagsConfig


class TestDagsConfig:
    @pytest.mark.parametrize(
        "yaml_text, expected_substring",
        [
            # Unknown top-level key
            ("dags: []\nunknown_top: oops\n", "extra"),
            # Unknown entry key
            (
                "dags:\n  - dag_id: x\n    factory: m.f\n    bad_key: 1\n",
                "bad_key",
            ),
            # Missing factory
            ("dags:\n  - dag_id: x\n", "factory"),
            # Missing dag_id
            ("dags:\n  - factory: m.f\n", "dag_id"),
        ],
    )
    def test_rejects_invalid_shape(self, yaml_text, expected_substring):
        with pytest.raises(ValidationError) as exc:
            DagsConfig.model_validate(yaml.safe_load(yaml_text))
        assert expected_substring.lower() in str(exc.value).lower()

    def test_accepts_minimal_valid_entry_with_defaults(self):
        config = DagsConfig.model_validate(yaml.safe_load("dags:\n  - dag_id: x\n    factory: m.f\n"))
        assert len(config.dags) == 1
        entry = config.dags[0]
        assert entry.dag_id == "x"
        assert entry.factory == "m.f"
        assert entry.enabled is True
        assert entry.params == {}

    def test_empty_config_yields_empty_dags(self):
        config = DagsConfig.model_validate({})
        assert config.dags == []
