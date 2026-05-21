from __future__ import annotations

import json

from airflow.exceptions import AirflowException
import pytest

from comet.airflow.utils import get_airflow_connection, resolve_release_record


def set_conn(monkeypatch, conn_id: str, **fields) -> None:
    """Inject an Airflow connection via the AIRFLOW_CONN_<ID> env var backend."""
    monkeypatch.setenv(f"AIRFLOW_CONN_{conn_id.upper()}", json.dumps(fields))


class TestGetRequiredConnection:
    def test_returns_connection_when_valid(self, monkeypatch):
        set_conn(monkeypatch, "valid", login="user", password="pw")
        conn = get_airflow_connection("valid")
        assert conn.login == "user"
        assert conn.password == "pw"

    def test_raises_when_connection_missing(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_CONN_NOPE", raising=False)
        with pytest.raises(AirflowException, match="nope") as exc_info:
            get_airflow_connection("nope")
        assert exc_info.value.__cause__ is not None

    @pytest.mark.parametrize(
        "fields, flags, missing",
        [
            ({"password": "pw"}, {}, ["login"]),
            ({"login": "user"}, {}, ["password"]),
            ({"login": "user", "password": "pw"}, {}, []),
            (
                {"login": "user", "password": "pw"},
                {"require_schema": True, "require_port": True},
                ["schema", "port"],
            ),
        ],
    )
    def test_validates_required_fields(self, monkeypatch, fields, flags, missing):
        set_conn(monkeypatch, "conn", **fields)
        if missing:
            with pytest.raises(AirflowException, match=", ".join(missing)):
                get_airflow_connection("conn", **flags)
        else:
            assert get_airflow_connection("conn", **flags) is not None


class TestResolveReleaseRecord:
    def test_asset_triggered_looks_up_triggering_key(self, mocker):
        get_release = mocker.patch("comet.airflow.utils.dataset_releases.get_release", return_value="rec")
        get_latest = mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release")

        record = resolve_release_record(("datacite", "2026-06-03"), dataset="datacite", release_date="ignored")

        assert record == "rec"
        get_release.assert_called_once_with(dataset="datacite", release_date="2026-06-03")
        get_latest.assert_not_called()

    def test_manual_run_looks_up_explicit_release_date(self, mocker):
        get_release = mocker.patch("comet.airflow.utils.dataset_releases.get_release", return_value="rec")
        get_latest = mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release")

        record = resolve_release_record(None, dataset="datacite", release_date="2026-06-01")

        assert record == "rec"
        get_release.assert_called_once_with(dataset="datacite", release_date="2026-06-01")
        get_latest.assert_not_called()

    def test_manual_run_without_release_date_uses_latest(self, mocker):
        get_release = mocker.patch("comet.airflow.utils.dataset_releases.get_release")
        get_latest = mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release", return_value="rec")

        record = resolve_release_record(None, dataset="datacite", release_date=None)

        assert record == "rec"
        get_latest.assert_called_once_with(dataset="datacite")
        get_release.assert_not_called()

    def test_raises_when_no_release_found(self, mocker):
        mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release", return_value=None)
        with pytest.raises(AirflowException, match="No release found"):
            resolve_release_record(None, dataset="datacite", release_date=None)
