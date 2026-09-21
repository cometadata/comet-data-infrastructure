from __future__ import annotations

import json
from types import SimpleNamespace

from airflow.exceptions import AirflowException, AirflowSkipException
import pytest

from comet.airflow.utils import get_airflow_connection, previous_release, resolve_release_record


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
        stub = SimpleNamespace(pruned_at=None)
        get_release = mocker.patch("comet.airflow.utils.dataset_releases.get_release", return_value=stub)
        get_latest = mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release")

        record = resolve_release_record(
            dataset="datacite", release_date="ignored", triggering_key=("datacite", "2026-06-03")
        )

        assert record is stub
        get_release.assert_called_once_with(dataset="datacite", release_date="2026-06-03")
        get_latest.assert_not_called()

    def test_manual_run_looks_up_explicit_release_date(self, mocker):
        stub = SimpleNamespace(pruned_at=None)
        get_release = mocker.patch("comet.airflow.utils.dataset_releases.get_release", return_value=stub)
        get_latest = mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release")

        record = resolve_release_record(dataset="datacite", release_date="2026-06-01")

        assert record is stub
        get_release.assert_called_once_with(dataset="datacite", release_date="2026-06-01")
        get_latest.assert_not_called()

    def test_manual_run_without_release_date_uses_latest(self, mocker):
        stub = SimpleNamespace(pruned_at=None)
        get_release = mocker.patch("comet.airflow.utils.dataset_releases.get_release")
        get_latest = mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release", return_value=stub)

        record = resolve_release_record(dataset="datacite", release_date=None)

        assert record is stub
        get_latest.assert_called_once_with(dataset="datacite")
        get_release.assert_not_called()

    def test_raises_when_no_release_found(self, mocker):
        mocker.patch("comet.airflow.utils.dataset_releases.get_latest_release", return_value=None)
        with pytest.raises(AirflowException, match="No release found"):
            resolve_release_record(dataset="datacite", release_date=None)

    def test_raises_when_release_was_pruned(self, mocker):
        pruned = SimpleNamespace(release_date="2026-06-01", pruned_at="2026-07-15T00:00:00+00:00")
        mocker.patch("comet.airflow.utils.dataset_releases.get_release", return_value=pruned)
        with pytest.raises(AirflowException, match="pruned"):
            resolve_release_record(dataset="datacite", release_date="2026-06-01")


class TestPreviousRelease:
    def test_returns_the_previous_release_full_prefix_and_date(self, mocker):
        lookup = mocker.patch(
            "comet.airflow.utils.dataset_releases.get_previous_release",
            return_value=SimpleNamespace(full_source_prefix="dag/run-2/full/", release_date="2026-02-02"),
        )

        previous = previous_release(dataset="datacite-funders", release_date="2026-03-02")

        assert previous == {"full_source_prefix": "dag/run-2/full/", "release_date": "2026-02-02"}
        lookup.assert_called_once_with(dataset="datacite-funders", before="2026-03-02")

    def test_skips_when_no_earlier_usable_release_exists(self, mocker):
        mocker.patch("comet.airflow.utils.dataset_releases.get_previous_release", return_value=None)

        with pytest.raises(AirflowSkipException):
            previous_release(dataset="datacite-funders", release_date="2026-03-02")
