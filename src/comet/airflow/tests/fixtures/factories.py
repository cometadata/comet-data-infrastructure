from __future__ import annotations

from comet.airflow.factory import BaseDagParams


class FakeDagParams(BaseDagParams):
    foo: str


def fake_factory(dag_id: str, params: FakeDagParams) -> dict:
    """Happy-path factory: returns a sentinel dict the loader stuffs into globals."""
    return {"dag_id": dag_id, "foo": params.foo}


def build_error_factory(dag_id: str, params: FakeDagParams) -> dict:
    """Factory that raises during create_dag — exercises per-entry isolation."""
    raise RuntimeError(f"intentional build error for {dag_id}")


# Targets for resolve_factory failure tests

not_callable = "i am a string, not a function"


def wrong_signature(only_one):
    pass


def unannotated(dag_id, params):
    return {"dag_id": dag_id}


def wrong_annotation(dag_id: str, params: dict) -> dict:
    return {"dag_id": dag_id}
