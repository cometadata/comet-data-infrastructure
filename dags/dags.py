"""Loads Airflow DAGs."""

from comet.airflow import load_dags

load_dags(globals())
