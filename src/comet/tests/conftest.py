import os

# Moto credentials. COMET_ENV / AWS_REGION are set in the repo-level conftest.py.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

from moto import mock_aws
import pytest

from comet.dynamodb_store import DatasetReleaseRecord


@pytest.fixture
def releases_table():
    with mock_aws():
        DatasetReleaseRecord.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
        yield
        DatasetReleaseRecord.delete_table()
