import copy

import pytest
import yaml
from moto import mock_aws

VARIABLES = {
    "env": "dev",
    "region": "us-east-1",
    "ssm_prefix": "/test/comet",
    "stack_tags": {
        "CodeRepo": "https://github.com/cometadata/comet-data-infrastructure",
        "Contact": "someone",
    },
}


@pytest.fixture(autouse=True)
def aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        yield


@pytest.fixture
def variables(tmp_path, monkeypatch):
    """Write vars-dev.yaml to a temporary working directory and return a private copy of its contents."""
    monkeypatch.chdir(tmp_path)
    variables = copy.deepcopy(VARIABLES)
    (tmp_path / "vars-dev.yaml").write_text(yaml.safe_dump(variables))
    return variables
