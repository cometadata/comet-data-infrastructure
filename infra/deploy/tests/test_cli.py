import subprocess
import sys
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from comet_deploy import cli


@pytest.mark.filterwarnings("ignore:Cyclopts application invoked without tokens:UserWarning")
class TestCLI:
    @pytest.mark.parametrize(
        ("error", "message"),
        [
            (
                ClientError(
                    {"Error": {"Code": "AccessDeniedException", "Message": "BatchGetImage is denied"}},
                    "BatchGetImage",
                ),
                "AccessDeniedException.*BatchGetImage is denied",
            ),
            (
                EndpointConnectionError(endpoint_url="https://ecr.example.com"),
                "Could not connect.*https://ecr.example.com",
            ),
        ],
    )
    def test_aws_failures_exit_with_a_concise_error(self, monkeypatch, error, message):
        ecr = boto3.client("ecr")
        ecr.create_repository(repositoryName="comet-dev-batch")

        def fail_fetch(**kwargs):
            raise error

        monkeypatch.setattr(ecr, "batch_get_image", fail_fetch)
        monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: ecr)
        monkeypatch.setattr(sys, "argv", ["comet-deploy", "retag", "dev", "sha-abc1234", "1.2.3"])

        with pytest.raises(SystemExit, match=message) as exc:
            cli.main()

        assert exc.value.code != 0
        assert "does not exist" not in str(exc.value)

    def test_installed_command_lists_the_deployment_commands(self, tmp_path):
        executable = Path(sys.executable).with_name("comet-deploy")
        result = subprocess.run([str(executable), "--help"], cwd=tmp_path, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        for name in ("promote", "retag", "sync-vars", "secrets"):
            assert name in result.stdout
