import boto3
import pytest
import yaml

from comet_deploy.cli import app
from comet_deploy.secrets import create_secrets

NAMES = [
    "comet-dev-batch-datacite-credentials",
    "comet-dev-batch-hf-credentials",
    "comet-dev-airflow-fernet",
    "comet-dev-airflow-slack-webhook",
]


def subservice(client, name):
    tags = client.describe_secret(SecretId=name)["Tags"]
    return next(tag["Value"] for tag in tags if tag["Key"] == "Subservice")


class TestCreateSecrets:
    def test_creates_missing_secrets_without_values(self, variables):
        app(["secrets", "dev"], result_action="return_value")

        client = boto3.client("secretsmanager")
        for name in NAMES:
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.get_secret_value(SecretId=name)
        assert subservice(client, "comet-dev-batch-datacite-credentials") == "jobs"
        assert subservice(client, "comet-dev-airflow-fernet") == "platform"

    def test_existing_secrets_only_get_tags(self, variables, tmp_path):
        # The Fernet secret is found by the ARN in the vars file, the Slack secret by its name.
        client = boto3.client("secretsmanager")
        fernet_arn = client.create_secret(Name="comet-dev-airflow-fernet", SecretString="fernet key")["ARN"]
        client.create_secret(Name="comet-dev-airflow-slack-webhook", SecretString="webhook")
        (tmp_path / "vars-dev.yaml").write_text(yaml.safe_dump({**variables, "fernet_secret_arn": fernet_arn}))

        create_secrets("dev")

        assert client.get_secret_value(SecretId="comet-dev-airflow-fernet")["SecretString"] == "fernet key"
        assert client.get_secret_value(SecretId="comet-dev-airflow-slack-webhook")["SecretString"] == "webhook"
        assert subservice(client, "comet-dev-airflow-fernet") == "platform"
        assert subservice(client, "comet-dev-airflow-slack-webhook") == "platform"
