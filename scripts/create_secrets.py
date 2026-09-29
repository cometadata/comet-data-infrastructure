"""Create the Secrets Manager secrets that CloudFormation doesn't manage, tagged like the stacks.

A secret that already exists, found by its ARN in vars-<env>.yaml or by name, only has its tags
updated. New secrets are created without a value. The script never writes secret values; set them
in the console.

Usage: python scripts/create_secrets.py <env>
"""

from dataclasses import dataclass
from pathlib import Path
import sys

import boto3
import yaml


@dataclass(frozen=True, kw_only=True)
class Secret:
    """A secret CloudFormation doesn't manage."""

    vars_key: str  # Field in vars-<env>.yaml used to look up the secret's ARN.
    name: str  # Secrets Manager name for creation or lookup when no ARN is configured.
    subservice: str  # Subservice tag value, e.g. jobs or platform.
    description: str  # Description stored in Secrets Manager when creating the secret.


def main(env: str) -> None:
    """Create or tag each secret for the environment."""
    vars_file = f"vars-{env}.yaml"
    variables = yaml.safe_load(Path(vars_file).read_text())
    client = boto3.client("secretsmanager", region_name=variables["region"])

    secrets = [
        Secret(
            vars_key="datacite_credentials_secret_arn",
            name=f"comet-{env}-batch-datacite-credentials",
            subservice="jobs",
            description="DataCite account ID and password for the download-datacite Batch job",
        ),
        Secret(
            vars_key="hf_credentials_secret_arn",
            name=f"comet-{env}-batch-hf-credentials",
            subservice="jobs",
            description="Hugging Face bucket credentials for the publish Batch job",
        ),
        Secret(
            vars_key="fernet_secret_arn",
            name=f"comet-{env}-airflow-fernet",
            subservice="platform",
            description="Airflow Fernet key that encrypts connections and variables stored in the metadata DB "
            "(AIRFLOW__CORE__FERNET_KEY)",
        ),
        Secret(
            vars_key="slack_webhook_secret_arn",
            name=f"comet-{env}-airflow-slack-webhook",
            subservice="platform",
            description="Slack webhook connection (slack_default) that Airflow uses for alerts",
        ),
    ]

    for secret in secrets:
        # Same tags that infra/config/config.yaml gives every stack.
        tags = {
            **variables["stack_tags"],
            "Environment": variables["env"],
            "Service": "comet",
            "Subservice": secret.subservice,
        }
        tag_list = [{"Key": k, "Value": str(v)} for k, v in tags.items()]

        # Existing secrets only get their tags updated; their values are never touched.
        arn = variables.get(secret.vars_key)
        if arn:
            client.tag_resource(SecretId=arn, Tags=tag_list)
            print(f"Tagged {arn}")
            continue

        try:
            arn = client.create_secret(Name=secret.name, Description=secret.description, Tags=tag_list)["ARN"]
            print(f"Created {arn}; set its value in the console.")
        except client.exceptions.ResourceExistsException:
            arn = client.describe_secret(SecretId=secret.name)["ARN"]
            client.tag_resource(SecretId=arn, Tags=tag_list)
            print(f"Tagged {arn}")
        print(f"  Set {secret.vars_key} to this ARN in {vars_file}.")


if __name__ == "__main__":
    main(sys.argv[1])
