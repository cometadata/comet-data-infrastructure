from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import yaml

REQUIRED_KEYS = ("env", "region", "ssm_prefix", "stack_tags")


def vars_path(env: str) -> Path:
    """Return the working directory's vars-<env>.yaml path; exit if it is missing."""
    path = Path(f"vars-{env}.yaml")
    if not path.exists():
        raise SystemExit(f"{path} does not exist.")
    return path


def load_variables(env: str) -> dict:
    """Load vars-<env>.yaml and check it has the keys the deploy commands use."""
    path = vars_path(env)
    try:
        variables = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise SystemExit(f"{path}: {exc}") from None
    if not isinstance(variables, dict):
        raise SystemExit(f"{path} is empty.")
    for key in REQUIRED_KEYS:
        if variables.get(key) is None:
            raise SystemExit(f"{path} is missing {key}.")
    if not isinstance(variables["stack_tags"], dict):
        raise SystemExit(f"{path} stack_tags must be a mapping.")
    if variables["env"] != env:
        raise SystemExit(f"{path} has env {variables['env']}.")
    return variables


def resource_tags(variables: dict, subservice: str) -> list[dict]:
    """Match infra/config/config.yaml stack tags and add Subservice."""
    tags = {
        **variables["stack_tags"],
        "Environment": variables["env"],
        "Service": "comet",
        "Subservice": subservice,
    }
    return [{"Key": key, "Value": str(value)} for key, value in tags.items()]


def put_parameter(variables: dict, name: str, value: str, parameter_type: str) -> None:
    """Create or overwrite an SSM parameter tagged for the build subservice.

    Tags go on before the value is written, so a tagging failure changes nothing.
    """
    tags = resource_tags(variables, "build")
    ssm = boto3.client("ssm", region_name=variables["region"])
    # PutParameter doesn't accept Tags together with Overwrite.
    try:
        ssm.put_parameter(Name=name, Value=value, Type=parameter_type, Tags=tags)
    except ssm.exceptions.ParameterAlreadyExists:
        try:
            ssm.add_tags_to_resource(ResourceType="Parameter", ResourceId=name, Tags=tags)
        except (BotoCoreError, ClientError) as exc:
            code = exc.response["Error"]["Code"] if isinstance(exc, ClientError) else type(exc).__name__
            raise SystemExit(f"{name} was not updated because tagging failed ({code}).") from None
        ssm.put_parameter(Name=name, Value=value, Type=parameter_type, Overwrite=True)


def sync_vars(env: str) -> None:
    """Store `vars-<env>.yaml` in SSM as a SecureString for the deploy project."""
    path = vars_path(env)
    variables = load_variables(env)
    name = f"{variables['ssm_prefix']}/{env}/{path.name}"
    put_parameter(variables, name, path.read_text(), "SecureString")
    print(f"Updated {name}")
