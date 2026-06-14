# Setup and deployment

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/),
then:

```bash
make install-infra
```

## Environment variables

Set your AWS profile (must match a profile in `~/.aws/config`) and log in via SSO:

```bash
export AWS_PROFILE=<your-sso-profile>
aws sso login
```

Then export the ECR registry, default region, and path to the `arxiv-tex-extract` checkout:

```bash
export ECR_REGISTRY=<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
export AWS_DEFAULT_REGION=us-east-1
export ARXIV_TEX_EXTRACT_PATH=/path/to/arxiv-tex-extract
```

## Deploying dev stacks

Preview changes:

```bash
make diff-dev
```

Build, push, and deploy all dev stacks:

```bash
make deploy-dev
```

### Manual sceptre commands

For per-stack operations not covered by the Makefile:

```bash
uv run sceptre --var-file=vars-dev.yaml status dev
uv run sceptre --var-file=vars-dev.yaml launch dev/ec2.yaml
uv run sceptre --var-file=vars-dev.yaml delete dev/ec2.yaml
```

### Templates

Sceptre templates are Jinja2 (`.j2`): Sceptre renders the Jinja first, then deploys the resulting CloudFormation. Wrap literal `{{...}}` values (e.g. `{{resolve:secretsmanager:...}}`) in `{% raw %}...{% endraw %}` so Jinja doesn't evaluate them.

## Airflow image

The Airflow services and Fargate workers run a custom image that extends `apache/airflow:slim` with the amazon provider and the comet package. `airflow_version` and `python_version` live in `vars-dev.yaml`; both the Makefile and Sceptre read them.

```bash
make build-airflow         # build comet-airflow:<version> and :latest
make push-airflow-dev      # push to ECR and record the image digest in SSM
make deploy-airflow-dev    # build, push, and roll the services
```

`push-airflow-dev` writes the pushed image's sha256 digest URI to SSM, and the airflow stack resolves it with `!ssm`. The digest changes with the image content, so the task definition changes and ECS rolls the services; re-pushing the same tag alone would not redeploy. `ssm_prefix` in `vars-dev.yaml` and the `SSM_PREFIX` environment variable must match, and the parameter must exist before the first `sceptre launch dev/airflow.yaml` (`make deploy-airflow-dev` and `make deploy-dev` push first, so they self-bootstrap).

To bump the Airflow version:

```bash
# 1. Edit vars-dev.yaml: airflow_version: x.y.z
make install-airflow
uv run pytest
make deploy-airflow-dev
```

### Local Airflow install

Airflow is deliberately not in `pyproject.toml`. To import `airflow.*` modules locally:

```bash
make install-airflow
```

`uv sync` removes it again (Airflow isn't in the lock file); re-run `make install-airflow` afterwards, or use `uv sync --inexact`.

## Open the Airflow UI

There is no public ingress; the UI is reached with an SSM port-forward through the ECS host. Set the AWS region first, then forward with `session` (find the host instance ID in the EC2 console — it is the instance in the airflow stack's Auto Scaling group):

```bash
export AWS_DEFAULT_REGION=us-east-1
session port <ec2-id> api-server.comet.local 8080:8080
```

Log in at <http://localhost:8080> as `admin`. The password is in the Secrets Manager console, in the `<stackname>-admin-password` secret. It is set when the admin user is first created; rotating the secret later doesn't change it (reset it via the UI instead).

## Logs

- Airflow service containers: CloudWatch `/comet/<env>/ecs/airflow`, one stream per container.
- Fargate workers: CloudWatch `/comet/<env>/ecs/airflow-worker`.
- Airflow task logs (what the UI shows): `s3://<stackname>-airflow-logs/logs/`.
- Batch jobs: CloudWatch `/comet/<env>/batch/job`.

## Rotating the Fernet key

The Fernet key lives in the `comet-dev-airflow-fernet` Secrets Manager secret, created outside CloudFormation and passed in by ARN (`fernet_secret_arn` in `vars-dev.yaml`), so stack updates and deletes never touch it.

Generate a key with:

```bash
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

One-time setup: create the secret in the Secrets Manager console with a generated key as its value, and put its ARN into `vars-dev.yaml` as `fernet_secret_arn`.

To rotate, in the Secrets Manager console:

1. Set the secret value to `<new key>,<old key>`, then force a new deployment of the airflow service (ECS console → service → Update → Force new deployment). The init container re-encrypts everything with the new key whenever it sees a comma in the value.
2. Once the service is healthy, set the value to just `<new key>` and force a new deployment again.

Don't do step 2 until step 1's deploy is healthy: while the value is `new,old`, Airflow encrypts with the new key and decrypts with either, so a partial re-encrypt is safe; dropping the old key early would orphan rows not yet re-encrypted.