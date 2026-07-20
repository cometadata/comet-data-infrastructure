# Setup and deployment

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/),
then:

```bash
make install-infra
```

### Local Airflow install

Airflow is deliberately not in `pyproject.toml`. To import `airflow.*` modules locally:

```bash
make install-airflow
```

`uv sync` removes it again (Airflow isn't in the lock file); re-run `make install-airflow` afterwards, or use `uv sync --inexact`.

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

### Airflow image

The Airflow services and Fargate workers run a custom image that extends `apache/airflow:slim` with the amazon provider and the comet package. `airflow_version` and `python_version` live in `vars-dev.yaml`; both the Makefile and Sceptre read them.

```bash
make build-airflow         # build comet-airflow:<version> and :latest
make push-airflow-dev      # push to ECR and record the image digest in SSM
make deploy-airflow-dev    # build, push, and roll the services
```

`push-airflow-dev` writes the pushed image's sha256 digest URI to SSM, and the airflow stack resolves it with `!ssm`. The digest changes with the image content, so the task definition changes and ECS rolls the services; re-pushing the same tag alone would not redeploy. `ssm_prefix` in `vars-dev.yaml` and the `SSM_PREFIX` environment variable must match, and the parameter must exist before the first `sceptre launch dev/airflow-services.yaml` (`make deploy-airflow-dev` and `make deploy-dev` push first, so they self-bootstrap).

To bump the Airflow version:

```bash
# 1. Edit vars-dev.yaml: airflow_version: x.y.z
make install-airflow
uv run pytest
make deploy-airflow-dev
```

### Enrichment configuration

Once the data bucket exists, upload the rules and provenance files from the matching `comet-enrich` release or checkout through the S3 console. Use these object keys:

- `enrichment-configs/resource-type-general-reclassification-rules.yaml`
- `enrichment-configs/resource-type-general-provenance.yaml`
- `enrichment-configs/affiliations-provenance.yaml`
- `enrichment-configs/funders-provenance.yaml`

These objects must exist before running the DataCite enrichment DAGs. Infrastructure deployment does not upload or update them.

### DataCite credentials

Configure the DataCite account ID and password in two places: Secrets Manager for the `download-datacite` Batch job and an Airflow connection for the `datacite_ingest` DAG.

For the Batch job, create an "other type of secret" with `account_id` and `password` keys. Name it `comet-<env>-batch-datacite-credentials` and set its ARN as `datacite_credentials_secret_arn` in `vars-dev.yaml`.

For the DAG, open the Airflow UI (see [Open the Airflow UI](#open-the-airflow-ui)), go to Admin → Connections, and add a connection with these fields:

- Connection Id: `datacite`
- Connection Type: `Generic`
- Login: the DataCite account ID
- Password: the DataCite password

Only Login and Password are used. Leave Description, Host, Schema, Port, and Extra empty.

Create the connection before running `datacite_ingest`; `fetch_release` fails if it is missing. `datacite_conn_name` defaults to `datacite`.

Airflow stores the connection in its metadata database and encrypts it with the [Fernet key](#rotating-the-fernet-key). Stack deployments do not manage the connection. When rotating the credentials, update both the secret and the connection.

### Manual sceptre commands

For per-stack operations not covered by the Makefile:

```bash
uv run sceptre --var-file=vars-dev.yaml status dev
uv run sceptre --var-file=vars-dev.yaml launch dev/ec2.yaml
uv run sceptre --var-file=vars-dev.yaml delete dev/ec2.yaml
```

### Templates

Sceptre templates are Jinja2 (`.j2`): Sceptre renders the Jinja first, then deploys the resulting CloudFormation. Wrap literal `{{...}}` values (e.g. `{{resolve:secretsmanager:...}}`) in `{% raw %}...{% endraw %}` so Jinja doesn't evaluate them.

## Open the Airflow UI

There is no public ingress and no EC2 host to connect through; the UI is reached by port-forwarding into the api-server Fargate task with ECS Exec (enabled on the service). Set the AWS region, then forward with `session`, passing the ECS cluster name (from the comet-dev-airflow stack outputs or the ECS console) and the `api-server` container:

```bash
export AWS_DEFAULT_REGION=us-east-1
session port <cluster-name> api-server api-server.comet.local 8080:8080
```

Log in at <http://localhost:8080> as `admin`. The password is in the Secrets Manager console, in the comet-dev-airflow stack's `-admin-password` secret. The api-server resets the admin password to this secret on each start.

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

One-time setup: create the secret in the Secrets Manager console with a generated key as its value and the description `Airflow Fernet key that encrypts connections and variables stored in the metadata DB (AIRFLOW__CORE__FERNET_KEY)`, then put its ARN into `vars-dev.yaml` as `fernet_secret_arn`.

To rotate, in the Secrets Manager console:

1. Set the secret value to `<new key>,<old key>`, then run the init task — redeploy with `sceptre --dir infra launch dev/airflow-services.yaml` (its hook runs the init task, which re-encrypts everything with the new key whenever it sees a comma), or run `infra/scripts/airflow-init.sh` by hand.
2. Once it succeeds, set the value to just `<new key>` and run it again.

Don't do step 2 until step 1's deploy is healthy: while the value is `new,old`, Airflow encrypts with the new key and decrypts with either, so a partial re-encrypt is safe; dropping the old key early would orphan rows not yet re-encrypted.
