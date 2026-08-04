# Setup and deployment

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/),
then:

```bash
make install
make install-infra
```

These commands create the root and infrastructure environments from `uv.lock` and `infra/uv.lock`, respectively.

## Environment variables

Set your AWS profile (must match a profile in `~/.aws/config`) and log in via SSO:

```bash
export AWS_PROFILE=<your-sso-profile>
aws sso login
```

Then export the ECR registry and default region:

```bash
export ECR_REGISTRY=<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
export AWS_DEFAULT_REGION=us-east-1
```

## AWS account prerequisites

Before deploying the monitoring stacks, enable resource tags for telemetry in the COMET AWS account. In the CloudWatch console, open **Settings**, find **Enable resource tags for telemetry**, and turn it on. The tag-scoped log-ingestion alarm receives no data until this account-level setting is enabled.

## Image builds and releases

Pushing to `main` runs the build pipeline, which builds the three images sequentially and tags them with the commit, for example `sha-98dea10`. Pushing a tag such as `v0.1.0` runs the release pipeline, which adds `0.1.0` to those existing images without rebuilding them. The release fails if the commit was not built first.

Release tags are retained. ECR retains the latest 50 `sha-*` builds and the latest three untagged images.

One-time setup:

1. In the Developer Tools console, open **Settings → Connections** and create a connection to GitHub, completing the authorization handshake. Put its ARN into `vars-dev.yaml` as `code_connection_arn` and set `github_repository_id`.
2. Set `codebuild_image` in `vars-dev.yaml` to `aws/codebuild/amazonlinux-x86_64-standard:6.0`.
3. Run `make sync-vars`. This stores `vars-dev.yaml` at `<ssm_prefix>/dev/vars-dev.yaml` as a SecureString for the deploy project.
4. Deploy the two stacks: `make launch STACK=build-pipeline.yaml` and `make launch STACK=deploy.yaml`.

Run `make sync-vars` whenever `vars-dev.yaml` changes.

## Deploying dev stacks

Deployments use image digest URIs stored in three SSM parameters:

- `<ssm_prefix>/dev/images/batch`
- `<ssm_prefix>/dev/images/marple`
- `<ssm_prefix>/dev/images/airflow`

Select an image set by tag. The command checks all three repositories before updating the parameters:

```bash
make promote SOURCE_TAG=0.1.0       # release
make promote SOURCE_TAG=sha-98dea10 # unreleased main build
```

Custom local tags also work if all three images were pushed with the same tag. Unreleased SHA builds are not protected from ECR cleanup.

Preview or launch locally with `make diff` and `make launch`. Use `STACK=<stack>.yaml` to target one stack.

To deploy without a workstation checkout, open the `comet-dev-deploy` project in the CodeBuild console and choose **Start build** without environment overrides. The project clones `main`, fetches `vars-dev.yaml` from Parameter Store, and launches Sceptre. It does not change the image parameters.

The `push-*` targets (`push-batch`, `push-marple`, `push-airflow`, or `push-all`) build and push images locally. Image tags are immutable, so uncommitted work needs an override, for example `make push-all IMAGE_TAG=jamie-test-1`.

### Airflow image

Airflow services and Fargate workers use a custom `apache/airflow:slim` image with COMET and the Amazon provider installed from `uv.lock`. `versions.env` sets the Airflow version; `.python-version` sets Python.

```bash
make build-airflow         # build the image locally
make push-airflow          # push to ECR tagged with IMAGE_TAG
```

To bump the Airflow version, edit `AIRFLOW_VERSION` in `versions.env`, then:

```bash
make bump-airflow
uv run --locked --extra airflow --extra dev pytest
```

Commit `versions.env`, `pyproject.toml`, and `uv.lock` together. The main pipeline builds the new image set; promote its SHA or a subsequent release tag before launching the Airflow services.

`make bump-airflow` rebuilds `uv.lock` using Airflow's official constraints. To change providers or extras, edit the target before running it.

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

### Stack status and deletion

Show the status of all dev stacks, or one stack. `STACK` takes the full config path:

```bash
make status-dev
make status-dev STACK=dev/ec2.yaml
```

Delete a stack. `STACK` is required; sceptre asks for confirmation before deleting:

```bash
make delete-dev STACK=dev/ec2.yaml
```

### Templates

Sceptre templates are Jinja2 (`.j2`): Sceptre renders the Jinja first, then deploys the resulting CloudFormation. Wrap literal `{{...}}` values (e.g. `{{resolve:secretsmanager:...}}`) in `{% raw %}...{% endraw %}` so Jinja doesn't evaluate them.

## Dev EC2 instance

The dev instance is managed by an Auto Scaling Group (`comet-dev-ec2-asg`) that defaults to zero instances, so the stack stays deployed while nothing is running. Start and stop it with:

```bash
make dev-up        # start the instance
make dev-status    # show desired capacity, instance ID, and suspended processes
make dev-down      # terminate the instance
```

After changing the AMI, instance type, or launch template, run `make launch-dev STACK=ec2.yaml` to update the Auto Scaling Group. This affects only newly launched instances. To apply the changes to the current instance, run `make dev-refresh`, which replaces it with one using the new configuration.

A scheduled action sets the desired capacity to zero each night, preventing forgotten instances from continuing to run. The hour and time zone come from `instance_shutdown_hour` and `instance_shutdown_timezone` in `vars-dev.yaml`. For work that must run overnight, use `make dev-keepalive` to disable the nightly shutdown schedule. Run `make dev-autostop` to re-enable it. Re-enabling the schedule does not trigger a missed shutdown, so if you re-enable it after the scheduled shutdown time, the instance will continue running until the following night.

Scaling down, refreshing, and the nightly shutdown all terminate the instance, which wipes the NVMe `/data` volume.

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

1. Set the secret value to `<new key>,<old key>`, then run the init task — redeploy with `make launch STACK=airflow-services.yaml` (its hook runs the init task, which re-encrypts everything with the new key whenever it sees a comma), or run `infra/scripts/airflow-init.sh` by hand.
2. Once it succeeds, set the value to just `<new key>` and run it again.

Don't do step 2 until step 1's deploy is healthy: while the value is `new,old`, Airflow encrypts with the new key and decrypts with either, so a partial re-encrypt is safe; dropping the old key early would orphan rows not yet re-encrypted.
