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

COMET uses a shared VPC, public subnet, and route table created outside this repository. You also need an S3 bucket for CloudFormation templates and a GitHub connection in **Developer Tools → Settings → Connections**. Create and authorize the connection, and confirm that its status is **Available**. Record the stack outputs, bucket name, connection ARN, and GitHub repository ID in `vars-dev.yaml` during the [first deployment](#first-deployment).

Create the [DataCite credentials](#datacite-credentials), [Hugging Face publish credentials](#hugging-face-publish-credentials), [Airflow Fernet key](#rotating-the-fernet-key), and [Slack webhook](#configure-slack-alerts) in Secrets Manager before deploying. Use the default `aws/secretsmanager` encryption key for all four. The COMET roles do not have permission to decrypt customer-managed KMS keys.

Enable resource tags for telemetry in the COMET AWS account before deploying the monitoring stacks. In the CloudWatch console, open **Settings**, find **Enable resource tags for telemetry**, and turn it on. The tag-scoped log-ingestion alarm receives no data until this account-level setting is enabled.

## Deployment permissions

`make bootstrap` creates three resources used by later deployments:

* The CodeBuild runner role reads the deployment inputs, creates, updates, and deletes only `comet-<env>-*` CloudFormation stacks through the service role, reads the external stack outputs used by the configuration, and runs the Airflow database migration task.
* The CloudFormation service role has broad permissions to create and update the AWS resources used by the environment. Any IAM role it creates must have the COMET permissions boundary. It cannot create IAM users or access keys or change the roles and policies created by `make bootstrap`.
* The permissions boundary is attached to every IAM role created by the environment stacks, including roles for services, jobs, builds, and monitoring. Both a role's own policy and the boundary must allow an action. The boundary permits the environment's resources and the AWS read, monitoring, and Systems Manager or ECS agent calls needed to run them.

CloudFormation assigns names to the roles and policies. Their IAM paths remain under `/comet/bootstrap/<env>/` or `/comet/<env>/<component>/`, so policies can match an environment's resources without depending on generated names. `make launch` does not update the bootstrap stacks; run `make bootstrap` again when these deployment permissions change.

## First deployment

1. Copy the example variables file:

   ```bash
   cp vars-dev.yaml.example vars-dev.yaml
   ```

2. Fill in the environment settings and every placeholder except the three bootstrap outputs. This includes the template bucket, SSM prefix, shared network stack outputs, alert addresses, GitHub connection, repository, and secret ARNs. Leave `permissions_boundary_arn`, `cloudformation_service_role_arn`, and `deployment_runner_role_arn` empty for now. `Environment` and `Service` are added to the stack tags by Sceptre.
3. Using credentials that can create IAM roles and managed policies, create the deployment permissions:

   ```bash
   make bootstrap
   ```

4. Copy the stack outputs into `vars-dev.yaml`:

   * `BoundaryArn` → `permissions_boundary_arn`
   * `ServiceRoleArn` → `cloudformation_service_role_arn`
   * `RunnerRoleArn` → `deployment_runner_role_arn`

5. Store the completed variables file in Parameter Store for the deploy project:

   ```bash
   make sync-vars
   ```

6. Deploy the image-build pipeline and deploy project. Sceptre also launches the ECR, S3, and monitoring alert stacks required by the build pipeline:

   ```bash
   make launch STACK=build-pipeline.yaml
   make launch STACK=deploy.yaml
   ```

7. Push a commit to `main`, or start the main image-build pipeline in CodePipeline, to build the first image set. When the build finishes, select its `sha-*` tag with `make promote SOURCE_TAG=<tag>`.
8. Preview and deploy the remaining stacks:

   ```bash
   make diff
   make launch
   ```

## Image builds and releases

The path from a commit to running stacks:

```mermaid
flowchart LR
    subgraph build["Build and release"]
        main["push to main"] --> bp["build pipeline<br/>buildspec.yaml, build mode"]
        vtag["push tag v0.1.0"] --> rp["release pipeline<br/>buildspec.yaml, release mode"]
        bp -->|"push images tagged sha-98dea10"| ecr["ECR"]
        rp -->|"add tag 0.1.0 to existing images"| ecr
    end

    subgraph promote["Promote"]
        pm["make promote SOURCE_TAG=0.1.0"]
    end
    ecr -->|"resolve tag to digests"| pm
    pm -->|"digest URIs"| img["SSM images/batch, marple, airflow"]

    subgraph deploy["Deploy"]
        vars["vars-dev.yaml"] -->|"make sync-vars"| varsp["SSM vars-dev.yaml"]
        start["start comet-dev-deploy in CodeBuild console"] --> ds["buildspec-deploy.yaml<br/>runs as DeploymentRunnerRole"]
        varsp --> ds
        ds -->|"make launch"| cfn["CloudFormation with<br/>DeploymentServiceRole"]
    end
    img --> cfn
```

Pushing to `main` runs the build pipeline, which builds the three images sequentially and tags them with the commit, for example `sha-98dea10`. Pushing a tag such as `v0.1.0` runs the release pipeline, which adds `0.1.0` to those existing images without rebuilding them. The release fails if the commit was not built first.

Release tags are retained. ECR retains the latest 50 `sha-*` builds and the latest three untagged images.

The pipeline uses the GitHub connection and `codebuild_image` configured during the [first deployment](#first-deployment).

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

Run `make sync-vars` whenever `vars-dev.yaml` changes so the deploy project receives the new settings.

### Deploying a local build

To deploy the current checkout without going through the pipeline, build and push all three images with a local tag, promote that tag, and launch:

```bash
export ECR_REGISTRY=<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
export IMAGE_TAG="local-$(git rev-parse --short=7 HEAD)"
make push-all IMAGE_TAG="$IMAGE_TAG"
make promote SOURCE_TAG="$IMAGE_TAG"
make launch
```

The `local-` prefix keeps these builds apart from the pipeline's `sha-*` tags for the same commit. ECR tags are immutable, so pushing again after further uncommitted changes needs a new tag. The individual `push-batch`, `push-marple`, and `push-airflow` targets build and push one image.

### Airflow image

Airflow services and Fargate workers use a custom `apache/airflow:slim` image with COMET and the Amazon and Slack providers installed from `uv.lock`. `versions.env` sets the Airflow version; `.python-version` sets Python.

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

### Configure Slack alerts

Create a Slack channel for the alerts:

1. In Slack, select **+ → Channel**, enter a name such as `airflow`, and create the channel.
2. Open [Slack API Apps](https://api.slack.com/apps), select **Create New App → From scratch**, and choose the workspace.
3. Open **Incoming Webhooks** and enable **Activate Incoming Webhooks**.
4. Select **Add New Webhook to Workspace**, choose the alert channel, and authorize it.
5. Copy the generated `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX` URL.

Then store the webhook as a Secrets Manager secret. The Airflow workers read the `slack_default`
connection from this secret. Every Slack message is sent from a worker, and deadline callbacks run
with a restricted Execution API token that cannot read connections from the metadata database, so
the connection must resolve from the environment instead.

1. Open Secrets Manager in the AWS console and select **Store a new secret → Other type of secret**.
2. On the **Plaintext** tab, enter the connection as JSON:

   ```json
   {"conn_type": "slackwebhook", "password": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"}
   ```

3. Name the secret, for example `comet-dev-airflow-slack-webhook`, and finish creating it.
4. Copy the secret ARN to `slack_webhook_secret_arn` in `vars-dev.yaml`.

#### Test Slack alerts

The `slack_alert_test` entry in `dags/dags.yaml.example` is disabled by default. Enable it in the deployed `dags.yaml`, upload the file as described in [dags.md](dags.md), then unpause and trigger the DAG in Airflow. One task sends the success message immediately; the other sleeps past the two-minute deadline and then fails.

### Enrichment configuration

Upload `configs/resource-type-general-reclassification-rules.yaml` from `comet-enrich` to the data bucket at `enrichment-configs/resource-type-general-reclassification-rules.yaml`. The object must exist before running the `datacite_enrich_resource_type_general` DAG.

Set `source_id` on each enrichment DAG's entry in `dags.yaml` to the enrichment project's DOI name, such as `10.1234/example`. Each DAG writes the value to the `sourceId` field of every output record. For an individual run, `source_id` can also be overridden in the Airflow UI when triggering the DAG.

### DataCite credentials

Configure the DataCite account ID and password in Secrets Manager for the `download-datacite` Batch job and in the `datacite` Airflow connection. Update both when rotating the credentials.

Create the Batch secret:

```bash
aws secretsmanager create-secret --name comet-dev-batch-datacite-credentials \
  --secret-string '{"account_id":"<id>","password":"<password>"}'
```

Copy the returned ARN to `datacite_credentials_secret_arn` in `vars-dev.yaml`.

For the DAG, open the Airflow UI (see [Open the Airflow UI](#open-the-airflow-ui)), go to Admin → Connections, and add a connection with these fields:

- Connection Id: `datacite`
- Connection Type: `Generic`
- Login: the DataCite account ID
- Password: the DataCite password

Create the connection before running `datacite_ingest`; `fetch_release` fails if it is missing. `datacite_conn_name` defaults to `datacite`.

### Hugging Face publish credentials

The `publish` Batch job uploads enrichment releases to a Hugging Face S3-compatible bucket and reads its credentials from Secrets Manager.

Create the publish secret:

```bash
aws secretsmanager create-secret --name comet-dev-batch-hf-credentials \
  --secret-string '{"access_key_id":"<key-id>","secret_access_key":"<secret-key>"}'
```

Copy the returned ARN to `hf_credentials_secret_arn` in `vars-dev.yaml`.

The Hugging Face bucket name and endpoint URL are set on the `datacite_publish` entry in `dags.yaml`.

### Stack status and deletion

Show the status of all dev stacks, or one stack. `STACK` is the config path relative to the environment:

```bash
make status
make status STACK=ec2.yaml
```

Delete a stack. `STACK` is required; sceptre asks for confirmation before deleting:

```bash
make delete STACK=ec2.yaml
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

After changing the AMI, instance type, or launch template, run `make launch STACK=ec2.yaml` to update the Auto Scaling Group. This affects only newly launched instances. To apply the changes to the current instance, run `make dev-refresh`, which replaces it with one using the new configuration.

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

One-time setup: create the secret in the Secrets Manager console with a generated key as its value and the description `Airflow Fernet key that encrypts connections and variables stored in the metadata DB (AIRFLOW__CORE__FERNET_KEY)`. Leave encryption on the default `aws/secretsmanager` key, then put its ARN into `vars-dev.yaml` as `fernet_secret_arn`.

To rotate, in the Secrets Manager console:

1. Set the secret value to `<new key>,<old key>`, then run the init task — redeploy with `make launch STACK=airflow-services.yaml` (its hook runs the init task, which re-encrypts everything with the new key whenever it sees a comma), or run `infra/scripts/airflow-init.sh` by hand.
2. Once it succeeds, set the value to just `<new key>` and run it again.

Don't do step 2 until step 1's deploy is healthy: while the value is `new,old`, Airflow encrypts with the new key and decrypts with either, so a partial re-encrypt is safe; dropping the old key early would orphan rows not yet re-encrypted.
