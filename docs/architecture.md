# Architecture

COMET runs data processing workflows that download and enrich external scholarly datasets such as [DataCite](https://datacite.org), [ROR](https://ror.org), and [arXiv](https://arxiv.org). [Apache Airflow 3](https://airflow.apache.org) provides the workflow orchestration layer, while ECS Fargate and AWS Batch provide the compute for processing tasks.

Everything is defined in CloudFormation and deployed with [Sceptre](https://docs.sceptre-project.org/) into a VPC public subnet in a single availability zone. See [setup.md](setup.md) for how to deploy.

![Architecture](img/architecture.png)

## Data pipelines

Two ingest DAGs run daily. Each checks the upstream source for a release newer than the last one recorded in the `comet-<env>-dataset-releases` DynamoDB table, downloads it to `s3://<data-bucket>/{dag_id}/{run_id}/`, records the release, and publishes an Airflow Asset. The three DataCite enrichment DAGs are scheduled on the DataCite asset, so they run whenever a new DataCite snapshot is ingested.

![Dataflow](img/dataflow.png)

Heavier processing runs as AWS Batch jobs. The general pattern is to store input and output data in S3: each job downloads the data it needs to local NVMe disk, processes it, uploads the results back to S3, and exits. [s5cmd](https://github.com/peak/s5cmd) is used for the transfers because it is significantly faster than the AWS CLI for large transfers and workloads involving many files. Each job writes to an S3 path that includes the Airflow run ID, and deletes anything already at that path before it starts, so jobs can be re-run safely and don't rely on local state or a specific instance.

Each DAG is created by a factory function in the `comet` package. The DAGs bucket holds a small `dags.py` entry point and a `dags.yaml` file with one entry per DAG instance; a new DAG is added by appending an entry to the YAML file (see [dags.md](dags.md)). Enrichment config files are stored on S3 (`enrichment-configs/`) and downloaded by jobs at runtime, so they can be changed without rebuilding images.

The arXiv TeX extraction pipeline has not been moved to Airflow yet; it is run manually on the dev EC2 instance (see [arxiv-pipeline.md](arxiv-pipeline.md)).

## Apache Airflow

The Airflow services run as a single ECS task on one EC2 instance. Running the services as ECS tasks on an EC2 instance is cheaper than running each service separately on Fargate. It is also easier to define in CloudFormation than running Docker Compose directly on an EC2 instance.

![Airflow](img/airflow.png)

The five containers:

* `init`: runs `airflow db migrate` and `airflow fab-db migrate`, then exits. If the Fernet key is set to `NEW,OLD` for rotation, it also runs `airflow rotate-fernet-key` to re-encrypt stored connections and variables. The other containers wait on it, so the schema is always migrated before anything starts.
* `api-server`: the UI and the Task Execution API that workers use.
* `scheduler`: triggers DAG runs and dispatches tasks.
* `dag-processor`: parses the DAG bundle from the DAGs bucket.
* `triggerer`: runs deferrable operators.

The scheduler uses the [AWS ECS Executor](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/executors/ecs-executor.html) to run each Airflow task as a one-off Fargate task that exits when the task finishes. Workers are not given any secrets; they read connections, variables, and state through the Task Execution API. Batch jobs are submitted with the `BatchOperator` in deferrable mode, which lets Airflow submit a job and wait for it to complete without using a worker while the job runs.

The deployment also relies on several supporting AWS services:

* RDS PostgreSQL stores the Airflow metadata database.
* S3 stores the DAG bundle and task logs.
* Secrets Manager stores the Fernet key, database credentials, admin password, the JWT secret for the Task Execution API, and the API server session signing key.
* CloudWatch stores container logs.

Inbound traffic from the internet is blocked; the UI is accessed with an SSM port forward (see [setup.md](setup.md)).

## AWS Batch

Fargate supports up to 16 vCPU, 120 GiB of memory, and 200 GiB of ephemeral disk, which is not enough for the heavier enrichment jobs. AWS Batch allows larger compute resources to be used when required, whilst only paying for that compute while jobs are running.

There is one job queue per compute environment, and each compute environment uses a single instance type, sized so that one job nearly fills the instance. This stops Batch from scheduling two jobs onto the same instance. A launch template mounts the instance NVMe disks at `/data` (RAID0 when there are two disks).

![AWS Batch](img/aws-batch.png)

Three job definitions:

* `download-datacite`: copies the DataCite snapshot to S3. It has its own execution role because ECS injects the DataCite credentials from Secrets Manager.
* `enrich`: generic single-container CPU enrichment, such as resource type reclassification.
* `enrich-with-ror`: a [single-node multi-container job](https://docs.aws.amazon.com/batch/latest/userguide/create-job-definition-single-node-multi-container.html) used by the funders and affiliations enrichments. It runs an OpenSearch container, a [Marple](https://gitlab.com/crossref/labs/marple) container that seeds the ROR index, and the main container, which waits until Marple reports ready before running the enrichment binary.

The `BatchOperator` sets the command and resource sizes for each run, so the job definitions don't need to be changed for each use case.

## Networking and security

The VPC has a public subnet with an internet gateway. Airflow could alternatively be placed in a private subnet, using a NAT gateway to reach the internet. However, this would add additional cost and complexity. Everything is deployed into a single availability zone, because each additional AZ adds more VPC interface endpoints that have to be paid for. The exception is the RDS subnet group, which AWS requires to cover at least two availability zones.

There are four security groups, one for each group of resources. None of them accept traffic from outside the VPC; the UI and shell access go through SSM, which only needs outbound access.

![Networking](img/networking.png)

* `services` contains the Airflow task and its EC2 host, the only resources with access to the Airflow secrets: the database credentials, Fernet key, JWT secret, API session signing key, and admin password. The only inbound rules are port 8080 from `jobs` for the Task Execution API, and port 8080 from itself for the SSM port forward.
* `jobs` contains the Fargate workers, Batch instances, and the dev instance, and has no inbound rules. Workers and Batch instances have public IPs because they download external data; the Airflow services have no public IPs and reach AWS services through the VPC endpoints.
* `endpoints` accepts 443 from `services` and `jobs`.
* `rds` accepts 5432 from `services` only. Workers and Batch jobs cannot connect to the database; they read and write state through the api-server.

The VPC endpoints are: S3 (a free gateway endpoint), ECR (image pulls), Secrets Manager, CloudWatch Logs, ECS (used by the scheduler to launch Fargate workers), and Batch (used by the triggerer to poll job status).

## Container images

| Image           | Built from           | Runs on                                                                  |
|-----------------|----------------------|--------------------------------------------------------------------------|
| `comet`         | `Dockerfile`         | Dev EC2 instance (arXiv pipeline; includes arxiv-tex-extract and DuckDB) |
| `comet-batch`   | `Dockerfile.batch`   | AWS Batch jobs (comet package + Rust enrichment CLIs)                    |
| `comet-marple`  | `Dockerfile.marple`  | The Marple container in enrich-with-ror jobs                             |
| `comet-airflow` | `Dockerfile.airflow` | Airflow services and Fargate workers                                     |

Images are stored in ECR. The Airflow image is pinned by its sha256 digest, which is stored in SSM. Pushing a new image changes the task definition, which makes ECS redeploy the services.

## Future work

The current deployment is built for development: it favours low cost and easy teardown over durability and high availability. Things to change before using it in production:

* Use CodePipeline and CodeBuild to automatically build the Docker images and deploy updates.
* Set up CloudWatch alerts for CloudWatch logs and other services.
* Could potentially move from ECS on EC2 to plain ECS on Fargate, so that each container is independent and can handle failures better.
* Enable Container Insights on the ECS cluster for per-task CPU/memory metrics.
* Could enable UI access via an internal ALB with SSO.
