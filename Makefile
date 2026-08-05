.RECIPEPREFIX := >

SHELL=/bin/bash

check-ecr-registry:
> @[ -n "$(ECR_REGISTRY)" ] || { echo >&2 "ECR_REGISTRY is required but not set. Aborting."; exit 1; }

check-env:
> @[ -n "$(ENV)" ] || { echo >&2 "ENV is required but not set. Aborting."; exit 1; }

check-arxiv-tex-extract:
> @[ -n "$(ARXIV_TEX_EXTRACT_PATH)" ] || { echo >&2 "ARXIV_TEX_EXTRACT_PATH is required but not set. Aborting."; exit 1; }

# SSM_PREFIX is the Parameter Store path prefix the deploy writes the Airflow image digest URI
# under. It MUST match `ssm_prefix` in vars-dev.yaml (which Sceptre reads via !ssm).
check-ssm-prefix:
> @[ -n "$(SSM_PREFIX)" ] || { echo >&2 "SSM_PREFIX is required but not set. Aborting."; exit 1; }

# Read Python from .python-version and other build versions from versions.env.
include versions.env
PYTHON_VERSION := $(shell cat .python-version)

build: check-arxiv-tex-extract
> docker buildx build --platform linux/amd64 --provenance=false --build-context arxiv-tex-extract=$(ARXIV_TEX_EXTRACT_PATH) \
>   --build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
>   --build-arg UV_VERSION=$(UV_VERSION) \
>   --build-arg S5CMD_VERSION=$(S5CMD_VERSION) \
>   --build-arg DUCKDB_VERSION=$(DUCKDB_VERSION) \
>   --build-arg RUST_TARGET_CPU=$(RUST_TARGET_CPU) \
>   -t comet:latest .

push-dev: check-ecr-registry build
> docker tag comet:latest $(ECR_REGISTRY)/comet-dev:latest
> docker push $(ECR_REGISTRY)/comet-dev:latest

# AWS Batch image (comet-dev-batch): runtime deps + comet package via Dockerfile.batch
# (no duckdb, no arxiv-tex-extract).
build-batch:
> docker buildx build --platform linux/amd64 --provenance=false -f Dockerfile.batch \
>   --build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
>   --build-arg UV_VERSION=$(UV_VERSION) \
>   --build-arg S5CMD_VERSION=$(S5CMD_VERSION) \
>   --build-arg COMET_ENRICH_VERSION=$(COMET_ENRICH_VERSION) \
>   --build-arg COMET_ENRICH_TARGET=$(COMET_ENRICH_TARGET) \
>   -t comet-batch:latest .

push-batch-dev: check-ecr-registry build-batch
> docker tag comet-batch:latest $(ECR_REGISTRY)/comet-dev-batch:latest
> docker push $(ECR_REGISTRY)/comet-dev-batch:latest

# Marple image (comet-dev-marple): built from Dockerfile.marple, which clones the marple
# repo branch from GitLab. CACHEBUST forces a fresh clone each build. Requires the Marple
# changes — create-ror-index/index-ror entry points, /health endpoint, S3 ROR fetch — see
# the plan / Marple prerequisites.
build-marple:
> docker buildx build --platform linux/amd64 --provenance=false --build-arg CACHEBUST=$$(date +%s) -f Dockerfile.marple \
>   --build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
>   --build-arg UV_VERSION=$(UV_VERSION) \
>   --build-arg MARPLE_REPO=$(MARPLE_REPO) \
>   --build-arg MARPLE_REF=$(MARPLE_REF) \
>   -t comet-marple:latest .

push-marple-dev: check-ecr-registry build-marple
> docker tag comet-marple:latest $(ECR_REGISTRY)/comet-dev-marple:latest
> docker push $(ECR_REGISTRY)/comet-dev-marple:latest

# Airflow image (comet-dev-airflow): extends apache/airflow:slim with the comet
# package via Dockerfile.airflow, installed from uv.lock.

# Update uv.lock using Airflow's official constraints.
bump-airflow:
> curl -sfL "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt" \
>     -o /tmp/airflow-constraints.txt
> uv add --optional airflow --no-sync -c /tmp/airflow-constraints.txt \
>     "apache-airflow[amazon,postgres,standard,fab]==$(AIRFLOW_VERSION)" \
>     "apache-airflow-providers-amazon[aiobotocore]"

check-airflow-version:
> @locked=$$(uv export --locked --no-dev --no-emit-project --no-hashes --no-annotate --extra airflow | grep '^apache-airflow==' | sed -E 's/^apache-airflow==([^ \\]+).*/\1/'); \
>   [ "$$locked" = "$(AIRFLOW_VERSION)" ] || { echo >&2 "versions.env has AIRFLOW_VERSION=$(AIRFLOW_VERSION) but uv.lock has $$locked. Run make bump-airflow."; exit 1; }

build-airflow: check-airflow-version
> docker buildx build --platform linux/amd64 --provenance=false -f Dockerfile.airflow \
>   --build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) \
>   --build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
>   --build-arg S5CMD_VERSION=$(S5CMD_VERSION) \
>   -t comet-airflow:$(AIRFLOW_VERSION) -t comet-airflow:latest .

# Build, tag, and push the image, then record its immutable digest URI in SSM so Sceptre (!ssm)
# pins the task definition to this exact image — re-pushing the same tag alone would not change
# the stack or roll ECS.
push-airflow-dev: check-ecr-registry check-ssm-prefix build-airflow
> docker tag comet-airflow:latest $(ECR_REGISTRY)/comet-dev-airflow:$(AIRFLOW_VERSION)
> docker tag comet-airflow:latest $(ECR_REGISTRY)/comet-dev-airflow:latest
> docker push $(ECR_REGISTRY)/comet-dev-airflow:$(AIRFLOW_VERSION)
> docker push $(ECR_REGISTRY)/comet-dev-airflow:latest
> aws ssm put-parameter \
    --name "$(SSM_PREFIX)/dev/AirflowEcrImageUri" \
    --value "$(ECR_REGISTRY)/comet-dev-airflow@$$(aws ecr describe-images \
        --repository-name comet-dev-airflow \
        --image-ids imageTag=$(AIRFLOW_VERSION) \
        --query 'imageDetails[0].imageDigest' \
        --output text)" \
    --type String --overwrite

# Optional STACK targets a single stack, e.g. `make diff-dev STACK=ecr.yaml`;
# omit it to diff/launch the whole dev environment.
diff-dev: check-uv
> uv run --project infra --locked --no-active sceptre --dir infra --var-file=vars-dev.yaml diff dev$(if $(STACK),/$(STACK))

launch-dev: check-uv
> uv run --project infra --locked --no-active sceptre --dir infra --var-file=vars-dev.yaml launch dev$(if $(STACK),/$(STACK))

deploy-dev: check-ecr-registry check-ssm-prefix push-dev push-batch-dev push-marple-dev push-airflow-dev check-uv
> uv run --project infra --locked --no-active sceptre --dir infra --var-file=vars-dev.yaml launch dev

# Iterate on the Airflow image/DAGs: rebuild, push (which records the new digest URI in SSM),
# then roll just the airflow stack. The digest changes whenever image content changes, so the
# task definition changes and ECS redeploys the services.
deploy-airflow-dev: check-ecr-registry check-ssm-prefix push-airflow-dev check-uv
> uv run --project infra --locked --no-active sceptre --dir infra --var-file=vars-dev.yaml launch dev/airflow-services.yaml

check-uv:
> @command -v uv >/dev/null 2>&1 || { echo >&2 "uv is required but not installed. Aborting."; exit 1; }

install: check-uv
> uv sync --locked --extra airflow --extra dev

install-infra: check-uv
> uv sync --project infra --locked --no-active

clean:
> rm -rf .venv infra/.venv

fmt:
> uv run --extra dev ruff format ./src ./dags

fmt-ci:
> uv run --extra dev ruff format --check ./src ./dags

lint:
> uv run --extra dev ruff check ./src ./dags --fix

lint-ci:
> uv run --extra dev ruff check ./src ./dags

test:
> uv run --locked --extra airflow --extra dev pytest
