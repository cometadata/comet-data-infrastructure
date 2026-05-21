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

# Versions are owned here and passed to Sceptre via --var (see SCEPTRE_VARS), so
# Make and Sceptre share one source of truth. Override per-invocation with an env
# var or make arg, e.g. `make build-airflow AIRFLOW_VERSION=3.2.2`.
AIRFLOW_VERSION         ?= 3.2.1
PYTHON_VERSION          ?= 3.12
AIRFLOW_CONSTRAINTS_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

# Passed to every Sceptre call; CLI --var overrides anything in --var-file.
SCEPTRE_VARS := --var airflow_version=$(AIRFLOW_VERSION) --var python_version=$(PYTHON_VERSION)

build: check-arxiv-tex-extract
> docker buildx build --platform linux/amd64 --provenance=false --build-context arxiv-tex-extract=$(ARXIV_TEX_EXTRACT_PATH) -t comet:latest .

push-dev: check-ecr-registry build
> docker tag comet:latest $(ECR_REGISTRY)/comet-dev:latest
> docker push $(ECR_REGISTRY)/comet-dev:latest

# AWS Batch image (comet-dev-batch): runtime deps + comet package via Dockerfile.batch
# (no duckdb, no arxiv-tex-extract).
build-batch:
> docker buildx build --platform linux/amd64 --provenance=false -f Dockerfile.batch -t comet-batch:latest .

push-batch-dev: check-ecr-registry build-batch
> docker tag comet-batch:latest $(ECR_REGISTRY)/comet-dev-batch:latest
> docker push $(ECR_REGISTRY)/comet-dev-batch:latest

# Enrichment configs are not baked into the batch image; they live on S3 as the source of truth.
# This uploads configs/ -> s3://$(DATA_BUCKET)/enrichment-configs/, the default config_uri
# prefix the enrich jobs download from at runtime (overridable per run via the Trigger form).
DATA_BUCKET ?= comet-dev-s3-data
upload-configs-dev:
> aws s3 cp configs s3://$(DATA_BUCKET)/enrichment-configs/ --recursive

# Marple image (comet-dev-marple): built from Dockerfile.marple, which clones the marple
# repo branch from GitLab. CACHEBUST forces a fresh clone each build. Requires the Marple
# changes — create-ror-index/index-ror entry points, /health endpoint, S3 ROR fetch — see
# the plan / Marple prerequisites.
build-marple:
> docker buildx build --platform linux/amd64 --provenance=false --build-arg CACHEBUST=$$(date +%s) -f Dockerfile.marple -t comet-marple:latest .

push-marple-dev: check-ecr-registry build-marple
> docker tag comet-marple:latest $(ECR_REGISTRY)/comet-dev-marple:latest
> docker push $(ECR_REGISTRY)/comet-dev-marple:latest

# Airflow image (comet-dev-airflow): extends apache/airflow:slim with the comet
# package via Dockerfile.airflow. Bump: edit vars-dev.yaml -> make install-airflow
# -> uv run pytest -> make deploy-airflow-dev (builds, pushes, records the image digest
# in SSM, and rolls the airflow stack).
install-airflow:
> uv pip install "apache-airflow[amazon]==$(AIRFLOW_VERSION)" --constraint $(AIRFLOW_CONSTRAINTS_URL)

build-airflow:
> docker buildx build --platform linux/amd64 --provenance=false -f Dockerfile.airflow \
>   --build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) \
>   --build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
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
diff-dev: install-infra
> sceptre --dir infra $(SCEPTRE_VARS) --var-file=vars-dev.yaml diff dev$(if $(STACK),/$(STACK))

launch-dev: install-infra
> sceptre --dir infra $(SCEPTRE_VARS) --var-file=vars-dev.yaml launch dev$(if $(STACK),/$(STACK))

deploy-dev: check-ecr-registry check-ssm-prefix push-dev push-batch-dev push-marple-dev push-airflow-dev upload-configs-dev install-infra
> sceptre --dir infra $(SCEPTRE_VARS) --var-file=vars-dev.yaml launch dev

# Iterate on the Airflow image/DAGs: rebuild, push (which records the new digest URI in SSM),
# then roll just the airflow stack. The digest changes whenever image content changes, so the
# task definition changes and ECS redeploys the services.
deploy-airflow-dev: check-ecr-registry check-ssm-prefix push-airflow-dev install-infra
> sceptre --dir infra $(SCEPTRE_VARS) --var-file=vars-dev.yaml launch dev/airflow.yaml

check-uv:
> @command -v uv >/dev/null 2>&1 || { echo >&2 "uv is required but not installed. Aborting."; exit 1; }

venv: check-uv
> @if [ ! -d ".venv" ]; then \
>     uv venv --seed .venv; \
> fi

install: venv
> uv sync

install-infra: venv
> uv sync --extra infra

clean:
> rm -rf .venv

fmt:
> uv run --extra dev ruff format ./src ./dags

fmt-ci:
> uv run --extra dev ruff format --check ./src ./dags

lint:
> uv run --extra dev ruff check ./src ./dags --fix

lint-ci:
> uv run --extra dev ruff check ./src ./dags
