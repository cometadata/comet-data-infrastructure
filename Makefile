.RECIPEPREFIX := >

SHELL=/bin/bash
.DEFAULT_GOAL := help

# Read Python from .python-version and other build versions from versions.env.
include versions.env
PYTHON_VERSION := $(shell cat .python-version)

ENV ?= dev

# The default tag identifies the current commit. When pushing uncommitted changes, set a unique
# tag such as IMAGE_TAG=local-1 because ECR tags cannot be overwritten.
IMAGE_TAG ?= sha-$(shell git rev-parse --short=7 HEAD)

.PHONY: help check-ecr-registry check-image-tag check-source-tag

help:
> @echo "Usage: make <target> [VARIABLE=value]"
> @echo
> @echo "Images: build-batch, build-marple, build-airflow, push-batch, push-marple, push-airflow, push-all"
> @echo "Releases: retag, promote"
> @echo "Deployment: sync-vars, status, diff, launch, bootstrap, delete"
> @echo "Dev instance: dev-up, dev-down, dev-refresh, dev-keepalive, dev-autostop, dev-status"
> @echo "Development: install, install-infra, bump-airflow, fmt, fmt-ci, lint, lint-ci, test, clean"
> @echo
> @echo "Variables: ENV, IMAGE_TAG, ECR_REGISTRY, SOURCE_TAG, VERSION_TAG, REGISTRY_CACHE, SKIP_EXISTING, STACK, YES"

check-ecr-registry:
> @test -n "$(ECR_REGISTRY)" || { echo >&2 "ECR_REGISTRY is required but not set. Aborting."; exit 1; }

check-image-tag:
> @test -n "$(IMAGE_TAG)" || { echo >&2 "IMAGE_TAG is required but not set. Aborting."; exit 1; }

check-source-tag:
> @test -n "$(SOURCE_TAG)" || { echo >&2 "SOURCE_TAG is required but not set. Aborting."; exit 1; }

# Sets the cache_args array used by buildx; empty unless REGISTRY_CACHE is set.
define cache_args
cache_args=(); \
  if [[ -n "$(REGISTRY_CACHE)" ]]; then \
    [[ -n "$(ECR_REGISTRY)" ]] || { echo >&2 "ECR_REGISTRY is required when REGISTRY_CACHE is set. Aborting."; exit 1; }; \
    cache_ref="$(ECR_REGISTRY)/comet-$(ENV)-buildcache:$(1)"; \
    cache_args+=(--cache-from "type=registry,ref=$${cache_ref}"); \
    cache_args+=(--cache-to "type=registry,ref=$${cache_ref},mode=max,image-manifest=true,oci-mediatypes=true"); \
  fi
endef

# COMET runtime image (comet-<env>-batch): AWS Batch and the dev arXiv pipeline.
build-batch:
> @$(call cache_args,batch); \
  docker buildx build --platform linux/amd64 --provenance=false --load "$${cache_args[@]}" -f Dockerfile.batch \
    --build-arg "PYTHON_VERSION=$(PYTHON_VERSION)" \
    --build-arg "UV_VERSION=$(UV_VERSION)" \
    --build-arg "S5CMD_VERSION=$(S5CMD_VERSION)" \
    --build-arg "DUCKDB_VERSION=$(DUCKDB_VERSION)" \
    --build-arg "RUST_TARGET_CPU=$(RUST_TARGET_CPU)" \
    --build-arg "ARXIV_TEX_EXTRACT_REPO=$(ARXIV_TEX_EXTRACT_REPO)" \
    --build-arg "ARXIV_TEX_EXTRACT_SHA=$(ARXIV_TEX_EXTRACT_SHA)" \
    --build-arg "COMET_ENRICH_VERSION=$(COMET_ENRICH_VERSION)" \
    --build-arg "COMET_ENRICH_TARGET=$(COMET_ENRICH_TARGET)" \
    -t comet-batch:latest .

push-batch: check-ecr-registry check-image-tag build-batch
> scripts/push-image.sh "comet-$(ENV)-batch" comet-batch:latest "$(ECR_REGISTRY)" "$(IMAGE_TAG)" "$(SKIP_EXISTING)"

# Marple image pinned by MARPLE_SHA in versions.env.
build-marple:
> @$(call cache_args,marple); \
  docker buildx build --platform linux/amd64 --provenance=false --load "$${cache_args[@]}" -f Dockerfile.marple \
    --build-arg "PYTHON_VERSION=$(PYTHON_VERSION)" \
    --build-arg "UV_VERSION=$(UV_VERSION)" \
    --build-arg "MARPLE_REPO=$(MARPLE_REPO)" \
    --build-arg "MARPLE_SHA=$(MARPLE_SHA)" \
    -t comet-marple:latest .

push-marple: check-ecr-registry check-image-tag build-marple
> scripts/push-image.sh "comet-$(ENV)-marple" comet-marple:latest "$(ECR_REGISTRY)" "$(IMAGE_TAG)" "$(SKIP_EXISTING)"

# Airflow image with COMET installed from uv.lock.

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
> @$(call cache_args,airflow); \
  docker buildx build --platform linux/amd64 --provenance=false --load "$${cache_args[@]}" -f Dockerfile.airflow \
    --build-arg "AIRFLOW_VERSION=$(AIRFLOW_VERSION)" \
    --build-arg "PYTHON_VERSION=$(PYTHON_VERSION)" \
    --build-arg "S5CMD_VERSION=$(S5CMD_VERSION)" \
    -t "comet-airflow:$(AIRFLOW_VERSION)" -t comet-airflow:latest .

push-airflow: check-ecr-registry check-image-tag build-airflow
> scripts/push-image.sh "comet-$(ENV)-airflow" comet-airflow:latest "$(ECR_REGISTRY)" "$(IMAGE_TAG)" "$(SKIP_EXISTING)"

push-all: push-batch push-marple push-airflow

# Run by the release pipeline; adds VERSION_TAG to an existing sha build.
retag:
> scripts/retag.sh "$(ENV)" "$(SOURCE_TAG)" "$(VERSION_TAG)"

# Select the image set deploys use, e.g. `make promote SOURCE_TAG=0.1.0`.
promote: check-source-tag
> scripts/promote.sh "$(ENV)" "$(SOURCE_TAG)" "$(ECR_REGISTRY)"

# Store vars-<env>.yaml in SSM for the deploy project.
sync-vars:
> scripts/sync-vars.sh "$(ENV)"

# Optional STACK targets a single stack, e.g. `make status STACK=ec2.yaml`.
status:
> @[[ -d "infra/config/$(ENV)" ]] || { echo >&2 "No Sceptre configuration for ENV=$(ENV)."; exit 1; }
> @[[ -f "vars-$(ENV).yaml" ]] || { echo >&2 "vars-$(ENV).yaml does not exist."; exit 1; }
> uv run --project infra --locked --no-active sceptre --dir infra --var-file="vars-$(ENV).yaml" status "$(ENV)$(if $(STACK),/$(STACK))"

delete:
> @[[ -d "infra/config/$(ENV)" ]] || { echo >&2 "No Sceptre configuration for ENV=$(ENV)."; exit 1; }
> @[[ -f "vars-$(ENV).yaml" ]] || { echo >&2 "vars-$(ENV).yaml does not exist."; exit 1; }
> @test -n "$(STACK)" || { echo >&2 "STACK is required, e.g. make delete STACK=ec2.yaml"; exit 1; }
> uv run --project infra --locked --no-active sceptre --dir infra --var-file="vars-$(ENV).yaml" delete "$(ENV)/$(STACK)"

# Optional STACK targets a single stack, e.g. `make diff STACK=ecr.yaml`.
diff:
> @[[ -d "infra/config/$(ENV)" ]] || { echo >&2 "No Sceptre configuration for ENV=$(ENV)."; exit 1; }
> @[[ -f "vars-$(ENV).yaml" ]] || { echo >&2 "vars-$(ENV).yaml does not exist."; exit 1; }
> uv run --project infra --locked --no-active sceptre --dir infra --var-file="vars-$(ENV).yaml" diff "$(ENV)$(if $(STACK),/$(STACK))"

# Pass YES=1 to skip Sceptre's confirmation prompt (used by the deploy buildspec).
launch:
> @[[ -d "infra/config/$(ENV)" ]] || { echo >&2 "No Sceptre configuration for ENV=$(ENV)."; exit 1; }
> @[[ -f "vars-$(ENV).yaml" ]] || { echo >&2 "vars-$(ENV).yaml does not exist."; exit 1; }
> uv run --project infra --locked --no-active sceptre --dir infra --var-file="vars-$(ENV).yaml" launch $(if $(YES),-y) "$(ENV)$(if $(STACK),/$(STACK))"

# Workload boundary and dependent deployment roles stacks. Run locally with admin credentials;
# the deploy project cannot update this stack group.
bootstrap:
> @[[ -f "vars-$(ENV).yaml" ]] || { echo >&2 "vars-$(ENV).yaml does not exist."; exit 1; }
> uv run --project infra --locked --no-active sceptre --dir infra --var-file="vars-$(ENV).yaml" launch $(if $(YES),-y) bootstrap

# Dev instance lifecycle (nightly shutdown, keepalive) — see "Dev EC2 instance" in docs/setup.md.
# The name must match ${AWS::StackName}-asg in infra/templates/ec2.j2.
dev-up:
> aws autoscaling set-desired-capacity --auto-scaling-group-name "comet-$(ENV)-ec2-asg" --desired-capacity 1

dev-down:
> aws autoscaling set-desired-capacity --auto-scaling-group-name "comet-$(ENV)-ec2-asg" --desired-capacity 0

# Replace the running instance using the ASG's configured launch template version.
dev-refresh:
> aws autoscaling start-instance-refresh --auto-scaling-group-name "comet-$(ENV)-ec2-asg" --no-cli-pager

dev-keepalive:
> aws autoscaling suspend-processes --auto-scaling-group-name "comet-$(ENV)-ec2-asg" --scaling-processes ScheduledActions

dev-autostop:
> aws autoscaling resume-processes --auto-scaling-group-name "comet-$(ENV)-ec2-asg" --scaling-processes ScheduledActions

dev-status:
> aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "comet-$(ENV)-ec2-asg" \
    --query 'AutoScalingGroups[0].{desired:DesiredCapacity,instances:Instances[].{id:InstanceId,state:LifecycleState},suspended:SuspendedProcesses[].ProcessName}' \
    --output json --no-cli-pager

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
