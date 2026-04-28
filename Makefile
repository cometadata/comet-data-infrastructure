.RECIPEPREFIX := >

SHELL=/bin/bash

check-ecr-registry:
> @[ -n "$(ECR_REGISTRY)" ] || { echo >&2 "ECR_REGISTRY is required but not set. Aborting."; exit 1; }

check-env:
> @[ -n "$(ENV)" ] || { echo >&2 "ENV is required but not set. Aborting."; exit 1; }

check-arxiv-tex-extract:
> @[ -n "$(ARXIV_TEX_EXTRACT_PATH)" ] || { echo >&2 "ARXIV_TEX_EXTRACT_PATH is required but not set. Aborting."; exit 1; }

build: check-arxiv-tex-extract
> docker buildx build --platform linux/amd64 --provenance=false --build-context arxiv-tex-extract=$(ARXIV_TEX_EXTRACT_PATH) -t comet:latest .

push-dev: check-ecr-registry build
> docker tag comet:latest $(ECR_REGISTRY)/comet-dev:latest
> docker push $(ECR_REGISTRY)/comet-dev:latest

diff-dev:
> sceptre --var-file=vars-dev.yaml diff dev

deploy-dev: check-ecr-registry push-dev install-infra
> sceptre --var-file=vars-dev.yaml launch dev

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
