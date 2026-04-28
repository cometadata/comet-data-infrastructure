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