#!/usr/bin/env bash
# Port-forwards the Airflow UI from the api-server Fargate task to http://localhost:8080.
# Usage: bash scripts/airflow-ui.sh [env] [local-port]
set -euo pipefail

env="${1:-dev}"
local_port="${2:-8080}"
service="comet-${env}-airflow-services-api-server"

cluster=$(aws ecs list-clusters \
  --query "clusterArns[?contains(@,'comet-${env}-airflow')]|[0]" --output text | awk -F/ '{print $NF}')
if [[ -z "$cluster" || "$cluster" == "None" ]]; then
  echo "No ECS cluster found for comet-${env}-airflow" >&2
  exit 1
fi

task=$(aws ecs list-tasks --cluster "$cluster" --service-name "$service" \
  --query 'taskArns[0]' --output text | awk -F/ '{print $NF}')
if [[ -z "$task" || "$task" == "None" ]]; then
  echo "No running task for ${service} on ${cluster}" >&2
  exit 1
fi

runtime=$(aws ecs describe-tasks --cluster "$cluster" --tasks "$task" \
  --query "tasks[0].containers[?name=='api-server'].runtimeId" --output text)

echo "Forwarding http://localhost:${local_port} -> ${service} (${task}) ... Ctrl-C to stop."
aws ssm start-session \
  --target "ecs:${cluster}_${task}_${runtime}" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"${local_port}\"]}"
