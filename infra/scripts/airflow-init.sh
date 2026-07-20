#!/usr/bin/env bash
# Runs the Airflow DB migration as a one-off Fargate task and waits for it to succeed.
# The airflow-services before_launch hook passes the args (Sceptre resolves the stack outputs).
# By hand: bash infra/scripts/airflow-init.sh <cluster> <init-task-def-arn> <subnet> <sg>
set -euo pipefail

cluster="$1"
taskdef="$2"
subnet="$3"
sg="$4"

echo "Running init task on ${cluster} ..."
task_arn=$(aws ecs run-task --cluster "$cluster" --task-definition "$taskdef" --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[${subnet}],securityGroups=[${sg}],assignPublicIp=DISABLED}" \
  --query 'tasks[0].taskArn' --output text)

echo "Waiting for ${task_arn} to stop ..."
aws ecs wait tasks-stopped --cluster "$cluster" --tasks "$task_arn"

read -r code reason <<< "$(aws ecs describe-tasks --cluster "$cluster" --tasks "$task_arn" \
  --query 'tasks[0].[containers[0].exitCode, stoppedReason]' --output text)"
if [ "$code" != "0" ]; then
  echo "Init task failed (exit ${code}): ${reason}" >&2
  exit 1
fi
echo "Init complete."
