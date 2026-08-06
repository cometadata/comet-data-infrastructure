#!/usr/bin/env bash
# Push a locally built image to ECR. A non-empty fifth argument skips a tag
# already in ECR, so a retried CI build does not fail re-pushing an immutable tag.
set -euo pipefail

repo=$1
local_image=$2
ecr_registry=$3
image_tag=$4
skip_existing=${5:-}

if [[ -n "$skip_existing" ]] && aws ecr describe-images --repository-name "$repo" --image-ids "imageTag=$image_tag" >/dev/null 2>&1; then
  echo "$repo:$image_tag already in ECR, skipping push"
else
  docker tag "$local_image" "$ecr_registry/$repo:$image_tag"
  docker push "$ecr_registry/$repo:$image_tag"
fi
