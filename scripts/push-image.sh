#!/usr/bin/env bash
# A non-empty fifth argument skips an image tag already present in ECR.
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
