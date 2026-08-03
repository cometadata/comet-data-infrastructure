#!/usr/bin/env bash
# Add VERSION_TAG to the SOURCE_TAG images and remove the sha tag, so released
# images sit outside the ECR sha retention rule. Run by the release pipeline.
# Safe to re-run: a repo that already has VERSION_TAG on the source image is left alone.
set -euo pipefail

env=$1
source_tag=$2
version_tag=$3

[[ "$version_tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo >&2 "VERSION_TAG must be X.Y.Z."; exit 1; }
[[ "$source_tag" =~ ^[A-Za-z0-9._-]+$ ]] || { echo >&2 "SOURCE_TAG has an invalid format."; exit 1; }

repos=("comet-$env-batch" "comet-$env-marple" "comet-$env-airflow")

# Every repo must have the version tag on the source image, or still have the
# source tag. Nothing is changed until all three check out.
declare -A manifests
for repo in "${repos[@]}"; do
  version_digest=$(aws ecr describe-images --repository-name "$repo" --image-ids "imageTag=$version_tag" \
    --query 'imageDetails[0].imageDigest' --output text 2>/dev/null) || version_digest=""
  source_digest=$(aws ecr describe-images --repository-name "$repo" --image-ids "imageTag=$source_tag" \
    --query 'imageDetails[0].imageDigest' --output text 2>/dev/null) || source_digest=""
  if [[ -n "$version_digest" && -n "$source_digest" && "$version_digest" != "$source_digest" ]]; then
    echo >&2 "$repo:$version_tag points to a different image."; exit 1
  elif [[ -z "$version_digest" && -z "$source_digest" ]]; then
    echo >&2 "$repo:$source_tag does not exist."; exit 1
  elif [[ -z "$version_digest" ]]; then
    manifests[$repo]=$(aws ecr batch-get-image --repository-name "$repo" --image-ids "imageTag=$source_tag" \
      --query 'images[0].imageManifest' --output text)
  fi
done

for repo in "${repos[@]}"; do
  if [[ -n "${manifests[$repo]:-}" ]]; then
    aws ecr put-image --repository-name "$repo" --image-tag "$version_tag" \
      --image-manifest "${manifests[$repo]}" --no-cli-pager >/dev/null
  fi
done

# Ignore source tags already removed by an earlier attempt.
for repo in "${repos[@]}"; do
  failures=$(aws ecr batch-delete-image --repository-name "$repo" --image-ids "imageTag=$source_tag" \
    --query "failures[?failureCode!='ImageNotFound']" --output text --no-cli-pager)
  [[ -z "$failures" ]] || { echo >&2 "Could not remove $repo:$source_tag: $failures"; exit 1; }
  echo "$repo:$version_tag"
done
