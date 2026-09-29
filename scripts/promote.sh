#!/usr/bin/env bash
# Store SOURCE_TAG in the SSM image tag parameter, which selects the image set the
# next deploy uses. Deploys resolve the tag to digests in each repository, so a
# promoted sha-* tag stops resolving once the release pipeline removes it.
set -euo pipefail

env=$1
source_tag=$2

[[ "$source_tag" =~ ^[A-Za-z0-9._-]+$ ]] || { echo >&2 "SOURCE_TAG has an invalid format."; exit 1; }

ssm_prefix=$(scripts/ssm-prefix.sh "vars-$env.yaml")

# Every repository must have the tag, so a deploy cannot pick up a mixed image set.
for name in batch marple airflow; do
  repo="comet-$env-$name"
  digest=$(aws ecr describe-images --repository-name "$repo" --image-ids "imageTag=$source_tag" \
    --query 'imageDetails[0].imageDigest' --output text) || { echo >&2 "$repo:$source_tag does not exist."; exit 1; }
  [[ "$digest" == sha256:* ]] || { echo >&2 "No digest for $repo:$source_tag."; exit 1; }
  echo "$repo:$source_tag -> $digest"
done

parameter="$ssm_prefix/$env/images/tag"
aws ssm put-parameter --name "$parameter" --type String --value "$source_tag" --overwrite --no-cli-pager >/dev/null
echo "$parameter -> $source_tag"
