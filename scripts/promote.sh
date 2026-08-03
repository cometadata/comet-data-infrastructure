#!/usr/bin/env bash
# Point the SSM image parameters at the digests of the SOURCE_TAG build, which
# selects the image set the next deploy uses.
set -euo pipefail

env=$1
source_tag=$2
ecr_registry=$3

[[ "$ecr_registry" =~ ^[0-9]+\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com$ ]] || { echo >&2 "Invalid ECR_REGISTRY: '$ecr_registry'"; exit 1; }

ssm_prefix=$(scripts/ssm-prefix.sh "vars-$env.yaml")

# Resolve all three digests before writing anything, so a missing image cannot
# leave the parameters pointing at a mixed image set.
names=(batch marple airflow)
declare -A digests
for name in "${names[@]}"; do
  repo="comet-$env-$name"
  digest=$(aws ecr describe-images --repository-name "$repo" --image-ids "imageTag=$source_tag" \
    --query 'imageDetails[0].imageDigest' --output text) || { echo >&2 "$repo:$source_tag does not exist."; exit 1; }
  [[ "$digest" == sha256:* ]] || { echo >&2 "No digest for $repo:$source_tag."; exit 1; }
  digests[$name]=$digest
done

for name in "${names[@]}"; do
  parameter="$ssm_prefix/$env/images/$name"
  uri="$ecr_registry/comet-$env-$name@${digests[$name]}"
  aws ssm put-parameter --name "$parameter" --type String --value "$uri" --overwrite --no-cli-pager >/dev/null
  echo "$parameter -> $uri"
done
