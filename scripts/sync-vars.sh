#!/usr/bin/env bash
# Store vars-<env>.yaml in SSM as a SecureString for the deploy project.
set -euo pipefail

env=$1

vars_file="vars-$env.yaml"
ssm_prefix=$(scripts/ssm-prefix.sh "$vars_file")

name="$ssm_prefix/$env/$vars_file"
aws ssm put-parameter --name "$name" --type SecureString \
  --value "$(<"$vars_file")" --overwrite --no-cli-pager >/dev/null
echo "Updated $name"
