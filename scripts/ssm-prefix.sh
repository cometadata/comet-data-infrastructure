#!/usr/bin/env bash
# Print the ssm_prefix value from a Sceptre variables file.
set -euo pipefail

vars_file=$1
[[ -f "$vars_file" ]] || { echo >&2 "$vars_file does not exist."; exit 1; }

uv run --project infra --locked --no-active python -c '
import sys, yaml
print(yaml.safe_load(open(sys.argv[1]))["ssm_prefix"])
' "$vars_file"
