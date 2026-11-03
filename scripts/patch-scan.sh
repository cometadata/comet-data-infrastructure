#!/usr/bin/env bash
# Scan a patch build's images and the promoted ones with Trivy, upload the reports to the artifact bucket and
# post a summary to the alert topic. buildspec.yaml runs this after a successful patch build.
# It is a report only: a failed scan or upload logs a warning and the script carries on.
set -uo pipefail

: "${ENV:?}" "${IMAGE_TAG:?}" "${ECR_REGISTRY:?}" "${SSM_PREFIX:?}" "${ALERT_TOPIC_ARN:?}" "${ARTIFACT_BUCKET:?}"

warn() { echo "WARNING: $*" >&2; }

cd "$(dirname "$0")/.." || exit 1
# shellcheck source=versions.env
. ./versions.env

deploy="uv run --project infra --locked --only-group deploy comet-deploy"
images=$($deploy images) || { warn "Could not list the images"; exit 1; }

# Install Trivy, checking the download against the release's checksums.
trivy_dir=$(mktemp -d)
trivy_url="https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}"
trivy_tarball="trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz"
(cd "$trivy_dir" \
  && curl -fsSLO "$trivy_url/$trivy_tarball" \
  && curl -fsSLO "$trivy_url/trivy_${TRIVY_VERSION}_checksums.txt" \
  && awk -v file="$trivy_tarball" '$2 == file' "trivy_${TRIVY_VERSION}_checksums.txt" | sha256sum -c - \
  && tar -xzf "$trivy_tarball" trivy) \
  || { warn "Could not install Trivy $TRIVY_VERSION"; exit 1; }
trivy="$trivy_dir/trivy"

promoted=$(aws ssm get-parameter --name "$SSM_PREFIX/$ENV/images/tag" --query Parameter.Value --output text) \
  || { warn "Could not read the promoted image tag"; promoted=""; }

"$trivy" image --quiet --download-db-only || warn "Could not download the Trivy database"

reports=reports
mkdir -p "$reports"

# Writes reports/<image>-<tag>.json and a readable .txt table. Arguments: image, tag, and where Trivy finds the
# image: docker for the new build, which is still in the local daemon, or remote for the promoted one in ECR.
scan() {
  local name="$1-$2" ref="$ECR_REGISTRY/comet-$ENV-$1:$2"
  if "$trivy" image --quiet --skip-db-update --image-src "$3" --scanners vuln --format json \
      -o "$reports/$name.json" "$ref"; then
    "$trivy" convert --quiet --format table -o "$reports/$name.txt" "$reports/$name.json" \
      || warn "Could not write the table for $name"
  else
    warn "Could not scan $ref"
    rm -f "$reports/$name.json"
  fi
}

for image in $images; do
  scan "$image" "$IMAGE_TAG" docker
  if [[ -n "$promoted" ]]; then
    scan "$image" "$promoted" remote
  fi
done

prefix="patch-scans/$IMAGE_TAG/"
report_url="https://s3.console.aws.amazon.com/s3/buckets/$ARTIFACT_BUCKET?prefix=$prefix${AWS_REGION:+&region=$AWS_REGION}"
aws s3 cp --recursive --only-show-errors "$reports" "s3://$ARTIFACT_BUCKET/$prefix" \
  || { warn "Could not upload the reports"; report_url=""; }

$deploy scan-summary \
  "$ENV" "$IMAGE_TAG" "$reports" "$ALERT_TOPIC_ARN" --promoted-tag "$promoted" --report-url "$report_url"
