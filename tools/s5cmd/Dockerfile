# s5cmd for the batch and Airflow images, built from its release source with the Go toolchain and module
# versions pinned here and in go.mod/go.sum. Dependabot proposes updates; `make test-s5cmd` (run by CI) runs
# s5cmd's own tests against them, and `make build-s5cmd` exports the binary to build/s5cmd/.
#
# The image is pinned by tag and digest here, not in versions.env, so Dependabot can update both.
FROM public.ecr.aws/docker/library/golang:1.27.3@sha256:2920bb124f1a2e12a1d39f0cab756ae0068441ee1da7655dec7f5e374944095b AS base
ENV GOTOOLCHAIN=local
WORKDIR /wrapper
COPY go.mod go.sum ./
RUN go mod download && go mod verify

FROM base AS build
# go list only reads back the s5cmd version pinned in go.mod, to stamp it into `s5cmd version`. GitCommit is
# the release commit the Go proxy recorded for that version.
RUN version=$(go list -m -f '{{.Version}}' github.com/peak/s5cmd/v2) && \
    info="$(go env GOMODCACHE)/cache/download/github.com/peak/s5cmd/v2/@v/${version}.info" && \
    commit=$(sed -n 's/.*"Hash":"\([0-9a-f]\{7\}\).*/\1/p' "$info") && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/peak/s5cmd/v2/version.Version=${version} -X github.com/peak/s5cmd/v2/version.GitCommit=${commit:-unknown}" \
      -o /s5cmd github.com/peak/s5cmd/v2 && \
    /s5cmd version

# s5cmd's test suite, including the end-to-end copy tests against a fake S3, run against the module versions
# the binary ships. The e2e harness builds s5cmd with -mod=vendor, so the tests run in a writable copy of the
# source whose go.mod is pinned to the shipped versions; test-only modules are added on top, and the build
# fails if they would change a shipped version. Vet is off because it flags upstream test code only.
FROM base AS test
ARG GOVULNCHECK_VERSION=v1.9.0
RUN shipped() { go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}}@{{.Version}}{{end}}{{end}}' "$@" \
      | grep -v '^github.com/peak/s5cmd/' | sort -u; } && \
    shipped github.com/peak/s5cmd/v2 > /shipped.txt && \
    cp -r "$(go list -m -f '{{.Dir}}' github.com/peak/s5cmd/v2)" /src && chmod -R u+w /src && \
    cd /src && \
    go mod edit $(sed 's/^/-require=/' /shipped.txt) && \
    go mod tidy && go mod vendor && \
    { shipped . | diff /shipped.txt - || { echo "Test dependencies change a shipped module version" >&2; exit 1; }; } && \
    go test -vet=off ./... && \
    cd /wrapper && go run "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" github.com/peak/s5cmd/v2

FROM scratch
COPY --from=build /s5cmd /s5cmd
