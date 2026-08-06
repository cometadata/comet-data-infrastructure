# Running arXiv batch extraction on EC2

Start the dev instance with `make dev-up` and get its instance ID from
`make dev-status` (see [setup.md](setup.md)). For runs that go past the
nightly shutdown, run `make dev-keepalive` first and
`make dev-autostop` when the run is done: the shutdown terminates the
instance, which wipes `/data` and the local resume state.

Connect to the instance via SSM, then pull and run the Docker
image. The `/data` directory is the NVMe mount on the instance; the
pipeline writes under `/data/arxiv/<release-date>/` by default.

Set the bucket, release date, and image env vars up front: the commands
below reference them. Then pull the image:

```bash
export BUCKET="<your-s3-data-bucket>"
export RELEASE_DATE="2026-04-24"   # change per run (e.g. "2026-04-13-test" for a test run)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
IMAGE="${ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/comet-dev:latest"
docker pull "$IMAGE"
```

## Test run (two batches, largest-first)

```bash
nohup docker run --rm \
  -v /data:/data \
  "$IMAGE" \
  comet arxiv pipeline "$BUCKET" "$RELEASE_DATE" \
    --batch-size 500 \
    --max-batches 2 \
    --sort-order smallest \
    --no-cleanup \
  > "/data/arxiv-pipeline-${RELEASE_DATE}.log" 2>&1 &

tail -f "/data/arxiv-pipeline-${RELEASE_DATE}.log"
```

Omitting `--cpus` lets the container use all host CPUs. `--sort-order
largest` processes the biggest tars first: a good shake-down for memory
behaviour.

## Full run

```bash
nohup docker run --rm \
  -v /data:/data \
  "$IMAGE" \
  comet arxiv pipeline "$BUCKET" "$RELEASE_DATE" \
  > "/data/arxiv-pipeline-${RELEASE_DATE}.log" 2>&1 &
```

Default `--batch-size` is 500 and default `--sort-order` is
`chronological`; override with `--batch-size N` or
`--sort-order {largest,smallest}`.

## Layout

Each batch runs in its own self-contained folder. Local:

```
/data/arxiv/${RELEASE_DATE}/
  state.json
  arXiv_src_manifest.xml(+ .etag)
  batch_00001/
    download/          # tars, deleted per-entry after extract
    output/
      checkpoint.log   # latex-extract resume state (stays local)
      <YYYY>/
        arXiv_src_*.parquet
        arXiv_src_*.json
  batch_00002/
  …
```

S3:

```
s3://${BUCKET}/arxiv/${RELEASE_DATE}/
  state.json
  results/<YYYY>/<file>   # merged across all batches
```

The `<YYYY>/` subdirectories keep the per-directory file count bounded
so the release can be published as a HuggingFace dataset without
hitting the platform's per-folder limits.

By default each `batch_NNNNN/` folder is deleted after its checkpoint
advances; pass `--no-cleanup` to keep the folders on disk for
inspection.

## Resuming

Progress is tracked in `/data/arxiv/${RELEASE_DATE}/state.json` and
mirrored to `s3://${BUCKET}/arxiv/${RELEASE_DATE}/state.json`. Re-run
the same command and the pipeline continues from the next unprocessed
batch, skipping already-uploaded results.

Resume requires these flags to match the prior run: `--batch-size`,
`--sort-order`, `--shuffle-seed`. The pipeline also re-verifies the
S3 manifest's ETag, so if the manifest has changed or any resume key
differs the run errors out and names the files to delete to start
over.
