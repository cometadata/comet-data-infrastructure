# Enrichment exports

COMET publishes DataCite enrichment releases to a Hugging Face S3-compatible bucket so consumers can download them without egress charges. Enrichment run outputs remain in the data bucket under `s3://<data-bucket>/{dag_id}/{run_id}/`; the Hugging Face bucket contains the published copies.

## Bucket layout

```
datacite/index.json
datacite/funders/2026-01-02/full/
  enrichments/
    part_0000.jsonl.gz
    part_0001.jsonl.gz
    ...
  manifest.json
datacite/affiliations/...
datacite/resource-type-general/...
```

Each release folder contains the gzip-compressed JSON Lines shards and manifest written by `comet-enrich`. COMET currently publishes full snapshots.

## Release index

Each source's index — `datacite/index.json` for DataCite — describes the available releases for every enrichment method and identifies the latest one, so consumers can detect new releases without listing the bucket:

```json
{
  "schema_version": 1,
  "updated_at": "2026-04-02T12:25:00+00:00",
  "datasets": {
    "datacite": {
      "funders": {
        "latest": {
          "release_date": "2026-04-02",
          "type": "full",
          "path": "datacite/funders/2026-04-02/full/"
        },
        "releases": [
          {
            "release_date": "2026-04-02",
            "type": "full",
            "path": "datacite/funders/2026-04-02/full/",
            "published_at": "2026-04-02T12:25:00+00:00"
          }
        ]
      }
    }
  }
}
```

The index is rendered from the `comet-<env>-dataset-releases` DynamoDB table, which is the source of truth for publish state; it is never edited in place on the bucket.

## Consumer contract

- Start from `datacite/index.json`. Poll it and compare `latest` per method with the last ingested release.
- A release folder is immutable once the index references it. The index is uploaded last, after all release files are in place, so a folder named by the index is always complete.
- Download the shards under `enrichments/` and verify against `manifest.json`.

## Publishing

Each enrichment asset event schedules the `datacite_publish` DAG. Unless a release date is supplied manually, the DAG resolves the latest release for every selected dataset. It publishes when all resolved dates match; an asset-triggered run skips when a release is missing or the dates differ, while a manual run fails.

A single `publish` Batch job copies releases not already marked as published, records each copy in DynamoDB, then uploads the release index. Retries skip published releases, and only one publish DAG run can be active at a time.

Manual runs can select the datasets and an optional release date; an empty date resolves the latest release for each dataset. A retry may clear an existing release folder only when the live index does not reference it. Once `datacite/index.json` references a release folder, publishing to that path is refused.

The source, data bucket, Hugging Face bucket name, and endpoint URL are set on the `datacite_publish` entry in `dags.yaml`; the source's datasets come from the enrichment registry in `constants.py`. Each release's files are located through the `source_prefix` that the enrich DAG stored on the release record when it ran, joined with the configured data bucket. The upload credentials come from Secrets Manager (see "Hugging Face publish credentials" in [setup.md](setup.md)).

The `comet publish` CLI command is an internal step of this single-writer DAG and must not be invoked concurrently outside it.

## Retention

COMET currently retains every run prefix in the data bucket and every release in the Hugging Face bucket.
