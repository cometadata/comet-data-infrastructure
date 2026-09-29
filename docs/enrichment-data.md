# Enrichment data

COMET publishes DataCite enrichment releases to the `comet-enrichments` Hugging Face bucket through its S3-compatible API.

## Accessing the bucket

You need a Hugging Face account with access to the bucket and an S3-compatible client, such as the [AWS CLI](https://aws.amazon.com/cli/), [s5cmd](https://github.com/peak/s5cmd), or an S3 SDK. The example below uses the AWS CLI.

1. Open your Hugging Face [Access Tokens](https://huggingface.co/settings/tokens) settings and create a read token with the default settings.
2. Open the token's menu, select **Generate S3 credentials**, and copy the access key ID and secret access key. The secret is shown only once.
3. Add a `comet-enrichments` profile to `~/.aws/config` using the settings required by the [Hugging Face S3 API](https://huggingface.co/docs/hub/storage-buckets-s3):

   ```ini
   [profile comet-enrichments]
   region = us-east-1
   endpoint_url = https://s3.hf.co/cometadata
   s3 =
       addressing_style = path
   request_checksum_calculation = when_required
   response_checksum_validation = when_required
   ```

4. Add the matching credentials to `~/.aws/credentials`:

   ```ini
   [comet-enrichments]
   aws_access_key_id = HFAK...
   aws_secret_access_key = ...
   ```

Download the DataCite release index:

```bash
aws --profile comet-enrichments s3 cp s3://comet-enrichments/datacite/index.json ./index.json
```

## Bucket layout

```
datacite/index.json
datacite/funders/2026-04-02/full/
  enrichments/
    part_0000.jsonl.gz
    part_0001.jsonl.gz
    ...
  manifest.json
datacite/funders/2026-04-02/diff/
  enrichments/
    part_0000.jsonl.gz
    ...
  manifest.json
datacite/affiliations/...
datacite/resource-type-general/...
```

Each release folder contains the gzip-compressed JSON Lines shards and manifest written by `comet-enrich`. A `full` release is the complete snapshot for that date; a `diff` release contains only what changed since the previous published release. A release has no diff when no usable earlier release exists. Published full releases have `"exit_status": "success"` in their manifests; a run that loses data fails before publication.

The manifest identifies the source releases used by the enrichment:

```json
{
  "sources": {
    "datacite": {"release_date": "2026-04-02"},
    "ror": {"release_date": "2026-03-19"}
  }
}
```

## Diff releases

Every enrichment record carries an enrichment content key in its `contentKey` field.
It identifies the content being enriched, scoped by method, DOI, field, and action.
Updates and deletions derive their key from `originalValue`; inserts use
`enrichedValue`.

Diff records add an `event` field describing what changed relative to the previous
published release:

| Event        | Meaning                                      | Consumer action        |
|--------------|----------------------------------------------|------------------------|
| `asserted`   | Content key present now, absent before.      | Apply the enrichment.  |
| `retracted`  | Content key present before, absent now.      | Remove the enrichment. |
| `superseded` | Same content key, different `enrichedValue`. | Replace it in place.   |

For updates, changing only `enrichedValue` preserves the content key and produces a
`superseded` event. Changing an inserted value changes its content key, producing a
`retracted` event for the old content key and an `asserted` event for the new content
key. Unchanged enrichments produce no events; changes to `sourceId` alone also produce
no events.

```json
{"doi":"10.1/x","action":"update","field":"types","originalValue":{"resourceTypeGeneral":"Text"},"enrichedValue":{"resourceTypeGeneral":"Dataset"},"sourceId":"10.1234/example","contentKey":"1dc558dae21181dcb1bff9c1c744244f","event":"asserted"}
```

`asserted` and `superseded` records carry the new values; `retracted` records carry the
old values. Start with a full release, then apply newer diffs in `release_date` order.

The diff manifest records the two full releases that were compared and the event totals. It
has no `exit_status`: a diff that cannot be computed cleanly writes no manifest and is not
published.

```json
{
  "schema_version": 1,
  "method": {
    "name": "funders",
    "old_version": "0.4.0",
    "new_version": "0.4.0",
    "diff_tool_version": "0.4.0"
  },
  "old": {
    "sources": {"datacite": {"release_date": "2026-03-02"}, "ror": {"release_date": "2026-02-19"}},
    "records": 1204331
  },
  "new": {
    "sources": {"datacite": {"release_date": "2026-04-02"}, "ror": {"release_date": "2026-03-19"}},
    "records": 1210842
  },
  "artifact_paths": {"enrichments": "enrichments/"},
  "counters": {"asserted": 6900, "retracted": 389, "superseded": 1120, "unchanged": 1202822},
  "timings_ms": {"total": 184211}
}
```

## Release index

The `datacite/index.json` file lists the available releases for every DataCite enrichment method. Use it to detect new releases without listing the bucket. `latest` points to the newest full release. The `releases` list is sorted by date, with each diff before the full release of the same date.

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
            "release_date": "2026-03-02",
            "type": "full",
            "path": "datacite/funders/2026-03-02/full/",
            "published_at": "2026-03-02T12:13:00+00:00"
          },
          {
            "release_date": "2026-04-02",
            "type": "diff",
            "path": "datacite/funders/2026-04-02/diff/",
            "published_at": "2026-04-02T12:25:00+00:00"
          },
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

## Downloading new enrichments

- Download `datacite/index.json` periodically.
- Compare each method's `latest.release_date` with the last release date you ingested.
- To reload from scratch, download the shards from the `enrichments/` directory under `latest.path`.
- To update incrementally, process newer releases in `release_date` order: apply each available diff's events (`asserted`, `retracted`, `superseded`).
