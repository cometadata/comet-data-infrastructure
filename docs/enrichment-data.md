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
datacite/affiliations/...
datacite/resource-type-general/...
```

Each release folder contains the gzip-compressed JSON Lines shards and manifest written by `comet-enrich`.

The manifest identifies the source releases used by the enrichment:

```json
{
  "sources": {
    "datacite": {"release_date": "2026-04-02"},
    "ror": {"release_date": "2026-03-19"}
  }
}
```

## Release index

The `datacite/index.json` file lists the available releases for every DataCite enrichment method and identifies the latest release. Consumers can use this index to detect new releases without listing the contents of the bucket.

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

## Downloading new enrichments

- Download `datacite/index.json` periodically.
- Compare each method's `latest.release_date` with the last release date you ingested.
- If a newer release is available, download the shards from the `enrichments/` directory under `latest.path`.
