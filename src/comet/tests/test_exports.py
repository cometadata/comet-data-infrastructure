import datetime
from io import BytesIO
import json

import boto3
from botocore.exceptions import ClientError
import pytest

from comet.constants import (
    DATACITE_AFFILIATIONS_ENRICHMENT,
    DATACITE_FUNDERS_ENRICHMENT,
    DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT,
    Enrichment,
)
from comet.dynamodb_store import get_release, mark_published, persist_discovered_release
from comet.exports import (
    copy_release_to_hf,
    full_release_prefix,
    hf_env,
    index_key,
    publish_index,
    publish_releases,
)
from comet.model.dataset_version_model import DatasetRelease

HF_ENDPOINT_URL = "https://s3.example.com"


@pytest.fixture
def hf_credentials(monkeypatch):
    monkeypatch.setenv("HF_S3_ACCESS_KEY_ID", "hf-key")
    monkeypatch.setenv("HF_S3_SECRET_ACCESS_KEY", "hf-secret")


def persist_release(enrichment: Enrichment, date_str: str, published: bool = False):
    release = DatasetRelease(release_date=datetime.date.fromisoformat(date_str))
    persist_discovered_release(dataset=enrichment.identifier, release=release, run_id=f"run-{date_str}")
    if published:
        mark_published(
            dataset=enrichment.identifier,
            release_date=date_str,
            export_path=full_release_prefix(enrichment, date_str),
            release_type="full",
        )


def mock_hf_client(mocker, index=None):
    client = mocker.Mock()
    if index is None:
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
            "GetObject",
        )
    else:
        client.get_object.return_value = {"Body": BytesIO(json.dumps(index).encode())}
    mocker.patch("comet.exports.hf_s3_client", return_value=client)
    return client


@pytest.mark.parametrize(
    ("enrichment", "expected"),
    [
        (DATACITE_FUNDERS_ENRICHMENT, "datacite/funders/2026-01-02/full/"),
        (Enrichment("openalex-works", "affiliations"), "openalex-works/affiliations/2026-01-02/full/"),
    ],
)
def test_full_release_prefix_joins_source_method_and_date(enrichment, expected):
    assert full_release_prefix(enrichment, "2026-01-02") == expected


class TestHfEnv:
    def test_remaps_keys_and_drops_session_token(self, hf_credentials, monkeypatch):
        monkeypatch.setenv("AWS_SESSION_TOKEN", "task-role-token")

        env = hf_env()

        assert env["AWS_ACCESS_KEY_ID"] == "hf-key"
        assert env["AWS_SECRET_ACCESS_KEY"] == "hf-secret"
        assert "AWS_SESSION_TOKEN" not in env


class TestPublishIndex:
    def test_uploads_index_of_published_releases_with_latest_pointer(self, releases_table):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", published=True)
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-02-02", published=True)
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-03-02", published=False)
        persist_release(DATACITE_AFFILIATIONS_ENRICHMENT, "2026-01-02", published=False)
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket="hf-bucket")

        publish_index(source="datacite", hf_bucket="hf-bucket", s3_client=s3_client)

        body = s3_client.get_object(Bucket="hf-bucket", Key=index_key("datacite"))["Body"].read()
        index = json.loads(body)
        assert index["schema_version"] == 1
        assert index["updated_at"] is not None
        assert set(index["datasets"]["datacite"].keys()) == {"funders"}
        funders = index["datasets"]["datacite"]["funders"]
        assert funders["latest"] == {
            "release_date": "2026-02-02",
            "type": "full",
            "path": "datacite/funders/2026-02-02/full/",
        }
        assert [r["release_date"] for r in funders["releases"]] == ["2026-01-02", "2026-02-02"]
        assert all(r["type"] == "full" and r["published_at"] for r in funders["releases"])


class TestCopyReleaseToHf:
    def test_stages_download_then_cleans_target_and_uploads(self, mocker, tmp_path):
        stage = tmp_path / "stage"
        client = mocker.Mock()
        env = {"AWS_ACCESS_KEY_ID": "hf-key"}
        mocker.patch("comet.exports.local_dir_for_uri", return_value=stage)
        mock_download = mocker.patch("comet.exports.download_files_from_s3")
        mock_clean = mocker.patch("comet.exports.clean_s3_prefix")
        mock_upload = mocker.patch("comet.exports.upload_files_to_s3")

        copy_release_to_hf(
            source_uri="s3://data-bucket/datacite_enrich_funders/run-1/",
            hf_bucket="hf-bucket",
            hf_prefix="datacite/funders/2026-01-02/full/",
            endpoint_url=HF_ENDPOINT_URL,
            s3_client=client,
            env=env,
        )

        target_uri = "s3://hf-bucket/datacite/funders/2026-01-02/full/"
        mock_download.assert_called_once_with("s3://data-bucket/datacite_enrich_funders/run-1/*", stage)
        mock_clean.assert_called_once_with(target_uri, s3_client=client, endpoint_url=HF_ENDPOINT_URL, env=env)
        mock_upload.assert_called_once_with(stage, target_uri, endpoint_url=HF_ENDPOINT_URL, env=env)
        assert not stage.exists()


class TestPublishReleases:
    SOURCE_URIS = {
        DATACITE_FUNDERS_ENRICHMENT.identifier: "s3://data-bucket/datacite_enrich_funders/run-1/",
        DATACITE_AFFILIATIONS_ENRICHMENT.identifier: "s3://data-bucket/datacite_enrich_affiliations/run-1/",
        DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier: (
            "s3://data-bucket/datacite_enrich_resource_type_general/run-1/"
        ),
    }

    def test_copies_and_marks_unpublished_datasets_then_uploads_index(self, mocker, releases_table, hf_credentials):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", published=True)
        persist_release(DATACITE_AFFILIATIONS_ENRICHMENT, "2026-01-02")
        persist_release(DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT, "2026-01-02")
        client = mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")
        mock_publish_index = mocker.patch("comet.exports.publish_index")

        publish_releases(
            source="datacite",
            release_date="2026-01-02",
            source_uris=self.SOURCE_URIS,
            hf_bucket="hf-bucket",
            endpoint_url=HF_ENDPOINT_URL,
        )

        copied = {(call.kwargs["source_uri"], call.kwargs["hf_prefix"]) for call in mock_copy.call_args_list}
        assert copied == {
            (
                self.SOURCE_URIS[DATACITE_AFFILIATIONS_ENRICHMENT.identifier],
                "datacite/affiliations/2026-01-02/full/",
            ),
            (
                self.SOURCE_URIS[DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier],
                "datacite/resource-type-general/2026-01-02/full/",
            ),
        }
        for enrichment in (
            DATACITE_FUNDERS_ENRICHMENT,
            DATACITE_AFFILIATIONS_ENRICHMENT,
            DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT,
        ):
            record = get_release(dataset=enrichment.identifier, release_date="2026-01-02")
            assert record.published_at
            assert record.export_path == full_release_prefix(enrichment, "2026-01-02")
        mock_publish_index.assert_called_once_with(source="datacite", hf_bucket="hf-bucket", s3_client=client)

    def test_raises_when_release_record_missing(self, mocker, releases_table, hf_credentials):
        mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="No release record"):
            publish_releases(
                source="datacite",
                release_date="2026-01-02",
                source_uris=self.SOURCE_URIS,
                hf_bucket="hf-bucket",
                endpoint_url=HF_ENDPOINT_URL,
            )

        mock_copy.assert_not_called()

    def test_publishes_only_datasets_given(self, mocker, releases_table, hf_credentials):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02")
        persist_release(DATACITE_AFFILIATIONS_ENRICHMENT, "2026-01-02")
        client = mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")
        mocker.patch("comet.exports.publish_index")
        funders_uri = self.SOURCE_URIS[DATACITE_FUNDERS_ENRICHMENT.identifier]

        publish_releases(
            source="datacite",
            release_date="2026-01-02",
            source_uris={DATACITE_FUNDERS_ENRICHMENT.identifier: funders_uri},
            hf_bucket="hf-bucket",
            endpoint_url=HF_ENDPOINT_URL,
        )

        mock_copy.assert_called_once_with(
            source_uri=funders_uri,
            hf_bucket="hf-bucket",
            hf_prefix="datacite/funders/2026-01-02/full/",
            endpoint_url=HF_ENDPOINT_URL,
            s3_client=client,
            env=mocker.ANY,
        )
        assert not get_release(
            dataset=DATACITE_AFFILIATIONS_ENRICHMENT.identifier, release_date="2026-01-02"
        ).published_at

    def test_refuses_to_replace_a_release_referenced_by_the_index(self, mocker, releases_table, hf_credentials):
        target = "datacite/funders/2026-01-02/full/"
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02")
        index = {
            "datasets": {
                "datacite": {
                    "funders": {
                        "latest": {"path": target},
                        "releases": [{"path": target}],
                    }
                }
            }
        }
        mock_hf_client(mocker, index)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="already referenced by datacite/index.json"):
            publish_releases(
                source="datacite",
                release_date="2026-01-02",
                source_uris={DATACITE_FUNDERS_ENRICHMENT.identifier: "s3://data-bucket/run-1/"},
                hf_bucket="hf-bucket",
                endpoint_url=HF_ENDPOINT_URL,
            )

        mock_copy.assert_not_called()

    def test_fails_before_any_copy_when_credentials_missing(self, mocker, monkeypatch):
        monkeypatch.delenv("HF_S3_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("HF_S3_SECRET_ACCESS_KEY", raising=False)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="HF_S3_ACCESS_KEY_ID"):
            publish_releases(
                source="datacite",
                release_date="2026-01-02",
                source_uris=self.SOURCE_URIS,
                hf_bucket="hf-bucket",
                endpoint_url=HF_ENDPOINT_URL,
            )

        mock_copy.assert_not_called()

    def test_raises_on_unknown_dataset(self, mocker, releases_table):
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="Unknown dataset"):
            publish_releases(
                source="datacite",
                release_date="2026-01-02",
                source_uris={"mystery-dataset": "s3://data-bucket/mystery/run-1/"},
                hf_bucket="hf-bucket",
                endpoint_url=HF_ENDPOINT_URL,
            )

        mock_copy.assert_not_called()

    def test_raises_on_unknown_source(self, mocker, releases_table):
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(ValueError, match="Unknown source"):
            publish_releases(
                source="mystery",
                release_date="2026-01-02",
                source_uris=self.SOURCE_URIS,
                hf_bucket="hf-bucket",
                endpoint_url=HF_ENDPOINT_URL,
            )

        mock_copy.assert_not_called()
