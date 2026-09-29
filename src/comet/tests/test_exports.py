import datetime
from io import BytesIO
import json
from types import SimpleNamespace

import boto3
from botocore.exceptions import ClientError
import pytest

from comet.constants import (
    DATACITE_AFFILIATIONS_ENRICHMENT,
    DATACITE_FUNDERS_ENRICHMENT,
    DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT,
    Source,
    Enrichment,
)
from comet.dynamodb_store import get_release, mark_published, persist_discovered_release
from comet.exports import (
    copy_release_to_hf,
    hf_env,
    hf_s3_client,
    index_key,
    publish_index,
    publish_releases,
    release_prefix,
    render_index,
)
from comet.model.dataset_version_model import DatasetRelease

HF_ENDPOINT_URL = "https://s3.example.com"
DATA_BUCKET = "data-bucket"


@pytest.fixture
def hf_credentials(monkeypatch):
    monkeypatch.setenv("HF_S3_ACCESS_KEY_ID", "hf-key")
    monkeypatch.setenv("HF_S3_SECRET_ACCESS_KEY", "hf-secret")


def persist_release(
    enrichment: Enrichment,
    date_str: str,
    published: bool = False,
    diff: bool = False,
    diff_base: str | None = None,
):
    """Persist an enrichment release record; when published, its diff is published too."""
    release = DatasetRelease(release_date=datetime.date.fromisoformat(date_str))
    prefix = f"enrich/run-{date_str}/"
    record = persist_discovered_release(
        dataset=enrichment.identifier,
        release=release,
        run_id=f"run-{date_str}",
        source_prefix=prefix,
        full_source_prefix=prefix + "full/",
        diff_source_prefix=prefix + "diff/" if diff else None,
        diff_base_release_date=diff_base,
    )
    if published:
        mark_published(
            dataset=enrichment.identifier,
            release_date=date_str,
            export_path=release_prefix(enrichment, date_str, "full"),
            diff_export_path=release_prefix(enrichment, date_str, "diff") if diff else None,
            expected_updated_at=record.updated_at,
        )


def publish(datasets: list[str], release_date: str = "2026-01-02"):
    publish_releases(
        source="datacite",
        release_date=release_date,
        datasets=datasets,
        data_bucket=DATA_BUCKET,
        hf_bucket="hf-bucket",
        endpoint_url=HF_ENDPOINT_URL,
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


def published_record(date_str: str, enrichment: Enrichment = DATACITE_FUNDERS_ENRICHMENT, diff: bool = False):
    return SimpleNamespace(
        release_date=date_str,
        published_at=f"{date_str}T12:00:00+00:00",
        export_path=release_prefix(enrichment, date_str, "full"),
        diff_export_path=release_prefix(enrichment, date_str, "diff") if diff else None,
    )


@pytest.mark.parametrize(
    ("enrichment", "release_type", "expected"),
    [
        (DATACITE_FUNDERS_ENRICHMENT, "full", "datacite/funders/2026-01-02/full/"),
        (DATACITE_FUNDERS_ENRICHMENT, "diff", "datacite/funders/2026-01-02/diff/"),
        (
            Enrichment(Source("openalex-works", 1), "affiliations", 3),
            "full",
            "openalex-works/affiliations/2026-01-02/full/",
        ),
    ],
)
def test_release_prefix_joins_source_method_date_and_type(enrichment, release_type, expected):
    assert release_prefix(enrichment, "2026-01-02", release_type) == expected


class TestHfEnv:
    def test_remaps_keys_and_drops_session_token(self, hf_credentials, monkeypatch):
        monkeypatch.setenv("AWS_SESSION_TOKEN", "task-role-token")
        monkeypatch.setenv("AWS_REGION", "eu-west-1")

        env = hf_env()

        assert env["AWS_ACCESS_KEY_ID"] == "hf-key"
        assert env["AWS_SECRET_ACCESS_KEY"] == "hf-secret"
        assert env["AWS_REGION"] == "us-east-1"
        assert "AWS_SESSION_TOKEN" not in env


class TestHfS3Client:
    def test_configures_hugging_face_s3_compatibility(self, hf_credentials, mocker):
        mock_client = mocker.patch("comet.exports.boto3.client")

        hf_s3_client(HF_ENDPOINT_URL)

        mock_client.assert_called_once()
        args, kwargs = mock_client.call_args
        assert args == ("s3",)
        assert kwargs["endpoint_url"] == HF_ENDPOINT_URL
        assert kwargs["aws_access_key_id"] == "hf-key"
        assert kwargs["aws_secret_access_key"] == "hf-secret"
        config = kwargs["config"]
        assert config.region_name == "us-east-1"
        assert config.s3 == {"addressing_style": "path"}
        assert config.request_checksum_calculation == "when_required"
        assert config.response_checksum_validation == "when_required"


class TestRenderIndex:
    def test_diff_entries_sort_before_fulls_and_latest_is_the_newest_full(self):
        records = [
            published_record("2026-02-02", diff=True),
            published_record("2026-01-02"),
            SimpleNamespace(release_date="2026-03-02", published_at=None, export_path=None, diff_export_path=None),
        ]

        index = render_index("datacite", {DATACITE_FUNDERS_ENRICHMENT: records})

        funders = index["datasets"]["datacite"]["funders"]
        assert [(e["release_date"], e["type"], e["path"]) for e in funders["releases"]] == [
            ("2026-01-02", "full", "datacite/funders/2026-01-02/full/"),
            ("2026-02-02", "diff", "datacite/funders/2026-02-02/diff/"),
            ("2026-02-02", "full", "datacite/funders/2026-02-02/full/"),
        ]
        assert all(e["published_at"] for e in funders["releases"])
        assert funders["latest"] == {
            "release_date": "2026-02-02",
            "type": "full",
            "path": "datacite/funders/2026-02-02/full/",
        }

    def test_omits_enrichments_with_nothing_published(self):
        records = {
            DATACITE_FUNDERS_ENRICHMENT: [published_record("2026-01-02")],
            DATACITE_AFFILIATIONS_ENRICHMENT: [
                SimpleNamespace(release_date="2026-01-02", published_at=None, export_path=None, diff_export_path=None)
            ],
        }

        index = render_index("datacite", records)

        assert index["schema_version"] == 1
        assert set(index["datasets"]["datacite"]) == {"funders"}


class TestPublishIndex:
    def test_uploads_index_rendered_from_the_releases_table(self, releases_table):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", published=True)
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-02-02", published=True, diff=True)
        persist_release(DATACITE_AFFILIATIONS_ENRICHMENT, "2026-01-02", published=False)
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket="hf-bucket")

        publish_index(source="datacite", hf_bucket="hf-bucket", s3_client=s3_client)

        body = s3_client.get_object(Bucket="hf-bucket", Key=index_key("datacite"))["Body"].read()
        index = json.loads(body)
        assert index["updated_at"] is not None
        assert set(index["datasets"]["datacite"].keys()) == {"funders"}
        funders = index["datasets"]["datacite"]["funders"]
        assert funders["latest"]["path"] == "datacite/funders/2026-02-02/full/"
        assert [(r["release_date"], r["type"]) for r in funders["releases"]] == [
            ("2026-01-02", "full"),
            ("2026-02-02", "diff"),
            ("2026-02-02", "full"),
        ]


class TestCopyReleaseToHf:
    SOURCE_URI = "s3://data-bucket/datacite_enrich_funders/run-1/full/"

    def test_checks_the_manifest_then_stages_cleans_target_and_uploads(self, mocker, tmp_path):
        stage = tmp_path / "stage"
        client = mocker.Mock()
        env = {"AWS_ACCESS_KEY_ID": "hf-key"}
        mock_has_files = mocker.patch("comet.exports.s3_uri_has_files", return_value=True)
        mocker.patch("comet.exports.local_dir_for_uri", return_value=stage)
        mock_download = mocker.patch("comet.exports.s5cmd_download_files")
        mock_clean = mocker.patch("comet.exports.s5cmd_clean_prefix")
        mock_upload = mocker.patch("comet.exports.s5cmd_upload_files")

        copy_release_to_hf(
            source_uri=self.SOURCE_URI,
            hf_bucket="hf-bucket",
            hf_prefix="datacite/funders/2026-01-02/full/",
            endpoint_url=HF_ENDPOINT_URL,
            s3_client=client,
            env=env,
        )

        target_uri = "s3://hf-bucket/datacite/funders/2026-01-02/full/"
        mock_has_files.assert_called_once_with(self.SOURCE_URI + "manifest.json")
        mock_download.assert_called_once_with(self.SOURCE_URI + "*", stage)
        mock_clean.assert_called_once_with(target_uri, s3_client=client, endpoint_url=HF_ENDPOINT_URL, env=env)
        mock_upload.assert_called_once_with(stage, target_uri, endpoint_url=HF_ENDPOINT_URL, env=env)
        assert not stage.exists()

    def test_refuses_a_directory_without_a_manifest_before_downloading(self, mocker):
        mocker.patch("comet.exports.s3_uri_has_files", return_value=False)
        mock_download = mocker.patch("comet.exports.s5cmd_download_files")
        mock_upload = mocker.patch("comet.exports.s5cmd_upload_files")

        with pytest.raises(RuntimeError, match="manifest.json"):
            copy_release_to_hf(
                source_uri=self.SOURCE_URI,
                hf_bucket="hf-bucket",
                hf_prefix="datacite/funders/2026-01-02/full/",
                endpoint_url=HF_ENDPOINT_URL,
                s3_client=mocker.Mock(),
                env={},
            )

        mock_download.assert_not_called()
        mock_upload.assert_not_called()


class TestPublishReleases:
    ALL_DATASETS = [
        DATACITE_FUNDERS_ENRICHMENT.identifier,
        DATACITE_AFFILIATIONS_ENRICHMENT.identifier,
        DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier,
    ]

    def test_copies_full_then_diff_marks_the_record_then_uploads_index(self, mocker, releases_table, hf_credentials):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", published=True)
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-02-02", diff=True, diff_base="2026-01-02")
        client = mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")
        mock_publish_index = mocker.patch("comet.exports.publish_index")

        publish([DATACITE_FUNDERS_ENRICHMENT.identifier], release_date="2026-02-02")

        assert [(c.kwargs["source_uri"], c.kwargs["hf_prefix"]) for c in mock_copy.call_args_list] == [
            ("s3://data-bucket/enrich/run-2026-02-02/full/", "datacite/funders/2026-02-02/full/"),
            ("s3://data-bucket/enrich/run-2026-02-02/diff/", "datacite/funders/2026-02-02/diff/"),
        ]
        assert mock_copy.call_args.kwargs["s3_client"] is client
        record = get_release(dataset=DATACITE_FUNDERS_ENRICHMENT.identifier, release_date="2026-02-02")
        assert record.published_at is not None
        assert record.export_path == "datacite/funders/2026-02-02/full/"
        assert record.diff_export_path == "datacite/funders/2026-02-02/diff/"
        mock_publish_index.assert_called_once_with(source="datacite", hf_bucket="hf-bucket", s3_client=client)

    def test_release_without_diff_copies_full_only(self, mocker, releases_table, hf_credentials):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02")
        mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")
        mocker.patch("comet.exports.publish_index")

        publish([DATACITE_FUNDERS_ENRICHMENT.identifier])

        assert [c.kwargs["hf_prefix"] for c in mock_copy.call_args_list] == ["datacite/funders/2026-01-02/full/"]
        record = get_release(dataset=DATACITE_FUNDERS_ENRICHMENT.identifier, release_date="2026-01-02")
        assert record.published_at is not None
        assert record.diff_export_path is None

    def test_publishes_only_unpublished_datasets_given(self, mocker, releases_table, hf_credentials):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", published=True, diff=True)
        persist_release(DATACITE_AFFILIATIONS_ENRICHMENT, "2026-01-02")
        persist_release(DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT, "2026-01-02")
        mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")
        mocker.patch("comet.exports.publish_index")

        publish([DATACITE_FUNDERS_ENRICHMENT.identifier, DATACITE_AFFILIATIONS_ENRICHMENT.identifier])

        assert [c.kwargs["hf_prefix"] for c in mock_copy.call_args_list] == ["datacite/affiliations/2026-01-02/full/"]
        assert not get_release(
            dataset=DATACITE_RESOURCE_TYPE_GENERAL_ENRICHMENT.identifier, release_date="2026-01-02"
        ).published_at

    def test_raises_when_release_record_missing(self, mocker, releases_table, hf_credentials):
        mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="No release record"):
            publish(self.ALL_DATASETS)

        mock_copy.assert_not_called()

    def test_raises_when_record_has_no_full_directory(self, mocker, releases_table, hf_credentials):
        persist_discovered_release(
            dataset=DATACITE_FUNDERS_ENRICHMENT.identifier,
            release=DatasetRelease(release_date=datetime.date(2026, 1, 2)),
            run_id="run-1",
            source_prefix="enrich/run-1/",
        )
        mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="no full_source_prefix"):
            publish([DATACITE_FUNDERS_ENRICHMENT.identifier])

        mock_copy.assert_not_called()

    @pytest.mark.parametrize("release_type", ["full", "diff"])
    def test_refuses_to_replace_a_release_referenced_by_the_index(
        self, mocker, releases_table, hf_credentials, release_type
    ):
        target = release_prefix(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", release_type)
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2025-12-02", published=True)
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", diff=True, diff_base="2025-12-02")
        index = {"datasets": {"datacite": {"funders": {"latest": {}, "releases": [{"path": target}]}}}}
        mock_hf_client(mocker, index)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="already referenced by datacite/index.json"):
            publish([DATACITE_FUNDERS_ENRICHMENT.identifier])

        assert all(c.kwargs["hf_prefix"] != target for c in mock_copy.call_args_list)
        assert not get_release(dataset=DATACITE_FUNDERS_ENRICHMENT.identifier, release_date="2026-01-02").published_at

    @pytest.mark.parametrize(
        ("setup", "newest"),
        [
            (lambda: persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-02-02", published=True), "2026-02-02"),
            (lambda: None, "none"),
        ],
        ids=["newer-release-published", "nothing-published"],
    )
    def test_refuses_diff_whose_baseline_is_not_the_newest_published_release(
        self, mocker, releases_table, hf_credentials, setup, newest
    ):
        setup()
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", diff=True, diff_base="2025-12-02")
        mock_hf_client(mocker)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match=f"cut against 2025-12-02 but the newest published release is {newest}"):
            publish([DATACITE_FUNDERS_ENRICHMENT.identifier])

        mock_copy.assert_not_called()
        assert not get_release(dataset=DATACITE_FUNDERS_ENRICHMENT.identifier, release_date="2026-01-02").published_at

    def test_republishes_a_release_over_its_own_indexed_prefixes(self, mocker, releases_table, hf_credentials):
        persist_release(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", published=True, diff=True)
        persist_discovered_release(
            dataset=DATACITE_FUNDERS_ENRICHMENT.identifier,
            release=DatasetRelease(release_date=datetime.date(2026, 1, 2)),
            run_id="run-again",
            source_prefix="enrich/run-again/",
            full_source_prefix="enrich/run-again/full/",
            replace_published=True,
        )
        full = release_prefix(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", "full")
        diff = release_prefix(DATACITE_FUNDERS_ENRICHMENT, "2026-01-02", "diff")
        index = {"datasets": {"datacite": {"funders": {"latest": {"path": full}, "releases": [{"path": diff}]}}}}
        mock_hf_client(mocker, index)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")
        mocker.patch("comet.exports.publish_index")

        publish([DATACITE_FUNDERS_ENRICHMENT.identifier])

        assert [c.kwargs["hf_prefix"] for c in mock_copy.call_args_list] == [full]
        record = get_release(dataset=DATACITE_FUNDERS_ENRICHMENT.identifier, release_date="2026-01-02")
        assert record.published_at is not None
        assert record.export_path == full
        assert record.diff_export_path is None

    def test_fails_before_any_copy_when_credentials_missing(self, mocker, monkeypatch):
        monkeypatch.delenv("HF_S3_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("HF_S3_SECRET_ACCESS_KEY", raising=False)
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(RuntimeError, match="HF_S3_ACCESS_KEY_ID"):
            publish(self.ALL_DATASETS)

        mock_copy.assert_not_called()

    @pytest.mark.parametrize(
        ("source", "datasets", "error", "match"),
        [
            ("datacite", ["mystery-dataset"], RuntimeError, "Unknown dataset"),
            ("mystery", ALL_DATASETS, ValueError, "Unknown source"),
        ],
        ids=["dataset", "source"],
    )
    def test_raises_on_unknown_source_or_dataset(self, mocker, releases_table, source, datasets, error, match):
        mock_copy = mocker.patch("comet.exports.copy_release_to_hf")

        with pytest.raises(error, match=match):
            publish_releases(
                source=source,
                release_date="2026-01-02",
                datasets=datasets,
                data_bucket=DATA_BUCKET,
                hf_bucket="hf-bucket",
                endpoint_url=HF_ENDPOINT_URL,
            )

        mock_copy.assert_not_called()
