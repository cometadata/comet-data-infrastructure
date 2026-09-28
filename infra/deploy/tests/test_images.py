import hashlib
import json

import boto3
import pytest
from botocore.stub import Stubber

from comet_deploy.cli import app
from comet_deploy.images import promote, repository_names, retag

REPOSITORIES = repository_names("dev")


@pytest.fixture
def ecr():
    client = boto3.client("ecr")
    for repository in REPOSITORIES:
        client.create_repository(repositoryName=repository, imageTagMutability="IMMUTABLE")
    return client


def push(ecr, repository, tag, content):
    """Push a minimal image; pushing the same content again adds a tag to the same image."""
    layer_digest = "sha256:" + hashlib.sha256(content.encode()).hexdigest()
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": 1,
            "digest": "sha256:" + "0" * 64,
        },
        "layers": [
            {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": 1, "digest": layer_digest}
        ],
    }
    ecr.put_image(repositoryName=repository, imageTag=tag, imageManifest=json.dumps(manifest))


def tags(ecr, repository):
    """Map each tag in the repository to its image digest."""
    images = ecr.describe_images(repositoryName=repository)["imageDetails"]
    return {tag: image["imageDigest"] for image in images for tag in image.get("imageTags", [])}


class TestPromote:
    @pytest.mark.parametrize(
        ("repositories", "pushed", "message"),
        [
            ((), (), "Repository comet-dev-batch does not exist"),
            (REPOSITORIES, ("comet-dev-batch", "comet-dev-airflow"), "comet-dev-marple:0.1.0 does not exist"),
        ],
    )
    def test_missing_image_aborts_before_updating_ssm(self, variables, repositories, pushed, message):
        ecr = boto3.client("ecr")
        for repository in repositories:
            ecr.create_repository(repositoryName=repository)
        for repository in pushed:
            push(ecr, repository, "0.1.0", repository)

        with pytest.raises(SystemExit, match=message):
            promote("dev", "0.1.0")

        ssm = boto3.client("ssm")
        with pytest.raises(ssm.exceptions.ParameterNotFound):
            ssm.get_parameter(Name="/test/comet/dev/images/tag")

    def test_stores_the_tag(self, ecr, variables):
        for repository in REPOSITORIES:
            push(ecr, repository, "0.1.0", repository)

        app(["promote", "dev", "0.1.0"], result_action="return_value")

        ssm = boto3.client("ssm")
        name = "/test/comet/dev/images/tag"
        assert ssm.get_parameter(Name=name)["Parameter"]["Value"] == "0.1.0"
        tag_list = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)["TagList"]
        assert {"Key": "Subservice", "Value": "build"} in tag_list


class TestRetag:
    def test_refuses_identical_tags(self, ecr):
        for repository in REPOSITORIES:
            push(ecr, repository, "1.2.3", repository)
        before = {repository: tags(ecr, repository) for repository in REPOSITORIES}

        with pytest.raises(SystemExit, match="must differ"):
            retag("dev", "1.2.3", "1.2.3")

        assert {repository: tags(ecr, repository) for repository in REPOSITORIES} == before

    @pytest.mark.parametrize("failure_code", ["ImageNotFound", "ImageNotReferencedByTag", None])
    def test_manifest_failure_leaves_all_tags_unchanged(self, ecr, monkeypatch, failure_code):
        for repository in REPOSITORIES:
            push(ecr, repository, "sha-abc1234", repository)
        before = {repository: tags(ecr, repository) for repository in REPOSITORIES}
        version_missing = {"imageId": {"imageTag": "0.1.0"}, "failureCode": "ImageNotFound", "failureReason": ""}
        response = {"images": [], "failures": []}
        if failure_code:
            response["failures"].append(
                {
                    "imageId": {"imageTag": "sha-abc1234"},
                    "failureCode": failure_code,
                    "failureReason": "source unavailable",
                }
            )
        monkeypatch.setattr("comet_deploy.images.boto3.client", lambda *args, **kwargs: ecr)

        with Stubber(ecr) as stub:
            stub.add_response("batch_get_image", {"images": [], "failures": [version_missing]})
            stub.add_response("batch_get_image", response)
            with pytest.raises(SystemExit) as exc:
                retag("dev", "sha-abc1234", "0.1.0")
            stub.assert_no_pending_responses()

        message = str(exc.value)
        assert "comet-dev-batch:sha-abc1234" in message
        if failure_code == "ImageNotFound":
            assert "does not exist" in message
        elif failure_code:
            assert failure_code in message
            assert "source unavailable" in message
        assert {repository: tags(ecr, repository) for repository in REPOSITORIES} == before

    def test_labels_the_source_images_and_removes_the_sha_tag(self, ecr):
        for repository in REPOSITORIES:
            push(ecr, repository, "sha-abc1234", repository)
        digests = {repository: tags(ecr, repository)["sha-abc1234"] for repository in REPOSITORIES}

        app(["retag", "dev", "sha-abc1234", "0.1.0"], result_action="return_value")

        for repository in REPOSITORIES:
            assert tags(ecr, repository) == {"0.1.0": digests[repository]}

    def test_can_be_rerun_after_a_partial_run(self, ecr):
        # Cover both tags present, only the version tag present, and only the source tag present.
        push(ecr, "comet-dev-batch", "sha-abc1234", "batch")
        push(ecr, "comet-dev-batch", "0.1.0", "batch")
        push(ecr, "comet-dev-marple", "0.1.0", "marple")
        push(ecr, "comet-dev-airflow", "sha-abc1234", "airflow")

        retag("dev", "sha-abc1234", "0.1.0")

        for repository in REPOSITORIES:
            assert tags(ecr, repository).keys() == {"0.1.0"}

    def test_refuses_a_version_tag_on_a_different_image(self, ecr):
        for repository in REPOSITORIES:
            push(ecr, repository, "sha-abc1234", repository)
        push(ecr, "comet-dev-marple", "0.1.0", "an older build")

        with pytest.raises(SystemExit, match="comet-dev-marple:0.1.0 points to a different image"):
            retag("dev", "sha-abc1234", "0.1.0")

        assert tags(ecr, "comet-dev-batch").keys() == {"sha-abc1234"}
        assert "sha-abc1234" in tags(ecr, "comet-dev-marple")


class TestTagValidation:
    @pytest.mark.parametrize(
        ("command", "args", "message"),
        [
            pytest.param(promote, ("dev", "safe; touch x"), "SOURCE_TAG has an invalid format.", id="promote-shell"),
            pytest.param(promote, ("dev", ""), "SOURCE_TAG has an invalid format.", id="promote-empty"),
            pytest.param(
                retag, ("dev", "safe; touch x", "1.2.3"), "SOURCE_TAG has an invalid format.", id="retag-shell"
            ),
            pytest.param(retag, ("dev", "sha-abc1234", "1.2"), "VERSION_TAG must be X.Y.Z.", id="retag-version"),
        ],
    )
    def test_rejects_malformed_tags_before_touching_aws(self, command, args, message, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        def unexpected_client(*args, **kwargs):
            pytest.fail("Invalid tags must be rejected before creating an AWS client")

        monkeypatch.setattr(boto3, "client", unexpected_client)
        with pytest.raises(SystemExit, match=message):
            command(*args)
