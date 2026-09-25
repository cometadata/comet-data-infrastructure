from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from sceptre.exceptions import SceptreException

from comet_resolvers.ecr_image import EcrImage

URI = "123456789012.dkr.ecr.us-east-1.amazonaws.com/comet-dev-batch"
DIGEST = "sha256:" + "a" * 64


def make_resolver(call):
    resolver = EcrImage({"repository": "comet-dev-batch", "tag": "0.1.0"})
    resolver.stack = SimpleNamespace(connection_manager=SimpleNamespace(call=call))
    return resolver


def test_resolves_tag_to_digest_uri():
    def call(service, command, kwargs):
        assert service == "ecr"
        if command == "describe_images":
            assert kwargs == {"repositoryName": "comet-dev-batch", "imageIds": [{"imageTag": "0.1.0"}]}
            return {"imageDetails": [{"imageDigest": DIGEST}]}
        assert command == "describe_repositories"
        return {"repositories": [{"repositoryUri": URI}]}

    assert make_resolver(call).resolve() == f"{URI}@{DIGEST}"


def test_missing_tag_names_the_image():
    def call(service, command, kwargs):
        raise ClientError({"Error": {"Code": "ImageNotFoundException", "Message": "no"}}, command)

    with pytest.raises(SceptreException, match="comet-dev-batch:0.1.0 does not exist"):
        make_resolver(call).resolve()
