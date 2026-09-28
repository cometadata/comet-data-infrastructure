import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from comet_deploy.settings import load_variables, put_parameter, sync_vars

EXPECTED_TAGS = {
    "CodeRepo": "https://github.com/cometadata/comet-data-infrastructure",
    "Contact": "someone",
    "Environment": "dev",
    "Service": "comet",
    "Subservice": "build",
}


class TestSyncVars:
    @pytest.mark.parametrize("existing", [False, True], ids=["new", "existing"])
    def test_stores_the_file_as_a_tagged_secure_string(self, variables, tmp_path, existing):
        ssm = boto3.client("ssm")
        name = "/test/comet/dev/vars-dev.yaml"
        if existing:
            ssm.put_parameter(Name=name, Value="old", Type="SecureString")

        sync_vars("dev")

        parameter = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]
        assert parameter["Type"] == "SecureString"
        assert parameter["Value"] == (tmp_path / "vars-dev.yaml").read_text()
        tag_list = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)["TagList"]
        assert {tag["Key"]: tag["Value"] for tag in tag_list} == EXPECTED_TAGS


class TestPutParameter:
    @pytest.mark.parametrize("failure", ["AccessDeniedException", "EndpointConnectionError"])
    def test_tagging_failure_leaves_the_parameter_unchanged(self, variables, monkeypatch, failure, capsys):
        ssm = boto3.client("ssm")
        name = "/test/comet/dev/config"
        ssm.put_parameter(Name=name, Value="old", Type="SecureString")
        value = "private-parameter-contents"

        def deny_tags(**kwargs):
            if failure == "EndpointConnectionError":
                raise EndpointConnectionError(endpoint_url="https://ssm.example.com")
            raise ClientError({"Error": {"Code": failure, "Message": value}}, "AddTagsToResource")

        monkeypatch.setattr(ssm, "add_tags_to_resource", deny_tags)
        monkeypatch.setattr("comet_deploy.settings.boto3.client", lambda *args, **kwargs: ssm)

        with pytest.raises(SystemExit) as exc:
            put_parameter(variables, name, value, "SecureString")

        message = str(exc.value)
        assert name in message
        assert "tagging failed" in message
        assert failure in message
        captured = capsys.readouterr()
        assert value not in message + captured.out + captured.err
        assert ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"] == "old"


class TestLoadVariables:
    @pytest.mark.parametrize(
        ("content", "message"),
        [
            pytest.param("", "vars-dev.yaml is empty.", id="empty"),
            pytest.param("env: dev\nregion: us-east-1\nssm_prefix: /x\n", "is missing stack_tags", id="missing"),
            pytest.param(
                "env: dev\nregion: us-east-1\nssm_prefix: /x\nstack_tags:\n", "is missing stack_tags", id="null"
            ),
            pytest.param(
                "env: prod\nregion: us-east-1\nssm_prefix: /x\nstack_tags: {}\n", "has env prod", id="other-env"
            ),
            pytest.param('env: "dev\n', "vars-dev.yaml: ", id="malformed"),
            pytest.param(
                "env: dev\nregion: us-east-1\nssm_prefix: /x\nstack_tags:\n  - Contact: x\n",
                "stack_tags must be a mapping",
                id="list-tags",
            ),
        ],
    )
    def test_rejects_an_unusable_file(self, tmp_path, monkeypatch, content, message):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "vars-dev.yaml").write_text(content)

        with pytest.raises(SystemExit, match=message):
            load_variables("dev")
