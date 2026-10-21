from types import SimpleNamespace

import pytest
from sceptre.exceptions import SceptreException

from comet_resolvers.prefix_list import PrefixList


def make_resolver(call, argument="s3"):
    resolver = PrefixList(argument)
    resolver.stack = SimpleNamespace(region="us-east-1", connection_manager=SimpleNamespace(call=call))
    return resolver


class TestPrefixList:
    def test_resolves_service_to_prefix_list_id_in_the_stack_region(self):
        def call(service, command, kwargs):
            assert service == "ec2"
            assert command == "describe_managed_prefix_lists"
            assert kwargs == {"Filters": [{"Name": "prefix-list-name", "Values": ["com.amazonaws.us-east-1.s3"]}]}
            return {"PrefixLists": [{"PrefixListId": "pl-63a5400a"}]}

        assert make_resolver(call).resolve() == "pl-63a5400a"

    def test_missing_prefix_list_names_the_list(self):
        def call(service, command, kwargs):
            return {"PrefixLists": []}

        with pytest.raises(SceptreException, match="com.amazonaws.us-east-1.s3 not found"):
            make_resolver(call).resolve()

    @pytest.mark.parametrize("argument", [None, "", {"service": "s3"}], ids=["bare", "empty", "mapping"])
    def test_rejects_arguments_that_are_not_a_service_name(self, argument):
        def call(service, command, kwargs):
            raise AssertionError("should not call AWS")

        with pytest.raises(SceptreException, match="needs a service name"):
            make_resolver(call, argument).resolve()
