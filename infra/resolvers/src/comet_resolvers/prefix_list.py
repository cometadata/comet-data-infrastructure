from sceptre.exceptions import SceptreException
from sceptre.resolvers import Resolver


class PrefixList(Resolver):
    """Resolve an AWS-managed prefix list ID in the stack's region, e.g. !prefix_list s3."""

    def resolve(self):
        """Return the service's AWS-managed prefix list ID in the stack's region."""
        if not isinstance(self.argument, str) or not self.argument:
            raise SceptreException("prefix_list needs a service name, for example !prefix_list s3")
        name = f"com.amazonaws.{self.stack.region}.{self.argument}"
        response = self.stack.connection_manager.call(
            service="ec2",
            command="describe_managed_prefix_lists",
            kwargs={"Filters": [{"Name": "prefix-list-name", "Values": [name]}]},
        )
        prefix_lists = response["PrefixLists"]
        if not prefix_lists:
            raise SceptreException(f"Prefix list {name} not found")
        return prefix_lists[0]["PrefixListId"]
