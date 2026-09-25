from botocore.exceptions import ClientError
from sceptre.exceptions import SceptreException
from sceptre.resolvers import Resolver


class EcrImage(Resolver):
    """Digest-pinned URI of an ECR repository at the given tag.

    Argument: {repository: <repository name>, tag: <image tag>}. The tag may be a
    nested resolver, for example !ssm.
    """

    def resolve(self):
        """Return the repository URI pinned to the digest of the tagged image."""
        repository = self.argument["repository"]
        tag = self.argument["tag"]

        try:
            images = self._call("describe_images", repositoryName=repository, imageIds=[{"imageTag": tag}])
        except ClientError as error:
            if error.response["Error"]["Code"] == "ImageNotFoundException":
                raise SceptreException(
                    f"{repository}:{tag} does not exist. Run make promote with a tag present in every repository."
                ) from error
            raise
        digest = images["imageDetails"][0]["imageDigest"]

        repositories = self._call("describe_repositories", repositoryNames=[repository])
        uri = repositories["repositories"][0]["repositoryUri"]
        return f"{uri}@{digest}"

    def _call(self, command, **kwargs):
        return self.stack.connection_manager.call(service="ecr", command=command, kwargs=kwargs)
