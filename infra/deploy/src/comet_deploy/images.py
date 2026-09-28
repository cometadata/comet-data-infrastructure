import re

import boto3

from comet_deploy.settings import load_variables, put_parameter

IMAGE_NAMES = ("batch", "marple", "airflow")

TAG_PATTERN = re.compile(r"[A-Za-z0-9._-]+")
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")


def repository_names(env: str) -> list[str]:
    """Return the ECR repository names for the environment's images."""
    return [f"comet-{env}-{name}" for name in IMAGE_NAMES]


def get_image(ecr, repository: str, tag: str) -> dict | None:
    """Return the image with the tag, or None if no image has it."""
    try:
        response = ecr.batch_get_image(repositoryName=repository, imageIds=[{"imageTag": tag}])
    except ecr.exceptions.RepositoryNotFoundException:
        raise SystemExit(f"Repository {repository} does not exist.") from None
    failures = [failure for failure in response.get("failures", []) if failure["failureCode"] != "ImageNotFound"]
    if failures:
        raise SystemExit(f"Could not fetch {repository}:{tag}: {failures}")
    images = response.get("images", [])
    return images[0] if images else None


def promote(env: str, source_tag: str) -> None:
    """Store SOURCE_TAG in the SSM image tag parameter, which selects the image set the next deploy uses.

    Deploys resolve the tag to digests in each repository, so a promoted sha-* tag stops resolving once
    the release pipeline removes it.
    """
    if not TAG_PATTERN.fullmatch(source_tag):
        raise SystemExit("SOURCE_TAG has an invalid format.")

    variables = load_variables(env)
    ecr = boto3.client("ecr", region_name=variables["region"])

    # Check that every repository has SOURCE_TAG before updating SSM.
    for repository in repository_names(env):
        image = get_image(ecr, repository, source_tag)
        if image is None:
            raise SystemExit(f"{repository}:{source_tag} does not exist.")
        print(f"{repository}:{source_tag} -> {image['imageId']['imageDigest']}")

    parameter = f"{variables['ssm_prefix']}/{env}/images/tag"
    put_parameter(variables, parameter, source_tag, "String")
    print(f"{parameter} -> {source_tag}")


def retag(env: str, source_tag: str, version_tag: str) -> None:
    """Add VERSION_TAG to the SOURCE_TAG images and remove the sha tag.

    Released images then sit outside the ECR sha retention rule. Run by the release pipeline, which has
    no vars file, so this uses the AWS region from the environment. Retries skip existing version tags
    and finish removing source tags. If both tags exist, their digests must match.
    """
    if not VERSION_PATTERN.fullmatch(version_tag):
        raise SystemExit("VERSION_TAG must be X.Y.Z.")
    if not TAG_PATTERN.fullmatch(source_tag):
        raise SystemExit("SOURCE_TAG has an invalid format.")
    if source_tag == version_tag:
        raise SystemExit("SOURCE_TAG and VERSION_TAG must differ.")

    ecr = boto3.client("ecr")
    repositories = repository_names(env)

    # Check every repository before changing any of them.
    sources = {}
    for repository in repositories:
        version = get_image(ecr, repository, version_tag)
        source = get_image(ecr, repository, source_tag)
        if version and source and version["imageId"]["imageDigest"] != source["imageId"]["imageDigest"]:
            raise SystemExit(f"{repository}:{version_tag} points to a different image.")
        if not version and not source:
            raise SystemExit(f"{repository}:{source_tag} does not exist.")
        if not version:
            sources[repository] = source

    for repository, image in sources.items():
        ecr.put_image(
            repositoryName=repository,
            imageTag=version_tag,
            imageManifest=image["imageManifest"],
            imageManifestMediaType=image["imageManifestMediaType"],
        )

    # Ignore source tags already removed by an earlier attempt.
    for repository in repositories:
        response = ecr.batch_delete_image(repositoryName=repository, imageIds=[{"imageTag": source_tag}])
        failures = [failure for failure in response.get("failures", []) if failure["failureCode"] != "ImageNotFound"]
        if failures:
            raise SystemExit(f"Could not remove {repository}:{source_tag}: {failures}")
        print(f"{repository}:{version_tag}")
