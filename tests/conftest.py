from dataclasses import dataclass
from pathlib import Path

import jinja2
import pytest
import yaml

PROJECT_DIR = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_DIR / "infra" / "config"
TEMPLATES_DIR = PROJECT_DIR / "infra" / "templates"

# Test values for bootstrap ARNs omitted from vars-dev.yaml.example.
BOOTSTRAP_ARN_STUBS = {
    "permissions_boundary_arn": "arn:aws:iam::123456789012:policy/comet/bootstrap/dev/boundary",
    "cloudformation_service_role_arn": "arn:aws:iam::123456789012:role/comet/bootstrap/dev/cloudformation/service",
    "deployment_runner_role_arn": "arn:aws:iam::123456789012:role/comet/bootstrap/dev/runner/runner",
}

# Matches sceptre_user_data in infra/config/dev/config.yaml.
STUB_USER_DATA = {
    "ssm_path": "/comet/dev",
    "stack_tags": {
        "CodeRepo": "https://github.com/cometadata/comet-data-infrastructure",
        "Contact": "someone",
        "Environment": "dev",
        "Service": "comet",
    },
    "vpc_id": "vpc-00000000",
    "public_subnet": "subnet-00000000",
    "alert_emails": ["alerts@example.org"],
    "permissions_boundary_arn": BOOTSTRAP_ARN_STUBS["permissions_boundary_arn"],
}


def example_variables():
    """Load vars-dev.yaml.example and fill in the bootstrap ARNs it leaves empty."""
    variables = yaml.safe_load((PROJECT_DIR / "vars-dev.yaml.example").read_text())
    variables.update(BOOTSTRAP_ARN_STUBS)
    return variables


@dataclass(frozen=True)
class TaggedValue:
    """YAML value with a CloudFormation or Sceptre short tag."""

    tag: str
    value: object


class TaggedLoader(yaml.SafeLoader):
    """Preserve CloudFormation and Sceptre short tags."""


def construct_tagged_value(loader, suffix, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    else:
        value = loader.construct_mapping(node)
    return TaggedValue(suffix, value)


TaggedLoader.add_multi_constructor("!", construct_tagged_value)


@pytest.fixture(scope="session")
def rendered_templates():
    """Render templates, keyed by template path."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(TEMPLATES_DIR),
        undefined=jinja2.StrictUndefined,
    )
    rendered = {}
    template_paths = [
        *TEMPLATES_DIR.glob("**/*.j2"),
        *(TEMPLATES_DIR / "bootstrap").glob("*.yaml"),
    ]
    for path in sorted(template_paths):
        if "includes" in path.parts:
            continue
        name = path.relative_to(TEMPLATES_DIR).as_posix()
        body = env.get_template(name).render(sceptre_user_data=STUB_USER_DATA)
        rendered[name] = yaml.load(body, Loader=TaggedLoader)
    return rendered


@pytest.fixture(scope="session")
def resources(rendered_templates):
    """Return (template, logical ID, resource) tuples."""
    return [
        (name, logical_id, resource)
        for name, template in rendered_templates.items()
        for logical_id, resource in template.get("Resources", {}).items()
    ]


def resources_of_type(resources, resource_type):
    return [(name, logical_id, r) for name, logical_id, r in resources if r.get("Type") == resource_type]


def tag_keys(tag_list):
    return {tag["Key"] for tag in tag_list or []}


@pytest.fixture(scope="session")
def stack_configs():
    """Render Sceptre configs, keyed by stack path."""
    variables = example_variables()
    stack_group_config = {"sceptre_user_data": {"ssm_path": "/comet/dev"}}
    configs = {}
    for path in sorted((CONFIG_DIR / "dev").glob("**/*.yaml")):
        if path.name == "config.yaml":
            continue
        source = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(path.read_text())
        body = source.render(var=variables, stack_group_config=stack_group_config)
        configs[path.relative_to(CONFIG_DIR).as_posix()] = yaml.load(body, Loader=TaggedLoader)
    return configs


@pytest.fixture(scope="session")
def bootstrap_stack_configs():
    """Render bootstrap Sceptre configs, keyed by stack path."""
    variables = example_variables()
    configs = {}
    for path in sorted((CONFIG_DIR / "bootstrap").glob("*.yaml")):
        source = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(path.read_text())
        body = source.render(var=variables)
        configs[path.name] = yaml.load(body, Loader=TaggedLoader)
    return configs


@pytest.fixture(scope="session")
def alarm_parameter_targets(stack_configs, rendered_templates):
    """Map alarm parameters to referenced resources."""
    alarm_config = stack_configs["dev/monitoring/alarms.yaml"]
    targets = {}
    for parameter, value in alarm_config.get("parameters", {}).items():
        if not isinstance(value, TaggedValue) or value.tag != "stack_output":
            continue
        stack_path, output_name = value.value.split("::", maxsplit=1)
        source_template = stack_configs[stack_path]["template"]["path"]
        output_value = rendered_templates[source_template]["Outputs"][output_name]["Value"]
        if isinstance(output_value, TaggedValue) and output_value.tag == "Ref":
            targets[parameter] = (source_template, output_value.value)
    return targets


def dimensions_by_name(metric):
    return {dimension["Name"]: dimension["Value"] for dimension in metric.get("Dimensions", [])}


def alarm_metrics(resources):
    """Yield concrete metrics used by CloudWatch alarms."""
    for template_name, alarm_id, alarm in resources_of_type(resources, "AWS::CloudWatch::Alarm"):
        properties = alarm["Properties"]
        if "MetricName" in properties:
            yield (
                template_name,
                alarm_id,
                {
                    "Namespace": properties["Namespace"],
                    "MetricName": properties["MetricName"],
                    "Dimensions": properties.get("Dimensions", []),
                },
            )
        for query in properties.get("Metrics", []):
            if "MetricStat" in query:
                yield template_name, alarm_id, query["MetricStat"]["Metric"]


def metric_resource_target(metric, dimension_name, alarm_parameter_targets):
    value = dimensions_by_name(metric).get(dimension_name)
    if isinstance(value, TaggedValue) and value.tag == "Ref":
        return alarm_parameter_targets.get(value.value)
    return None
