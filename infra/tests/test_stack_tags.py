from pathlib import Path

import yaml
from sceptre.config.reader import ConfigReader
from sceptre.context import SceptreContext

PROJECT_DIR = Path(__file__).parents[2]


def test_stack_tags_add_to_group_tags_instead_of_replacing_them():
    variables = yaml.safe_load((PROJECT_DIR / "vars-dev.yaml.example").read_text())
    # The dev config refuses to render without the bootstrap ARNs, which the example leaves empty.
    variables.update(
        permissions_boundary_arn="arn:aws:iam::123456789012:policy/boundary",
        cloudformation_service_role_arn="arn:aws:iam::123456789012:role/service",
        deployment_runner_role_arn="arn:aws:iam::123456789012:role/runner",
    )
    context = SceptreContext(project_path=str(PROJECT_DIR / "infra"), command_path="dev", user_variables=variables)
    stacks, _ = ConfigReader(context).construct_stacks()

    group_tags = {**variables["stack_tags"], "Environment": "dev", "Service": "comet"}
    for stack in stacks:
        assert group_tags.items() <= stack.tags.items(), stack.name
