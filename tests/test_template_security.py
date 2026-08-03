import re

import yaml

from conftest import PROJECT_DIR, TaggedValue, resources_of_type

# These roles define and enforce the deployment boundary, so environment stacks cannot manage them.
UNBOUNDED_ROLES = {
    ("bootstrap/deployment-roles.yaml", "DeploymentRunnerRole"),
    ("bootstrap/deployment-roles.yaml", "DeploymentServiceRole"),
}

# These IAM grants require the workload boundary to prevent privilege escalation.
GRANTING_ACTIONS = {
    "iam:AttachRolePolicy",
    "iam:CreateRole",
    "iam:PutRolePermissionsBoundary",
    "iam:PutRolePolicy",
}

# IAM resource types created by environment stacks and the property that would pin their names.
NAMED_IAM_PROPERTIES = {
    "AWS::IAM::Role": "RoleName",
    "AWS::IAM::InstanceProfile": "InstanceProfileName",
    "AWS::IAM::ManagedPolicy": "ManagedPolicyName",
}

WORKLOAD_IAM_PATH = re.compile(r"^/comet/\$\{Env\}/([a-z0-9-]+/)+$")

BOOTSTRAP_IAM_ARNS = {
    "arn:${AWS::Partition}:iam::${AWS::AccountId}:policy/comet/bootstrap/${Env}/*",
    "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/comet/bootstrap/${Env}/*",
}


def statement_actions(statement):
    action = statement["Action"]
    return {action} if isinstance(action, str) else set(action)


def statement_resources(statement):
    resource = statement["Resource"]
    resources = resource if isinstance(resource, list) else [resource]
    return {r.value if isinstance(r, TaggedValue) else r for r in resources}


def policy_statements(role, policy_name):
    policy = next(p for p in role["Properties"]["Policies"] if p["PolicyName"] == policy_name)
    return policy["PolicyDocument"]["Statement"]


class TestDeploymentPermissions:
    def test_every_environment_role_carries_the_boundary(self, resources):
        missing = [
            f"{template}:{logical_id}"
            for template, logical_id, role in resources_of_type(resources, "AWS::IAM::Role")
            if (template, logical_id) not in UNBOUNDED_ROLES and "PermissionsBoundary" not in role["Properties"]
        ]
        assert not missing

    def test_role_grants_require_the_boundary(self, rendered_templates):
        service_role = rendered_templates["bootstrap/deployment-roles.yaml"]["Resources"]["DeploymentServiceRole"]
        statements = policy_statements(service_role, "iam")

        escalation_gate = next(
            s for s in statements if s["Effect"] == "Deny" and "Action" in s and statement_actions(s) == GRANTING_ACTIONS
        )
        assert escalation_gate["Resource"] == "*"
        assert escalation_gate["Condition"]["StringNotEquals"]["iam:PermissionsBoundary"] == TaggedValue(
            "Ref", "BoundaryArn"
        )

        flat_deny = next(
            s for s in statements if s["Effect"] == "Deny" and "Action" in s and "Condition" not in s
        )
        assert {
            "iam:AttachUserPolicy",
            "iam:CreateAccessKey",
            "iam:CreateUser",
            "iam:DeleteRolePermissionsBoundary",
            "iam:PutUserPolicy",
        } <= statement_actions(flat_deny)

    def test_bootstrap_iam_is_protected_from_the_deployment_path(self, rendered_templates):
        boundary = rendered_templates["bootstrap/workload-boundary.yaml"]["Resources"]["WorkloadBoundary"]
        boundary_deny = next(
            s
            for s in boundary["Properties"]["PolicyDocument"]["Statement"]
            if s["Effect"] == "Deny" and statement_actions(s) == {"iam:*"}
        )
        assert statement_resources(boundary_deny) == BOOTSTRAP_IAM_ARNS

        service_role = rendered_templates["bootstrap/deployment-roles.yaml"]["Resources"]["DeploymentServiceRole"]
        service_deny = next(
            s for s in policy_statements(service_role, "iam") if s["Effect"] == "Deny" and "NotAction" in s
        )
        assert service_deny["NotAction"] == "iam:PassRole"
        assert statement_resources(service_deny) == BOOTSTRAP_IAM_ARNS

    def test_runner_delegates_provisioning_through_the_service_role(self, rendered_templates):
        runner = rendered_templates["bootstrap/deployment-roles.yaml"]["Resources"]["DeploymentRunnerRole"]
        statements = policy_statements(runner, "deploy")

        pass_role_statements = [s for s in statements if statement_actions(s) == {"iam:PassRole"}]
        services = {s["Condition"]["StringEquals"]["iam:PassedToService"] for s in pass_role_statements}
        resources = {r for s in pass_role_statements for r in statement_resources(s)}
        assert services == {"cloudformation.amazonaws.com", "ecs-tasks.amazonaws.com"}
        assert resources == {
            "DeploymentServiceRole.Arn",
            "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/comet/${Env}/airflow/service/*",
        }

        stack_mutation = next(
            s
            for s in statements
            if statement_actions(s)
            == {"cloudformation:CreateStack", "cloudformation:DeleteStack", "cloudformation:UpdateStack"}
        )
        assert stack_mutation["Condition"]["StringEquals"]["cloudformation:RoleArn"].value == (
            "DeploymentServiceRole.Arn"
        )
        assert stack_mutation["Resource"].value.endswith("stack/comet-${Env}-*/*")

    def test_deploy_project_uses_the_bootstrap_runner(self, rendered_templates):
        template = rendered_templates["deploy.j2"]
        project = template["Resources"]["DeployProject"]["Properties"]

        assert "DeployRole" not in template["Resources"]
        assert project["ServiceRole"].value == "DeploymentRunnerRoleArn"
        assert set(template["Parameters"]) >= {"DeploymentRunnerRoleArn", "Env"}

    def test_workload_iam_resources_use_generated_names_under_the_environment_path(self, resources):
        problems = []
        for template, logical_id, resource in resources:
            name_property = NAMED_IAM_PROPERTIES.get(resource["Type"])
            if name_property is None or template.startswith("bootstrap/"):
                continue
            properties = resource["Properties"]
            if name_property in properties:
                problems.append(f"{template}:{logical_id} pins {name_property}")
            path = properties.get("Path")
            if not (isinstance(path, TaggedValue) and WORKLOAD_IAM_PATH.match(path.value)):
                problems.append(f"{template}:{logical_id} path is not under /comet/${{Env}}/")

        assert not problems, problems

    def test_bootstrap_iam_resources_use_generated_names_under_the_bootstrap_path(self, rendered_templates):
        bootstrap = {
            **rendered_templates["bootstrap/workload-boundary.yaml"]["Resources"],
            **rendered_templates["bootstrap/deployment-roles.yaml"]["Resources"],
        }
        expected_paths = {
            "WorkloadBoundary": "/comet/bootstrap/${Env}/",
            "DeploymentServiceRole": "/comet/bootstrap/${Env}/cloudformation/",
            "DeploymentRunnerRole": "/comet/bootstrap/${Env}/runner/",
        }
        assert set(bootstrap) == set(expected_paths)
        for logical_id, expected_path in expected_paths.items():
            properties = bootstrap[logical_id]["Properties"]
            name_property = NAMED_IAM_PROPERTIES[bootstrap[logical_id]["Type"]]
            assert name_property not in properties
            assert properties["Path"].value == expected_path

    def test_example_does_not_guess_generated_bootstrap_arns(self):
        variables = yaml.safe_load((PROJECT_DIR / "vars-dev.yaml.example").read_text())

        assert variables["permissions_boundary_arn"] == ""
        assert variables["cloudformation_service_role_arn"] == ""
        assert variables["deployment_runner_role_arn"] == ""
