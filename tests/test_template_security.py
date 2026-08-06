def test_deploy_role_cannot_pass_environment_roles_to_cloudformation(rendered_templates):
    template = rendered_templates["deploy.j2"]
    resources = rendered_templates["deploy.j2"]["Resources"]

    managed_policies = resources["DeployRole"]["Properties"]["ManagedPolicyArns"]
    assert isinstance(managed_policies, list)
    assert len(managed_policies) == 1
    assert "PowerUserAccess" in str(managed_policies[0])

    deployment_policy = resources["DeploymentIamPolicy"]
    pass_role = next(
        statement
        for statement in deployment_policy["Properties"]["PolicyDocument"]["Statement"]
        if statement["Action"] == "iam:PassRole"
    )
    assert "cloudformation.amazonaws.com" not in str(pass_role["Condition"])
    assert "ProductionDeploymentPolicy" not in resources
    assert "CloudFormationServiceRoleArn" not in template["Parameters"]
