import json

from conftest import resources_of_type, tag_keys, tag_values


class TestTagging:
    required_scope_tags = {"Environment", "Service", "Subservice"}
    subservices = {"platform", "jobs", "build", "dev-instance"}
    # These stacks mix subservices, so their resources set Subservice individually.
    per_resource_subservice_stacks = {"dev/airflow.yaml", "dev/s3.yaml"}
    # CloudFormation exposes no Tags property for these types.
    untaggable_types = {"AWS::IAM::ManagedPolicy"}

    def test_stacks_set_subservice_only_when_shared_by_all_resources(self, stack_configs):
        for path, config in stack_configs.items():
            tags = config.get("stack_tags", {})
            if path in self.per_resource_subservice_stacks:
                assert "Subservice" not in tags, path
            else:
                assert tags.get("Subservice") in self.subservices, path

    def test_mixed_stack_resources_each_set_one_subservice(self, stack_configs, rendered_templates):
        problems = []

        for path in self.per_resource_subservice_stacks:
            template = stack_configs[path]["template"]["path"]
            for logical_id, resource in rendered_templates[template]["Resources"].items():
                tags = resource["Properties"].get("Tags")
                if resource["Type"] in self.untaggable_types:
                    if tags is not None:
                        problems.append(f"{template}:{logical_id} sets Tags on an untaggable type")
                    continue
                allocation = [tag["Value"] for tag in tags or [] if tag["Key"] == "Subservice"]
                if len(allocation) != 1 or allocation[0] not in self.subservices:
                    problems.append(f"{template}:{logical_id} has Subservice {allocation}")

        assert not problems, problems

    def test_single_subservice_stack_templates_match_their_stack_tag(self, stack_configs, rendered_templates):
        problems = []

        for path, config in stack_configs.items():
            if path in self.per_resource_subservice_stacks:
                continue
            subservice = config["stack_tags"]["Subservice"]
            template = config["template"]["path"]
            for logical_id, resource in rendered_templates[template]["Resources"].items():
                values = set(tag_values(resource["Properties"], "Subservice"))
                if values - {subservice}:
                    problems.append(f"{template}:{logical_id} tags Subservice {values} but the stack is {subservice}")

        assert not problems, problems

    def test_monitoring_resources_rely_on_inherited_stack_tags(self, resources):
        manually_tagged = [
            f"{name}:{logical_id}"
            for name, logical_id, resource in resources
            if name.startswith("monitoring/")
            if {"Tags", "ResourceTags"} & resource["Properties"].keys()
        ]

        assert not manually_tagged, f"monitoring resources with manual tags: {manually_tagged}"

    def test_resources_propagate_scope_tags_to_compute(self, resources):
        problems = []

        for name, logical_id, resource in resources_of_type(resources, "AWS::EC2::LaunchTemplate"):
            properties = resource["Properties"]
            template_specs = properties.get("TagSpecifications", [])
            data_specs = properties["LaunchTemplateData"].get("TagSpecifications", [])
            for resource_type, specs in [
                ("launch-template", template_specs),
                ("instance", data_specs),
                ("volume", data_specs),
            ]:
                tags = [tag_keys(s.get("Tags")) for s in specs if s.get("ResourceType") == resource_type]
                if not any(self.required_scope_tags <= keys for keys in tags):
                    problems.append(f"{name}:{logical_id} does not tag {resource_type} resources with scope tags")

        for name, logical_id, resource in resources_of_type(resources, "AWS::ECS::Service"):
            if resource["Properties"].get("PropagateTags") != "SERVICE":
                problems.append(f"{name}:{logical_id} ECS service does not propagate tags to tasks")

        for name, logical_id, resource in resources_of_type(resources, "AWS::ECS::TaskDefinition"):
            if not self.required_scope_tags <= tag_keys(resource["Properties"].get("Tags")):
                problems.append(f"{name}:{logical_id} task definition is missing scope tags")

        for name, logical_id, resource in resources_of_type(resources, "AWS::Batch::ComputeEnvironment"):
            tags = resource["Properties"]["ComputeResources"].get("Tags", {})
            if not self.required_scope_tags <= set(tags):
                problems.append(f"{name}:{logical_id} compute environment does not tag instances with scope tags")

        assert not problems, problems

    def test_notification_rules_rely_on_inherited_stack_tags(self, resources):
        manually_tagged = [
            f"{name}:{logical_id}"
            for name, logical_id, resource in resources_of_type(
                resources, "AWS::CodeStarNotifications::NotificationRule"
            )
            if "Tags" in resource["Properties"]
        ]

        assert not manually_tagged

    def test_ecr_retention_only_expires_sha_local_and_untagged_images(self, rendered_templates):
        resources = rendered_templates["ecr.j2"]["Resources"]

        for logical_id in ("BatchRepository", "MarpleRepository", "AirflowRepository"):
            policy = json.loads(resources[logical_id]["Properties"]["LifecyclePolicy"]["LifecyclePolicyText"])
            selections = [rule["selection"] for rule in policy["rules"]]
            assert selections == [
                {
                    "tagStatus": "tagged",
                    "tagPrefixList": ["sha-"],
                    "countType": "imageCountMoreThan",
                    "countNumber": 50,
                },
                {
                    "tagStatus": "tagged",
                    "tagPrefixList": ["local-"],
                    "countType": "imageCountMoreThan",
                    "countNumber": 10,
                },
                {
                    "tagStatus": "untagged",
                    "countType": "imageCountMoreThan",
                    "countNumber": 3,
                },
            ]
