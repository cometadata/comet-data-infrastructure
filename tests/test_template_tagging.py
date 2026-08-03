import json

from conftest import resources_of_type, tag_keys


class TestTagging:
    required_scope_tags = {"Environment", "Service"}

    def test_monitoring_resources_rely_on_inherited_stack_tags(self, resources):
        manually_tagged = [
            f"{name}:{logical_id}"
            for name, logical_id, resource in resources
            if name.startswith("monitoring/")
            if {"Tags", "ResourceTags"} & resource["Properties"].keys()
        ]

        assert not manually_tagged, f"monitoring resources with manual tags: {manually_tagged}"

    def test_tag_propagation_switches_are_enabled(self, resources):
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

        for name, logical_id, resource in resources_of_type(resources, "AWS::Batch::ComputeEnvironment"):
            tags = resource["Properties"]["ComputeResources"].get("Tags", {})
            if not self.required_scope_tags <= set(tags):
                problems.append(f"{name}:{logical_id} compute environment does not tag instances with scope tags")

        assert not problems, problems

    def test_cloud_map_and_notification_rules_rely_on_inherited_stack_tags(self, resources):
        inherited_types = {
            "AWS::CodeStarNotifications::NotificationRule",
            "AWS::ServiceDiscovery::PrivateDnsNamespace",
            "AWS::ServiceDiscovery::Service",
        }
        manually_tagged = [
            f"{name}:{logical_id}"
            for name, logical_id, resource in resources
            if resource["Type"] in inherited_types
            if "Tags" in resource["Properties"]
        ]

        assert not manually_tagged

    def test_ecr_retention_only_expires_sha_and_untagged_images(self, rendered_templates):
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
                    "tagStatus": "untagged",
                    "countType": "imageCountMoreThan",
                    "countNumber": 3,
                },
            ]
