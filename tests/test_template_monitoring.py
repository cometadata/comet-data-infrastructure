import datetime
import re
from types import SimpleNamespace

import boto3
import jinja2
import pytest
import yaml

from conftest import (
    PROJECT_DIR,
    STUB_USER_DATA,
    TaggedLoader,
    TaggedValue,
    alarm_metrics,
    metric_resource_target,
    resources_of_type,
)


class TestAlarms:
    def test_every_alarm_notifies_the_alert_topic(self, resources):
        silent = [
            f"{name}:{logical_id}"
            for name, logical_id, resource in resources_of_type(resources, "AWS::CloudWatch::Alarm")
            if not resource["Properties"].get("AlarmActions")
        ]
        assert not silent, f"alarms with no actions: {silent}"

    def test_log_ingestion_uses_a_tag_scoped_insights_query(self, rendered_templates):
        alarm = rendered_templates["monitoring/alarms.j2"]["Resources"]["LogIngestionAlarm"]
        queries = alarm["Properties"]["Metrics"]
        assert len(queries) == 1
        service = STUB_USER_DATA["stack_tags"]["Service"]
        environment = STUB_USER_DATA["stack_tags"]["Environment"]
        assert queries[0]["Expression"].endswith(
            f"WHERE tag.Service = '{service}' AND tag.Environment = '{environment}'"
        )

    def test_every_bucket_has_a_storage_metric(self, resources, alarm_parameter_targets):
        expected = {
            (name, logical_id)
            for name, logical_id, _ in resources_of_type(resources, "AWS::S3::Bucket")
            if name == "s3.j2"
        }
        assert {logical_id for _, logical_id in expected} == {
            "S3Bucket",
            "AirflowDagsBucket",
            "AirflowLogsBucket",
            "ArtifactBucket",
        }
        covered = {
            target
            for _, _, metric in alarm_metrics(resources)
            if metric["Namespace"] == "AWS/S3" and metric["MetricName"] == "BucketSizeBytes"
            if (target := metric_resource_target(metric, "BucketName", alarm_parameter_targets))
        }
        assert expected <= covered, f"buckets missing BucketSizeBytes: {sorted(expected - covered)}"

    def test_total_storage_expression_includes_every_bucket_metric(self, rendered_templates):
        alarm = rendered_templates["monitoring/alarms.j2"]["Resources"]["S3TotalStorageAlarm"]
        queries = alarm["Properties"]["Metrics"]
        [total] = [query for query in queries if query.get("ReturnData")]
        metric_ids = {query["Id"] for query in queries if "MetricStat" in query}

        assert set(total["Expression"].split(" + ")) == metric_ids

    def test_every_db_instance_gets_low_storage_events(self, resources, alarm_parameter_targets):
        expected = {(name, logical_id) for name, logical_id, _ in resources_of_type(resources, "AWS::RDS::DBInstance")}
        covered = set()
        for _, _, subscription in resources_of_type(resources, "AWS::RDS::EventSubscription"):
            properties = subscription["Properties"]
            if "low storage" not in properties.get("EventCategories", []):
                continue
            for source in properties.get("SourceIds", []):
                if isinstance(source, TaggedValue) and source.tag == "Ref":
                    if target := alarm_parameter_targets.get(source.value):
                        covered.add(target)
        assert expected <= covered, f"DB instances missing low storage events: {sorted(expected - covered)}"


class TestAlertTopic:
    def test_budgets_can_publish_to_the_alert_topic(self, rendered_templates):
        template = rendered_templates["monitoring/alerts.j2"]
        topic_policy = template["Resources"]["AlertTopicPolicy"]["Properties"]["PolicyDocument"]

        budget_statements = [
            statement
            for statement in topic_policy["Statement"]
            if statement["Principal"]["Service"] == "budgets.amazonaws.com"
        ]

        assert budget_statements == [
            {
                "Sid": "AllowBudgetsPublish",
                "Effect": "Allow",
                "Principal": {"Service": "budgets.amazonaws.com"},
                "Action": "sns:Publish",
                "Resource": TaggedValue("Ref", "AlertTopic"),
                "Condition": {
                    "StringEquals": {"aws:SourceAccount": TaggedValue("Ref", "AWS::AccountId")},
                    "ArnLike": {
                        "aws:SourceArn": TaggedValue("Sub", "arn:${AWS::Partition}:budgets::${AWS::AccountId}:*")
                    },
                },
            }
        ]


class TestMonitor:
    @pytest.fixture
    def monitor_resources(self, resources):
        return [(name, logical_id, r) for name, logical_id, r in resources if name == "monitoring/monitor.j2"]

    @pytest.fixture
    def custom_monitor_alarms(self, monitor_resources):
        return [
            (name, logical_id, alarm)
            for name, logical_id, alarm in resources_of_type(monitor_resources, "AWS::CloudWatch::Alarm")
            if alarm["Properties"].get("Namespace") == TaggedValue("Ref", "MetricNamespace")
        ]

    def test_monitor_runs_every_five_minutes(self, rendered_templates):
        rule = rendered_templates["monitoring/monitor.j2"]["Resources"]["ScheduleRule"]

        assert rule["Properties"]["ScheduleExpression"] == "rate(5 minutes)"

    def test_monitor_allows_only_one_concurrent_execution(self, rendered_templates):
        function = rendered_templates["monitoring/monitor.j2"]["Resources"]["MonitorFunction"]

        assert function["Properties"]["ReservedConcurrentExecutions"] == 1

    def test_expected_service_task_count_output_matches_service_desired_counts(self, rendered_templates):
        template = rendered_templates["airflow-services.j2"]
        desired_count = sum(
            resource["Properties"]["DesiredCount"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::ECS::Service"
        )
        assert template["Outputs"]["ExpectedServiceTaskCount"]["Value"] == str(desired_count)

    def test_worker_task_definition_is_passed_to_the_monitor(self, rendered_templates, stack_configs):
        template = rendered_templates["monitoring/monitor.j2"]
        assert template["Parameters"]["FargateWorkerTaskDefinitionArn"] == {"Type": "String"}
        environment = template["Resources"]["MonitorFunction"]["Properties"]["Environment"]["Variables"]
        assert environment["FARGATE_WORKER_TASK_DEFINITION_ARN"] == TaggedValue("Ref", "FargateWorkerTaskDefinitionArn")

        parameters = stack_configs["dev/monitoring/monitor.yaml"]["parameters"]
        assert parameters["FargateWorkerTaskDefinitionArn"] == TaggedValue(
            "stack_output", "dev/airflow.yaml::AirflowWorkerTaskDefArn"
        )

    def test_alarms_fire_when_the_monitor_stops_publishing(self, custom_monitor_alarms):
        not_breaching = [
            logical_id
            for _, logical_id, alarm in custom_monitor_alarms
            if alarm["Properties"].get("TreatMissingData") != "breaching"
        ]
        assert not not_breaching, f"monitor alarms without the dead-man switch: {not_breaching}"

    def test_custom_health_alarms_require_two_five_minute_periods(self, custom_monitor_alarms):
        alarms = [alarm["Properties"] for _, _, alarm in custom_monitor_alarms]

        assert len(alarms) == 5
        for alarm in alarms:
            assert alarm["Period"] == 300
            assert alarm["EvaluationPeriods"] == 2
            assert alarm["DatapointsToAlarm"] == 2

    def test_excessive_monitor_invocations_alarm_immediately(self, rendered_templates):
        alarm = rendered_templates["monitoring/monitor.j2"]["Resources"]["MonitorExcessiveInvocationsAlarm"][
            "Properties"
        ]

        assert alarm["Namespace"] == "AWS/Lambda"
        assert alarm["MetricName"] == "Invocations"
        assert alarm["Dimensions"] == [{"Name": "FunctionName", "Value": TaggedValue("Ref", "MonitorFunction")}]
        assert alarm["Statistic"] == "Sum"
        assert alarm["Period"] == 300
        assert alarm["EvaluationPeriods"] == 1
        assert alarm["DatapointsToAlarm"] == 1
        assert alarm["Threshold"] == 2
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
        assert alarm["TreatMissingData"] == "notBreaching"
        assert alarm["AlarmActions"] == [TaggedValue("Ref", "AlertTopicArn")]
        assert alarm["OKActions"] == [TaggedValue("Ref", "AlertTopicArn")]

    def test_alarms_watch_metrics_the_lambda_publishes(self, monitor_resources, custom_monitor_alarms):
        [(_, _, function)] = resources_of_type(monitor_resources, "AWS::Lambda::Function")
        code = function["Properties"]["Code"]["ZipFile"]
        unpublished = [
            alarm["Properties"]["MetricName"]
            for _, _, alarm in custom_monitor_alarms
            if not re.search("['\"]{}['\"]".format(alarm["Properties"]["MetricName"]), code)
        ]
        assert not unpublished, f"alarms on metrics the Lambda does not publish: {unpublished}"

    def test_metrics_and_alarms_are_scoped_to_environment(
        self, rendered_templates, monitor_resources, custom_monitor_alarms, monkeypatch
    ):
        template = rendered_templates["monitoring/monitor.j2"]
        assert template["Parameters"]["MetricNamespace"]["Default"] == "Comet/Monitoring"

        [(_, _, function)] = resources_of_type(monitor_resources, "AWS::Lambda::Function")
        environment = function["Properties"]["Environment"]["Variables"]
        assert environment["ENVIRONMENT"] == TaggedValue("Ref", "Env")

        class Paginator:
            def __init__(self, page):
                self.page = page

            def paginate(self, **kwargs):
                return [self.page]

        class Ec2Client:
            def get_paginator(self, name):
                assert name == "describe_instances"
                return self

            def paginate(self, **kwargs):
                assert kwargs["Filters"] == [
                    {"Name": "tag:Service", "Values": [STUB_USER_DATA["stack_tags"]["Service"]]},
                    {"Name": "tag:Environment", "Values": ["dev"]},
                ]
                return [{"Reservations": []}]

        class EcsClient:
            def get_paginator(self, name):
                assert name == "list_tasks"
                return Paginator({"taskArns": []})

        class CloudWatchClient:
            request = None

            def put_metric_data(self, **kwargs):
                self.request = kwargs

        cloudwatch = CloudWatchClient()
        clients = {
            "ec2": Ec2Client(),
            "ecs": EcsClient(),
            "cloudwatch": cloudwatch,
        }
        monkeypatch.setattr(boto3, "client", clients.__getitem__)
        monkeypatch.setenv("CLUSTER_NAME", "test-cluster")
        monkeypatch.setenv(
            "FARGATE_WORKER_TASK_DEFINITION_ARN",
            "arn:aws:ecs:us-east-1:123456789012:task-definition/comet-dev-airflow-worker:1",
        )
        monkeypatch.setenv("METRIC_NAMESPACE", "Comet/Monitoring")
        monkeypatch.setenv("SERVICE_TAG_VALUE", STUB_USER_DATA["stack_tags"]["Service"])
        monkeypatch.setenv("ENVIRONMENT", "dev")

        namespace = {}
        exec(function["Properties"]["Code"]["ZipFile"], namespace)
        namespace["handler"]({}, None)

        assert cloudwatch.request["Namespace"] == "Comet/Monitoring"
        assert {metric["MetricName"]: metric["Dimensions"] for metric in cloudwatch.request["MetricData"]} == {
            "OldestComputeAgeHours": [{"Name": "Environment", "Value": "dev"}],
            "RecentEc2Launches": [{"Name": "Environment", "Value": "dev"}],
            "RecentFargateWorkerLaunches": [{"Name": "Environment", "Value": "dev"}],
            "FargateServiceTaskCount": [{"Name": "Environment", "Value": "dev"}],
            "CrashedServiceTasks": [{"Name": "Environment", "Value": "dev"}],
        }
        assert (
            next(
                metric["Value"]
                for metric in cloudwatch.request["MetricData"]
                if metric["MetricName"] == "RecentFargateWorkerLaunches"
            )
            == 0
        )

        expected_dimension = [{"Name": "Environment", "Value": TaggedValue("Ref", "Env")}]
        for _, _, alarm in custom_monitor_alarms:
            assert alarm["Properties"]["Dimensions"] == expected_dimension

    def test_counts_only_service_tasks_that_are_actually_running(self, rendered_templates, monkeypatch):
        function = rendered_templates["monitoring/monitor.j2"]["Resources"]["MonitorFunction"]
        code = function["Properties"]["Code"]["ZipFile"]
        now = datetime.datetime(2026, 7, 31, 12, tzinfo=datetime.timezone.utc)
        task_definition_arn = "arn:aws:ecs:us-east-1:123456789012:task-definition/comet-dev-api-server:3"

        tasks = [
            {
                "containers": [{"name": "api-server"}],
                "createdAt": now - datetime.timedelta(minutes=5),
                "desiredStatus": "RUNNING",
                "group": "service:comet-dev-api-server",
                "lastStatus": last_status,
                "taskArn": f"arn:aws:ecs:us-east-1:123456789012:task/test-cluster/{last_status.lower()}",
                "taskDefinitionArn": task_definition_arn,
            }
            for last_status in ("RUNNING", "PENDING")
        ]
        tasks_by_arn = {task["taskArn"]: task for task in tasks}

        class EcsClient:
            def get_paginator(self, name):
                assert name == "list_tasks"
                return self

            def paginate(self, **kwargs):
                task_arns = [task["taskArn"] for task in tasks] if kwargs["desiredStatus"] == "RUNNING" else []
                return [{"taskArns": task_arns}]

            def describe_tasks(self, cluster, tasks):
                assert cluster == "test-cluster"
                return {"tasks": [tasks_by_arn[arn] for arn in tasks], "failures": []}

        clients = {
            "ec2": SimpleNamespace(
                get_paginator=lambda name: SimpleNamespace(paginate=lambda **kwargs: [{"Reservations": []}])
            ),
            "ecs": EcsClient(),
            "cloudwatch": SimpleNamespace(put_metric_data=lambda **kwargs: None),
        }
        monkeypatch.setattr(boto3, "client", clients.__getitem__)
        monkeypatch.setenv("CLUSTER_NAME", "test-cluster")
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FARGATE_WORKER_TASK_DEFINITION_ARN", task_definition_arn)
        monkeypatch.setenv("METRIC_NAMESPACE", "Comet/Monitoring")
        monkeypatch.setenv("SERVICE_TAG_VALUE", STUB_USER_DATA["stack_tags"]["Service"])

        namespace = {}
        exec(code, namespace)
        result = namespace["handler"]({}, None)

        assert result["FargateServiceTaskCount"] == 1

    def test_counts_ec2_launches_from_the_previous_ten_minutes(self, rendered_templates, monkeypatch):
        function = rendered_templates["monitoring/monitor.j2"]["Resources"]["MonitorFunction"]
        code = function["Properties"]["Code"]["ZipFile"]
        now = datetime.datetime(2026, 7, 31, 12, tzinfo=datetime.timezone.utc)
        instances = [
            {
                "LaunchTime": now - age,
                "State": {"Name": "running"},
            }
            for age in (
                datetime.timedelta(minutes=5),
                datetime.timedelta(minutes=10),
                datetime.timedelta(minutes=10, seconds=1),
            )
        ]

        class Ec2Client:
            def get_paginator(self, name):
                assert name == "describe_instances"
                return self

            def paginate(self, **kwargs):
                return [{"Reservations": [{"Instances": instances}]}]

        class EcsClient:
            def get_paginator(self, name):
                assert name == "list_tasks"
                return SimpleNamespace(paginate=lambda **kwargs: [{"taskArns": []}])

        clients = {
            "ec2": Ec2Client(),
            "ecs": EcsClient(),
            "cloudwatch": SimpleNamespace(put_metric_data=lambda **kwargs: None),
        }
        monkeypatch.setattr(boto3, "client", clients.__getitem__)
        monkeypatch.setenv("CLUSTER_NAME", "test-cluster")
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv(
            "FARGATE_WORKER_TASK_DEFINITION_ARN",
            "arn:aws:ecs:us-east-1:123456789012:task-definition/comet-dev-airflow-worker:1",
        )
        monkeypatch.setenv("METRIC_NAMESPACE", "Comet/Monitoring")
        monkeypatch.setenv("SERVICE_TAG_VALUE", STUB_USER_DATA["stack_tags"]["Service"])

        namespace = {}
        exec(code, namespace)

        class FixedDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        namespace["datetime"] = SimpleNamespace(datetime=FixedDateTime, timezone=datetime.timezone)
        result = namespace["handler"]({}, None)

        assert result["RecentEc2Launches"] == 2

    def test_counts_recent_fargate_workers_across_task_definition_revisions(self, rendered_templates, monkeypatch):
        template = rendered_templates["monitoring/monitor.j2"]
        function = template["Resources"]["MonitorFunction"]
        code = function["Properties"]["Code"]["ZipFile"]

        now = datetime.datetime(2026, 7, 31, 12, tzinfo=datetime.timezone.utc)
        worker_family_arn = "arn:aws:ecs:us-east-1:123456789012:task-definition/comet-dev-airflow-worker"

        def task(name, status, task_definition_arn, created_at, group):
            return {
                "containers": [{"name": "worker", "exitCode": 0}],
                "createdAt": created_at,
                "desiredStatus": status,
                "group": group,
                "launchType": "FARGATE",
                "lastStatus": status,
                "startedAt": created_at,
                "stopCode": "UserInitiated" if status == "STOPPED" else None,
                "taskArn": f"arn:aws:ecs:us-east-1:123456789012:task/test-cluster/{name}",
                "taskDefinitionArn": task_definition_arn,
            }

        tasks = [
            task(
                "current-running",
                "RUNNING",
                f"{worker_family_arn}:9",
                now - datetime.timedelta(minutes=5),
                "family:comet-dev-airflow-worker",
            ),
            task(
                "previous-stopped",
                "STOPPED",
                f"{worker_family_arn}:8",
                now - datetime.timedelta(minutes=10),
                "family:comet-dev-airflow-worker",
            ),
            task(
                "old-worker",
                "STOPPED",
                f"{worker_family_arn}:9",
                now - datetime.timedelta(minutes=10, seconds=1),
                "family:comet-dev-airflow-worker",
            ),
            task(
                "service",
                "RUNNING",
                "arn:aws:ecs:us-east-1:123456789012:task-definition/comet-dev-api-server:3",
                now - datetime.timedelta(minutes=10),
                "service:comet-dev-api-server",
            ),
            task(
                "init",
                "STOPPED",
                "arn:aws:ecs:us-east-1:123456789012:task-definition/comet-dev-airflow-init:4",
                now - datetime.timedelta(minutes=10),
                "family:comet-dev-airflow-init",
            ),
        ]
        tasks_by_arn = {task["taskArn"]: task for task in tasks}

        class Paginator:
            def paginate(self, **kwargs):
                status = kwargs["desiredStatus"]
                return [{"taskArns": [task["taskArn"] for task in tasks if task["desiredStatus"] == status]}]

        class Ec2Client:
            def get_paginator(self, name):
                assert name == "describe_instances"
                return SimpleNamespace(paginate=lambda **kwargs: [{"Reservations": []}])

        class EcsClient:
            def get_paginator(self, name):
                assert name == "list_tasks"
                return Paginator()

            def describe_tasks(self, cluster, tasks):
                assert cluster == "test-cluster"
                return {"tasks": [tasks_by_arn[arn] for arn in tasks], "failures": []}

        class CloudWatchClient:
            request = None

            def put_metric_data(self, **kwargs):
                self.request = kwargs

        cloudwatch = CloudWatchClient()
        clients = {
            "ec2": Ec2Client(),
            "ecs": EcsClient(),
            "cloudwatch": cloudwatch,
        }
        monkeypatch.setattr(boto3, "client", clients.__getitem__)
        monkeypatch.setenv("CLUSTER_NAME", "test-cluster")
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("FARGATE_WORKER_TASK_DEFINITION_ARN", f"{worker_family_arn}:9")
        monkeypatch.setenv("METRIC_NAMESPACE", "Comet/Monitoring")
        monkeypatch.setenv("SERVICE_TAG_VALUE", STUB_USER_DATA["stack_tags"]["Service"])

        namespace = {}
        exec(code, namespace)

        class FixedDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        namespace["datetime"] = SimpleNamespace(datetime=FixedDateTime, timezone=datetime.timezone)
        result = namespace["handler"]({}, None)

        assert result["RecentFargateWorkerLaunches"] == 2

    @pytest.mark.parametrize(
        ("logical_id", "metric_name", "threshold_name"),
        [
            ("Ec2LaunchRateAlarm", "RecentEc2Launches", "Ec2LaunchesPer10MinutesThreshold"),
            (
                "FargateWorkerLaunchRateAlarm",
                "RecentFargateWorkerLaunches",
                "FargateWorkerLaunchesPer10MinutesThreshold",
            ),
        ],
    )
    def test_launch_rate_alarms_use_ten_minute_thresholds(
        self, rendered_templates, logical_id, metric_name, threshold_name
    ):
        template = rendered_templates["monitoring/monitor.j2"]
        alarm = template["Resources"][logical_id]["Properties"]

        assert alarm["MetricName"] == metric_name
        assert alarm["Statistic"] == "Maximum"
        assert alarm["Threshold"] == TaggedValue("Ref", threshold_name)
        assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
        assert alarm["TreatMissingData"] == "breaching"
        assert alarm["AlarmActions"] == [TaggedValue("Ref", "AlertTopicArn")]
        assert alarm["OKActions"] == [TaggedValue("Ref", "AlertTopicArn")]


class TestCosts:
    def test_budget_units_match_their_parameters(self, rendered_templates):
        resources = rendered_templates["monitoring/costs.j2"]["Resources"]

        monthly = resources["MonthlyCostBudget"]["Properties"]["Budget"]
        assert monthly["BudgetType"] == "COST"
        assert monthly["BudgetLimit"] == {
            "Amount": TaggedValue("Ref", "MonthlyBudgetUsd"),
            "Unit": "USD",
        }

        egress = resources["EgressBudget"]["Properties"]["Budget"]
        assert egress["BudgetType"] == "USAGE"
        assert egress["BudgetLimit"] == {
            "Amount": TaggedValue("Ref", "EgressBudgetGb"),
            "Unit": "GB",
        }
        assert "CostTypes" not in egress

    def test_budgets_filter_by_service_and_environment(self, rendered_templates):
        resources = rendered_templates["monitoring/costs.j2"]["Resources"]
        service = STUB_USER_DATA["stack_tags"]["Service"]
        environment = STUB_USER_DATA["stack_tags"]["Environment"]
        tag_filters = [
            {"Tags": {"Key": "user:Service", "Values": [service]}},
            {"Tags": {"Key": "user:Environment", "Values": [environment]}},
        ]

        monthly = resources["MonthlyCostBudget"]["Properties"]["Budget"]
        assert monthly["FilterExpression"] == {"And": tag_filters}

        egress = resources["EgressBudget"]["Properties"]["Budget"]
        assert egress["FilterExpression"] == {
            "And": [
                *tag_filters,
                {
                    "Dimensions": {
                        "Key": "USAGE_TYPE_GROUP",
                        "Values": [
                            "EC2: Data Transfer - Internet (Out)",
                            "S3: Data Transfer - Internet (Out)",
                        ],
                    }
                },
            ]
        }

    def test_budget_metrics_preserve_cost_and_usage_semantics(self, rendered_templates):
        resources = rendered_templates["monitoring/costs.j2"]["Resources"]

        monthly = resources["MonthlyCostBudget"]["Properties"]["Budget"]
        assert monthly["Metrics"] == ["AmortizedCost"]
        assert "CostFilters" not in monthly
        assert "CostTypes" not in monthly

        egress = resources["EgressBudget"]["Properties"]["Budget"]
        assert egress["Metrics"] == ["UsageQuantity"]
        assert "CostFilters" not in egress

    def test_budget_notifications_publish_to_the_shared_alert_topic(self, rendered_templates):
        resources = rendered_templates["monitoring/costs.j2"]["Resources"]
        expected_subscribers = [
            {
                "SubscriptionType": "SNS",
                "Address": TaggedValue("Ref", "AlertTopicArn"),
            }
        ]

        for logical_id in ("MonthlyCostBudget", "EgressBudget"):
            notifications = resources[logical_id]["Properties"]["NotificationsWithSubscribers"]
            for notification in notifications:
                assert notification["Subscribers"] == expected_subscribers


class TestMonitoringConfig:
    @pytest.fixture
    def config_template(self):
        path = PROJECT_DIR / "infra" / "config" / "dev" / "config.yaml"
        return jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(path.read_text())

    @pytest.fixture
    def config_variables(self):
        return yaml.safe_load((PROJECT_DIR / "vars-dev.yaml.example").read_text())

    @pytest.mark.parametrize("alert_emails", [None, []])
    def test_alert_emails_must_be_present_and_nonempty(self, config_template, config_variables, alert_emails):
        variables = config_variables.copy()
        if alert_emails is None:
            variables.pop("alert_emails")
        else:
            variables["alert_emails"] = alert_emails

        with pytest.raises(jinja2.UndefinedError, match="alert_emails_must_contain_at_least_one_address"):
            config_template.render(var=variables)

    def test_nonempty_alert_emails_render(self, config_template, config_variables):
        rendered = config_template.render(var=config_variables)
        config = yaml.load(rendered, Loader=TaggedLoader)
        assert config["sceptre_user_data"]["alert_emails"] == ["alerts@example.org"]

    def test_stack_tags_are_forwarded_without_derived_tags(self, config_template, config_variables):
        root_config_template = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(
            (PROJECT_DIR / "infra" / "config" / "config.yaml").read_text()
        )

        root_config = yaml.safe_load(root_config_template.render(var=config_variables))
        environment_config = yaml.load(
            config_template.render(var=config_variables),
            Loader=TaggedLoader,
        )

        assert root_config["stack_tags"] == config_variables["stack_tags"]
        assert environment_config["sceptre_user_data"]["stack_tags"] == config_variables["stack_tags"]

    def test_environment_tag_must_match_environment_name(self, config_template, config_variables):
        variables = config_variables.copy()
        variables["stack_tags"] = {
            **config_variables["stack_tags"],
            "Environment": "production",
        }

        with pytest.raises(jinja2.UndefinedError, match="environment_tag_must_match_env"):
            config_template.render(var=variables)

    def test_threshold_variables_are_converted_and_passed_as_required_parameters(
        self, rendered_templates, stack_configs
    ):
        expected = {
            "monitoring/alarms.j2": {
                "LogIngestionBytesPer5MinThreshold": 10_485_760,
                "S3TotalBytesThreshold": 1_073_741_824_000,
            },
            "monitoring/monitor.j2": {
                "ComputeAgeHoursThreshold": 16,
                "Ec2LaunchesPer10MinutesThreshold": 5,
                "FargateWorkerLaunchesPer10MinutesThreshold": 15,
            },
            "monitoring/costs.j2": {
                "MonthlyBudgetUsd": 150,
                "EgressBudgetGb": 500,
            },
        }

        for template_path, parameters in expected.items():
            stack_path = f"dev/{template_path.removesuffix('.j2')}.yaml"
            assert {name: stack_configs[stack_path]["parameters"][name] for name in parameters} == parameters
            for name in parameters:
                assert "Default" not in rendered_templates[template_path]["Parameters"][name]

    def test_costs_stack_uses_the_shared_alert_topic(self, rendered_templates, stack_configs):
        template = rendered_templates["monitoring/costs.j2"]
        config = stack_configs["dev/monitoring/costs.yaml"]

        assert template["Parameters"]["AlertTopicArn"] == {"Type": "String"}
        assert "dev/monitoring/alerts.yaml" in config["dependencies"]
        assert config["parameters"]["AlertTopicArn"] == TaggedValue(
            "stack_output", "dev/monitoring/alerts.yaml::AlertTopicArn"
        )
