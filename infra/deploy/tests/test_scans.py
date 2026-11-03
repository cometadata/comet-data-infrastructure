import json

import boto3

from comet_deploy.scans import format_summary, scan_summary, severity_counts


def report(*severities, null_result=False):
    """Build a minimal Trivy JSON report with one vulnerability per severity."""
    results = [{"Target": "os", "Vulnerabilities": [{"Severity": severity} for severity in severities]}]
    if null_result:
        results.insert(0, {"Target": "python", "Vulnerabilities": None})
    return {"Results": results}


def test_severity_counts_ignores_null_results():
    trivy_report = report("CRITICAL", "HIGH", "CRITICAL", "MEDIUM", null_result=True)

    assert severity_counts(trivy_report) == {"CRITICAL": 2, "HIGH": 1}


def test_format_summary_marks_missing_reports_na():
    rows = {
        "batch": ({"CRITICAL": 0, "HIGH": 1}, None),
        "airflow": (None, {"CRITICAL": 17, "HIGH": 40}),
    }

    summary = format_summary("0.3.0-20261102T0300Z", "0.3.0", rows)

    assert summary.splitlines() == [
        "0.3.0-20261102T0300Z vs promoted 0.3.0",
        "  batch    CRIT n/a→0  HIGH n/a→1",
        "  airflow  CRIT 17→n/a  HIGH 40→n/a",
    ]


def test_scan_summary_publishes_counts_and_report_link(tmp_path):
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    topic_arn = sns.create_topic(Name="alerts")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="alerts")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    (tmp_path / "airflow-0.3.0-x.json").write_text(json.dumps(report("HIGH")))
    (tmp_path / "airflow-0.3.0.json").write_text(json.dumps(report(*["CRITICAL"] * 17)))

    scan_summary("dev", "0.3.0-x", str(tmp_path), topic_arn, promoted_tag="0.3.0", report_url="https://example.org/r")

    body = json.loads(sqs.receive_message(QueueUrl=queue_url)["Messages"][0]["Body"])
    assert body["Subject"] == "COMET dev patch build 0.3.0-x"
    assert "  airflow  CRIT 17→0  HIGH 0→1" in body["Message"]
    assert body["Message"].endswith("Full reports: https://example.org/r")
