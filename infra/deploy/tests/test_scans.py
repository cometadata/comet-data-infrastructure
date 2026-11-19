import json

import boto3

from comet_deploy.scans import duplicate_packages, format_summary, format_warning, scan_summary, severity_counts


def report(*severities, null_result=False):
    """Build a minimal Trivy JSON report with one vulnerability per severity."""
    results = [{"Target": "os", "Vulnerabilities": [{"Severity": severity} for severity in severities]}]
    if null_result:
        results.insert(0, {"Target": "python", "Vulnerabilities": None})
    return {"Results": results}


def os_packages(*packages):
    """Build a Trivy JSON report that lists OS packages and has no findings."""
    return {"Results": [{"Class": "os-pkgs", "Packages": [dict(package) for package in packages]}]}


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


def test_duplicate_packages_names_os_packages_listed_at_two_versions():
    trivy_report = os_packages(
        {"Name": "linux-libc-dev", "Version": "6.12.113", "Release": "1+dhi0"},
        {"Name": "linux-libc-dev", "Version": "6.12.115", "Release": "2+dhi0"},
        {"Name": "zlib1g", "Version": "1.3.1", "Release": "1", "Epoch": 1},
        {"Name": "zlib1g", "Version": "1.3.1", "Release": "1", "Epoch": 1},
        {"Name": "apt", "Version": "3.0.4"},
    )
    trivy_report["Results"].append(
        {
            "Class": "lang-pkgs",
            "Packages": [{"Name": "urllib3", "Version": "2.7.0"}, {"Name": "urllib3", "Version": "2.8.0"}],
        }
    )

    assert duplicate_packages(trivy_report) == ["linux-libc-dev"]


def test_duplicate_packages_tolerates_reports_without_package_lists():
    assert duplicate_packages({"Results": [{"Target": "os", "Vulnerabilities": None}, {"Class": "os-pkgs"}]}) == []
    assert duplicate_packages({}) == []


def test_format_summary_appends_warnings():
    summary = format_summary("0.3.2-x", "0.3.2", {"airflow": ({"CRITICAL": 0, "HIGH": 1}, None)}, ["WARNING one"])

    assert summary.splitlines()[-1] == "WARNING one"


def test_format_warning_lists_five_names_then_counts_the_rest():
    warning = format_warning("airflow", "0.3.0-x", ["a", "b", "c", "d", "e", "f", "g"])

    assert warning == (
        "  WARNING airflow 0.3.0-x: 7 packages listed at more than one version (a, b, c, d, e, and 2 more); "
        "the counts include versions that are no longer installed"
    )


def test_scan_summary_warns_about_packages_listed_at_more_than_one_version(tmp_path):
    sns = boto3.client("sns")
    sqs = boto3.client("sqs")
    topic_arn = sns.create_topic(Name="alerts")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="alerts")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    stale = os_packages({"Name": "tzdata", "Version": "2026e"}, {"Name": "tzdata", "Version": "2026f"})
    (tmp_path / "airflow-0.3.0-x.json").write_text(json.dumps(stale))
    (tmp_path / "airflow-0.3.0.json").write_text(json.dumps(report("HIGH")))
    (tmp_path / "batch-0.3.0-x.json").write_text(json.dumps(os_packages({"Name": "tzdata", "Version": "2026f"})))

    scan_summary("dev", "0.3.0-x", str(tmp_path), topic_arn, promoted_tag="0.3.0", report_url="https://example.org/r")

    message = json.loads(sqs.receive_message(QueueUrl=queue_url)["Messages"][0]["Body"])["Message"]
    warnings = [line for line in message.splitlines() if "WARNING" in line]
    assert warnings == [
        "  WARNING airflow 0.3.0-x: 1 package listed at more than one version (tzdata); "
        "the counts include versions that are no longer installed"
    ]
    assert "  airflow  CRIT 0→0  HIGH 1→0" in message
    assert message.endswith("Full reports: https://example.org/r")


def test_duplicate_packages_ignores_rpm_signing_keys():
    # RPM lists each imported signing key as a gpg-pubkey "package", one version per key.
    trivy_report = os_packages(
        {"Name": "gpg-pubkey", "Version": "d832c631", "Release": "6515b3c2"},
        {"Name": "gpg-pubkey", "Version": "c87f5b1a", "Release": "593863f8"},
    )

    assert duplicate_packages(trivy_report) == []
