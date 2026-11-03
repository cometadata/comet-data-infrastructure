"""Summarise the Trivy scans of a patch build; see "Patch builds" in docs/setup.md."""

import json
from pathlib import Path

import boto3

from comet_deploy.images import IMAGE_NAMES, TAG_PATTERN

SEVERITIES = ("CRITICAL", "HIGH")


def severity_counts(report: dict) -> dict[str, int]:
    """Count the CRITICAL and HIGH vulnerabilities in a Trivy JSON report.

    Trivy writes null for targets without findings, so missing or null lists count as empty.
    """
    counts = dict.fromkeys(SEVERITIES, 0)
    for result in report.get("Results") or []:
        for vulnerability in result.get("Vulnerabilities") or []:
            if vulnerability.get("Severity") in counts:
                counts[vulnerability["Severity"]] += 1
    return counts


def format_summary(
    new_tag: str,
    promoted_tag: str,
    rows: dict[str, tuple[dict[str, int] | None, dict[str, int] | None]],
) -> str:
    """Compare each image's counts with the promoted image's, as `old→new`; n/a marks a missing report or tag."""

    def count(counts, severity):
        return "n/a" if counts is None else counts[severity]

    lines = [f"{new_tag} vs promoted {promoted_tag or 'n/a'}"]
    for image, (new, old) in rows.items():
        lines.append(
            f"  {image:<8} CRIT {count(old, 'CRITICAL')}→{count(new, 'CRITICAL')}"
            f"  HIGH {count(old, 'HIGH')}→{count(new, 'HIGH')}"
        )
    return "\n".join(lines)


def read_counts(path: Path) -> dict[str, int] | None:
    """Return the counts from a Trivy report, or None if the scan wrote no report."""
    if not path.is_file():
        return None
    return severity_counts(json.loads(path.read_text()))


def scan_summary(
    env: str, new_tag: str, reports_dir: str, topic_arn: str, promoted_tag: str = "", report_url: str = ""
) -> None:
    """Print the patch build's scan summary and publish it to the alert topic.

    Reads {reports_dir}/{image}-{tag}.json for the new and promoted tags. Run by scripts/patch-scan.sh in the
    patch pipeline, which has no vars file, so this uses the AWS region from the environment.
    """
    for tag in filter(None, (new_tag, promoted_tag)):
        if not TAG_PATTERN.fullmatch(tag):
            raise SystemExit(f"Tag {tag!r} has an invalid format.")

    reports = Path(reports_dir)
    rows = {
        image: (
            read_counts(reports / f"{image}-{new_tag}.json"),
            read_counts(reports / f"{image}-{promoted_tag}.json") if promoted_tag else None,
        )
        for image in IMAGE_NAMES
    }
    summary = format_summary(new_tag, promoted_tag, rows)
    if report_url:
        summary += f"\n\nFull reports: {report_url}"
    print(summary)

    sns = boto3.client("sns")
    sns.publish(TopicArn=topic_arn, Subject=f"COMET {env} patch build {new_tag}", Message=summary)
