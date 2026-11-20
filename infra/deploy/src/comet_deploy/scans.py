"""Summarise the Trivy scans of a patch build; see "Patch builds" in docs/setup.md."""

from collections.abc import Sequence
import json
from pathlib import Path

import boto3

from comet_deploy.images import IMAGE_NAMES, TAG_PATTERN

SEVERITIES = ("CRITICAL", "HIGH")
# The most package names a warning lists before it summarises the rest.
MAX_NAMES = 5
# RPM lists each imported signing key as a gpg-pubkey package, one version per key.
MULTI_VERSION_PACKAGES = frozenset({"gpg-pubkey"})


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


def duplicate_packages(report: dict) -> list[str]:
    """Return the OS packages a Trivy report lists at more than one version, sorted.

    An image should record each package once. A base image that keeps a second package list which upgrades
    don't update makes Trivy report the old version too, with the old version's findings.
    """
    versions: dict[str, set] = {}
    for result in report.get("Results") or []:
        if result.get("Class") != "os-pkgs":
            continue
        for package in result.get("Packages") or []:
            if package["Name"] in MULTI_VERSION_PACKAGES:
                continue
            version = (package.get("Epoch"), package.get("Version"), package.get("Release"))
            versions.setdefault(package["Name"], set()).add(version)
    return sorted(name for name, found in versions.items() if len(found) > 1)


def format_warning(image: str, tag: str, packages: list[str]) -> str:
    """Describe an image whose report lists packages at more than one version."""
    names = ", ".join(packages[:MAX_NAMES])
    if len(packages) > MAX_NAMES:
        names += f", and {len(packages) - MAX_NAMES} more"
    noun = "package" if len(packages) == 1 else "packages"
    return (
        f"  WARNING {image} {tag}: {len(packages)} {noun} listed at more than one version ({names}); "
        "the counts include versions that are no longer installed"
    )


def format_summary(
    new_tag: str,
    promoted_tag: str,
    rows: dict[str, tuple[dict[str, int] | None, dict[str, int] | None]],
    warnings: Sequence[str] = (),
) -> str:
    """Compare each image's counts with the promoted image's, as `old→new`; n/a marks a missing report or tag.

    Warnings follow the image rows.
    """

    def count(counts, severity):
        return "n/a" if counts is None else counts[severity]

    lines = [f"{new_tag} vs promoted {promoted_tag or 'n/a'}"]
    for image, (new, old) in rows.items():
        lines.append(
            f"  {image:<8} CRIT {count(old, 'CRITICAL')}→{count(new, 'CRITICAL')}"
            f"  HIGH {count(old, 'HIGH')}→{count(new, 'HIGH')}"
        )
    return "\n".join([*lines, *warnings])


def read_report(path: Path) -> dict | None:
    """Return a Trivy report, or None if the scan wrote no report."""
    if not path.is_file():
        return None
    return json.loads(path.read_text())


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
    rows = {}
    warnings = []
    for image in IMAGE_NAMES:
        counts = []
        for tag in (new_tag, promoted_tag):
            report = read_report(reports / f"{image}-{tag}.json") if tag else None
            counts.append(None if report is None else severity_counts(report))
            duplicates = duplicate_packages(report or {})
            if duplicates:
                warnings.append(format_warning(image, tag, duplicates))
        rows[image] = tuple(counts)
    summary = format_summary(new_tag, promoted_tag, rows, warnings)
    if report_url:
        summary += f"\n\nFull reports: {report_url}"
    print(summary)

    sns = boto3.client("sns")
    sns.publish(TopicArn=topic_arn, Subject=f"COMET {env} patch build {new_tag}", Message=summary)
