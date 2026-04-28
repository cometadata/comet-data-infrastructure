"""Find arXiv tar entries modified after the "before" boundary.

Downloads the latest manifest from s3://{arxiv-bucket}/{manifest-key}, splits
tars into a "before" group (manifest timestamp < start-date) and an "after"
group (>= start-date). For each before tar, records the member with the
latest mtime. The globally latest of those becomes the threshold. Then scans
the after tars and emits CSV rows for members whose mtime is strictly greater
than that threshold. The latest-per-tar before rows are included as a sanity
check.
"""

import csv
import hashlib
import logging
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

import boto3
import cyclopts
from tqdm import tqdm

from comet.arxiv.manifest import ManifestEntry, parse_manifest
from comet.arxiv.pipeline import download_batch, download_manifest

CSV_HEADER = [
    "group",
    "tar_filename",
    "manifest_timestamp",
    "entry_name",
    "entry_mtime",
    "filename",
]

Row = tuple[str, str, str, str, str, str]


def parse_start_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def split_entries(
    entries: list[ManifestEntry], start_date: datetime, preview_before: int
) -> tuple[list[ManifestEntry], list[ManifestEntry]]:
    entries_sorted = sorted(entries, key=lambda e: e.timestamp)
    before = [e for e in entries_sorted if e.timestamp < start_date]
    in_scope = [e for e in entries_sorted if e.timestamp >= start_date]
    preview = before[-preview_before:] if preview_before > 0 else []
    return preview, in_scope


def compute_md5(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def report_download_status(
    entries: list[ManifestEntry], target_dir: Path, label: str
) -> tuple[list[ManifestEntry], list[ManifestEntry]]:
    """Return (missing, mismatched) lists of entries that need (re)download."""
    present = 0
    missing: list[ManifestEntry] = []
    mismatched: list[tuple[ManifestEntry, str]] = []
    for entry in tqdm(entries, desc=f"Checking {label} tars (md5)"):
        local = target_dir / Path(entry.filename).name
        if not local.exists():
            missing.append(entry)
            continue
        actual = compute_md5(local)
        if actual != entry.md5sum:
            mismatched.append((entry, actual))
        else:
            present += 1
    print(
        f"{label}: {present} present (md5 match, will skip), "
        f"{len(missing)} missing, {len(mismatched)} md5-mismatched"
    )
    for entry, actual in mismatched[:10]:
        print(
            f"  md5 mismatch: {Path(entry.filename).name} "
            f"local={actual} manifest={entry.md5sum}"
        )
    if len(mismatched) > 10:
        print(f"  ... and {len(mismatched) - 10} more")
    return missing, [e for e, _ in mismatched]


def confirm(prompt: str) -> bool:
    answer = input(f"{prompt} [y/N]: ").strip().lower()
    return answer in ("y", "yes")


def print_preview(preview: list[ManifestEntry], in_scope: list[ManifestEntry]) -> None:
    print(f"\nBefore — last {len(preview)} entries before start date (sanity):")
    for entry in preview:
        print(f"  {entry.timestamp.isoformat(sep=' ')}  {entry.filename}")
    print(f"\nAfter — {len(in_scope)} entries on/after start date:")
    for entry in in_scope:
        print(f"  {entry.timestamp.isoformat(sep=' ')}  {entry.filename}")
    print()


def make_row(
    group: str,
    tar_name: str,
    manifest_ts: datetime,
    entry_name: str,
    mtime: float,
    filename: str,
) -> Row:
    return (
        group,
        tar_name,
        manifest_ts.isoformat(),
        entry_name,
        datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat(),
        filename,
    )


def find_latest_per_before_tar(
    preview: list[ManifestEntry], before_dir: Path
) -> tuple[list[Row], float | None]:
    rows: list[Row] = []
    global_max: float | None = None
    for entry in tqdm(preview, desc="Scanning before tars"):
        tar_path = before_dir / Path(entry.filename).name
        best: tuple[float, str, str] | None = None
        with tarfile.open(tar_path, "r:") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                if best is None or member.mtime > best[0]:
                    best = (member.mtime, member.name, Path(member.name).name)
        if best is None:
            continue
        mtime, entry_name, filename = best
        rows.append(
            make_row(
                "before", tar_path.name, entry.timestamp, entry_name, mtime, filename
            )
        )
        if global_max is None or mtime > global_max:
            global_max = mtime
    return rows, global_max


def collect_after_rows(
    in_scope: list[ManifestEntry], after_dir: Path, threshold_epoch: float
) -> tuple[list[Row], int]:
    rows: list[Row] = []
    total_entries = 0
    for entry in tqdm(in_scope, desc="Scanning after tars"):
        tar_path = after_dir / Path(entry.filename).name
        with tarfile.open(tar_path, "r:") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                total_entries += 1
                if member.mtime <= threshold_epoch:
                    continue
                rows.append(
                    make_row(
                        "after",
                        tar_path.name,
                        entry.timestamp,
                        member.name,
                        member.mtime,
                        Path(member.name).name,
                    )
                )
    return rows, total_entries


app = cyclopts.App(help=__doc__)


@app.default
def main(
    *,
    start_date: Annotated[
        datetime,
        cyclopts.Parameter(
            name=["-s", "--start-date"],
            converter=lambda type_, tokens: parse_start_date(tokens[0].value),
            help="Inclusive start date in YYYY-MM-DD (UTC).",
        ),
    ],
    download_dir: Annotated[
        Path,
        cyclopts.Parameter(
            name=["-d", "--download-dir"],
            help="Parent dir for before/ and after/ subfolders.",
        ),
    ] = Path("/data/arxiv/mtime_check"),
    output: Annotated[
        Path,
        cyclopts.Parameter(name=["-o", "--output"], help="Output CSV path."),
    ] = Path("modified_entries.csv"),
    preview_before: Annotated[
        int,
        cyclopts.Parameter(
            name=["-p", "--preview-before"],
            help="Before-start tars to download and scan for the latest-mtime threshold.",
        ),
    ] = 5,
    arxiv_bucket: str = "arxiv",
    manifest_key: str = "src/arXiv_src_manifest.xml",
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    start_epoch = start_date.replace(tzinfo=timezone.utc).timestamp()

    before_dir = download_dir / "before"
    after_dir = download_dir / "after"
    download_dir.mkdir(parents=True, exist_ok=True)
    before_dir.mkdir(parents=True, exist_ok=True)
    after_dir.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3")
    manifest_path, _ = download_manifest(
        s3, arxiv_bucket, manifest_key, download_dir
    )
    print(f"Manifest: {manifest_path}")

    entries = parse_manifest(manifest_path)
    print(f"Parsed {len(entries)} manifest entries")

    preview, in_scope = split_entries(entries, start_date, preview_before)
    print_preview(preview, in_scope)

    before_missing, before_mismatched = (
        report_download_status(preview, before_dir, "before") if preview else ([], [])
    )
    after_missing, after_mismatched = (
        report_download_status(in_scope, after_dir, "after") if in_scope else ([], [])
    )
    before_todo = before_missing + before_mismatched
    after_todo = after_missing + after_mismatched
    total_needed = len(before_todo) + len(after_todo)

    if total_needed > 0:
        if not confirm(f"\nDownload {total_needed} tar(s)?"):
            print("Aborted.")
            return
        for entry in before_mismatched:
            (before_dir / Path(entry.filename).name).unlink(missing_ok=True)
        for entry in after_mismatched:
            (after_dir / Path(entry.filename).name).unlink(missing_ok=True)
        if before_todo:
            download_batch(before_todo, before_dir, arxiv_bucket)
        if after_todo:
            download_batch(after_todo, after_dir, arxiv_bucket)
    else:
        print("\nAll tars already present (md5 match); skipping download.")

    before_rows, global_max = find_latest_per_before_tar(preview, before_dir)
    if global_max is None:
        threshold_epoch = start_epoch
        print(
            f"No before tars scanned; using start date as threshold: "
            f"{start_date.isoformat()} (exclusive)"
        )
    else:
        threshold_epoch = global_max
        threshold_iso = datetime.fromtimestamp(
            global_max, tz=timezone.utc
        ).isoformat()
        print(f"Latest mtime across all before tars: {threshold_iso}")
        print(f"Threshold for after tars (exclusive): {threshold_iso}")

    after_rows, after_total = collect_after_rows(in_scope, after_dir, threshold_epoch)
    print(f"Total entries across after tars: {after_total}")
    print(f"Matched (mtime > threshold): {len(after_rows)}")

    with output.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in before_rows:
            writer.writerow(row)
        for row in after_rows:
            writer.writerow(row)

    print(
        f"Wrote {len(before_rows)} before rows + {len(after_rows)} after rows "
        f"to {output}"
    )


if __name__ == "__main__":
    app()
