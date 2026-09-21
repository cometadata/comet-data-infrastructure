"""Enrichment diff runner."""

from comet.aws import s5cmd_download_files, s5cmd_upload_files, staged_scratch_dir
from comet.utils import run_process


def diff_enrichments(*, old_uri: str, new_uri: str, output_uri: str) -> None:
    """Diff two full releases and upload the diff release.

    Runs ``comet-enrich diff`` on local copies of both releases. Clears the local
    staging directory and ``output_uri`` before starting, then uploads all diff output.

    Args:
        old_uri: S3 URI of the previous release's full directory (trailing slash).
        new_uri: S3 URI of the current release's full directory (trailing slash).
        output_uri: S3 URI to upload the diff release to (trailing slash).
    """
    with staged_scratch_dir(output_uri) as stage_dir:
        old_dir = stage_dir / "old"
        new_dir = stage_dir / "new"
        out_dir = stage_dir / "diff"
        for uri, target_dir in ((old_uri, old_dir), (new_uri, new_dir)):
            target_dir.mkdir(parents=True, exist_ok=True)
            s5cmd_download_files(f"{uri}*", target_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_process(
            [
                "comet-enrich",
                "diff",
                "--old",
                str(old_dir),
                "--new",
                str(new_dir),
                "--output",
                str(out_dir),
            ]
        )
        s5cmd_upload_files(out_dir, output_uri)
