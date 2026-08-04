import os
import subprocess

from conftest import PROJECT_DIR


def test_invalid_source_tag_fails_before_running_commands(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    invocation_log = tmp_path / "invocations"
    sentinel = tmp_path / "shell-injection"
    for command in ("aws", "docker"):
        stub = bin_dir / command
        stub.write_text(f"#!/bin/sh\nprintf '%s\\n' {command} >> \"$INVOCATION_LOG\"\n")
        stub.chmod(0o755)

    result = subprocess.run(
        [
            "make",
            "retag",
            "ENV=dev",
            "ECR_REGISTRY=123456789012.dkr.ecr.us-east-1.amazonaws.com",
            f"SOURCE_TAG=safe; touch {sentinel}; false #",
            "VERSION_TAG=1.2.3",
        ],
        cwd=PROJECT_DIR,
        env=os.environ
        | {
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "INVOCATION_LOG": str(invocation_log),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert not sentinel.exists()
    assert not invocation_log.exists()
