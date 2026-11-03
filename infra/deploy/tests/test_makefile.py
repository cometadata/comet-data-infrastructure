import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[3]

UV_INFRA = ["run", "--project", "infra", "--locked", "--no-active"]
UV_DEPLOY = [*UV_INFRA, "--only-group", "deploy"]

# The variables the Makefile exports to its recipes.
MAKE_VARIABLES = (
    "ENV",
    "IMAGE_TAG",
    "ECR_REGISTRY",
    "SOURCE_TAG",
    "VERSION_TAG",
    "SKIP_EXISTING",
    "STACK",
    "YES",
    "RELEASE",
)

# The external commands the fixture replaces with stubs that log their argv.
STUBBED_COMMANDS = ("uv", "aws")


@pytest.fixture
def make(tmp_path):
    """Run a make target against a copy of the Makefile, with uv and aws replaced by stubs that log their argv.

    Each call returns the result and the uv argv; `run.argv(command)` returns another stub's last argv.
    """
    for name in ("Makefile", "versions.env", ".python-version"):
        shutil.copyfile(PROJECT_DIR / name, tmp_path / name)
    # The Sceptre targets check that the environment exists.
    (tmp_path / "infra" / "config" / "preview").mkdir(parents=True)
    (tmp_path / "vars-preview.yaml").write_text("")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_dir = tmp_path / "invocations"
    log_dir.mkdir()
    for command in STUBBED_COMMANDS:
        stub = bin_dir / command
        stub.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            "log = Path(os.environ['INVOCATION_LOG_DIR']) / (Path(sys.argv[0]).name + '.json')\n"
            "log.write_text(json.dumps(sys.argv[1:]))\n"
        )
        stub.chmod(0o755)

    def argv(command):
        log = log_dir / f"{command}.json"
        return json.loads(log.read_text()) if log.exists() else None

    def run(*args):
        # Drop the flags an outer `make test` exports and any Makefile variables set in the shell.
        excluded = ("MAKEFLAGS", "MFLAGS", "MAKELEVEL", *MAKE_VARIABLES)
        env = {key: value for key, value in os.environ.items() if key not in excluded}
        result = subprocess.run(
            ["make", *args],
            cwd=tmp_path,
            env=env | {"PATH": f"{bin_dir}:{os.environ['PATH']}", "INVOCATION_LOG_DIR": str(log_dir)},
            capture_output=True,
            text=True,
        )
        return result, argv("uv")

    run.argv = argv
    return run


class TestMakefile:
    @pytest.mark.parametrize(
        ("target", "arguments"),
        [
            ("promote", ["preview", "sha-abc1234"]),
            ("retag", ["preview", "sha-abc1234", "1.2.3"]),
            ("sync-vars", ["preview"]),
            ("secrets", ["preview"]),
        ],
    )
    def test_deployment_targets_forward_arguments(self, make, target, arguments):
        result, uv_argv = make(target, "ENV=preview", "SOURCE_TAG=sha-abc1234", "VERSION_TAG=1.2.3")

        assert result.returncode == 0, result.stderr
        assert uv_argv == [*UV_DEPLOY, "comet-deploy", target, *arguments]

    @pytest.mark.parametrize(
        ("target", "variable", "expected"),
        [
            ("promote", "SOURCE_TAG", [*UV_DEPLOY, "comet-deploy", "promote", "preview", "{value}"]),
            ("retag", "SOURCE_TAG", [*UV_DEPLOY, "comet-deploy", "retag", "preview", "{value}", "1.2.3"]),
            (
                "status",
                "STACK",
                [*UV_INFRA, "sceptre", "--dir", "infra", "--var-file=vars-preview.yaml", "status", "preview/{value}"],
            ),
        ],
    )
    def test_hostile_values_are_forwarded_verbatim(self, make, tmp_path, target, variable, expected):
        pwned = tmp_path / "pwned"
        value = f'x"; touch {pwned}; echo "`touch {pwned}`'
        variables = {"ENV": "preview", "SOURCE_TAG": "sha-abc1234", "VERSION_TAG": "1.2.3", variable: value}

        result, uv_argv = make(target, *(f"{name}={setting}" for name, setting in variables.items()))

        assert result.returncode == 0, result.stderr
        assert uv_argv == [argument.format(value=value) for argument in expected]
        assert not pwned.exists()

    def test_promote_requires_source_tag(self, make):
        result, uv_argv = make("promote", "ENV=preview")

        assert result.returncode != 0
        assert "SOURCE_TAG is required" in result.stderr
        assert uv_argv is None


class TestPatchBuild:
    @pytest.mark.parametrize(("release", "value"), [("0.2.3", "v0.2.3"), ("v0.2.3", "v0.2.3"), ("latest", "latest")])
    def test_patch_build_starts_the_patch_pipeline_with_the_release_tag(self, make, release, value):
        result, _ = make("patch-build", "ENV=preview", f"RELEASE={release}")

        assert result.returncode == 0, result.stderr
        assert make.argv("aws") == [
            "codepipeline",
            "start-pipeline-execution",
            "--name",
            "comet-preview-build-pipeline-patch",
            "--variables",
            f"name=RELEASE_TAG,value={value}",
            "--no-cli-pager",
        ]

    @pytest.mark.parametrize("release", ["V0.2.3", "0.2", "v0.2.3-rc1", "0.2.3 ", "vv0.2.3"])
    def test_invalid_release_fails_before_starting_the_pipeline(self, make, release):
        result, _ = make("patch-build", "ENV=preview", f"RELEASE={release}")

        assert result.returncode != 0
        assert "RELEASE must be X.Y.Z, vX.Y.Z or latest" in result.stderr
        assert make.argv("aws") is None
