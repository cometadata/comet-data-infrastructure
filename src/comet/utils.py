from __future__ import annotations

import logging
import os
import pathlib
import shlex
import subprocess
from typing import TYPE_CHECKING

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)


def get_env() -> str:
    """Return the deployment environment from AWS_ENV (e.g. "dev").

    Returns:
        The value of the AWS_ENV environment variable.

    Raises:
        RuntimeError: If AWS_ENV is unset or empty.
    """
    env = os.environ.get("AWS_ENV")
    if not env:
        raise RuntimeError("AWS_ENV environment variable is not set")
    return env


def get_region() -> str:
    """Return the AWS region from AWS_REGION.

    Returns:
        The value of the AWS_REGION environment variable.

    Raises:
        RuntimeError: If AWS_REGION is unset or empty.
    """
    region = os.environ.get("AWS_REGION")
    if not region:
        raise RuntimeError("AWS_REGION environment variable is not set")
    return region


def data_path() -> pathlib.Path:
    """Get the base data path.

    Returns:
        A Path object pointing to /data.
    """
    return pathlib.Path("/") / "data"


def local_path(*parts: str) -> pathlib.Path:
    """Construct a local path relative to the data directory.

    Args:
        *parts: Path components to append.

    Returns:
        A Path object.
    """
    return pathlib.Path(data_path(), *parts)


def retry_session(
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    raise_on_status: bool = True,
) -> requests.Session:
    """Create a requests session with retry logic.

    Args:
        total_retries: Total number of retries to allow.
        backoff_factor: A backoff factor to apply between attempts.
        status_forcelist: A set of HTTP status codes that we should force a retry on.
        raise_on_status: Whether to raise an exception on status codes.

    Returns:
        A requests.Session object configured with the specified retry strategy.
    """
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=raise_on_status,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def run_process(
    args: list[str],
    env: Mapping[str, str] | None = None,
):
    """Run a subprocess, streaming combined stdout/stderr to the logger.

    Args:
        args: The command and arguments to run.
        env: Environment variables to set for the process.
    """
    logger.info(f"run_process command: `{shlex.join(args)}`")

    with subprocess.Popen(  # noqa: S603
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        shell=False,
    ) as proc:
        for line in proc.stdout:
            logger.info(line)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args)
