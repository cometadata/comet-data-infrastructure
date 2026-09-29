"""Deployment CLI used by the Makefile."""

from botocore.exceptions import BotoCoreError, ClientError
import cyclopts

from comet_deploy.images import promote, retag
from comet_deploy.secrets import create_secrets
from comet_deploy.settings import sync_vars

app = cyclopts.App(name="comet-deploy", help="COMET deployment commands.")
app.command(create_secrets, name="secrets")
app.command(sync_vars)
app.command(promote)
app.command(retag)


def main() -> None:
    """Run deployment commands and report AWS failures without a traceback."""
    try:
        app()
    except (BotoCoreError, ClientError) as exc:
        raise SystemExit(str(exc)) from None
