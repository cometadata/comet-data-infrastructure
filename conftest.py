"""Pytest session setup.

comet reads COMET_ENV / AWS_REGION at runtime and raises if they are unset (see
``comet.utils.get_env`` / ``get_region``). Provide defaults so importing and exercising
modules like ``comet.dynamodb_store`` during the test session doesn't trip those guards.
"""

import os

os.environ.setdefault("COMET_ENV", "test")
os.environ.setdefault("AWS_REGION", "us-east-1")
