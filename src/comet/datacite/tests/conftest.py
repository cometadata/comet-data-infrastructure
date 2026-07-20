"""Shared helpers for DataCite tests."""

from __future__ import annotations

import json
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"

CREDENTIAL_HEADERS = [
    "Authorization",
    "User-Agent",
    "X-Amz-Content-SHA256",
    "X-Amz-Date",
    "X-Amz-Security-Token",
    "amz-sdk-invocation-id",
    "amz-sdk-request",
    "x-amz-request-payer",
    "x-amz-checksum-mode",
]

HEADER_KEYS = [
    "ETag",
    "X-Credential-Username",
    "X-Request-Id",
    "x-amz-checksum-crc64nvme",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-amz-version-id",
]


def make_dummy(key: str) -> str:
    return "DUMMY_" + key.upper().replace("-", "_")


def vcrpy_clean_response(
    response: dict,
    body_keys: list[str] | None = None,
    header_keys: list[str] | None = None,
) -> dict:
    """Vcrpy before_record_response callback that replaces sensitive values with dummy placeholders.

    Bind body_keys/header_keys with functools.partial before passing to vcr.use_cassette.
    """
    if body_keys:
        try:
            body = response["body"]["string"]
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            data = json.loads(body)
            for key in body_keys:
                if key in data:
                    data[key] = make_dummy(key)
            body_bytes = json.dumps(data).encode("utf-8")
            response["body"]["string"] = body_bytes
            if "Content-Length" in response.get("headers", {}):
                response["headers"]["Content-Length"] = [str(len(body_bytes))]
        except Exception:
            pass

    if header_keys:
        headers = response.get("headers", {})
        for key in header_keys:
            if key in headers:
                headers[key] = [make_dummy(key)]

    return response
