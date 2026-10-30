#!/usr/bin/env python3
"""Format a sanitized curl equivalent for Hardcharger support diagnostics."""

from __future__ import annotations

import json
import os
import re
import shlex
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SENSITIVE = re.compile(
    r"(^|[_-])(password|secret|token|api[_-]?key|authorization|credential|cookie|username)([_-]|$)",
    re.I,
)


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    query = [
        (key, "<REDACTED>" if SENSITIVE.search(key) else value)
        for key, value in parse_qsl(parts.query)
    ]
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit(
        (parts.scheme, netloc, parts.path, urlencode(query), parts.fragment)
    )


def redact_value(value: Any, key: str = "") -> Any:
    if SENSITIVE.search(key):
        return "<REDACTED>"
    if isinstance(value, dict):
        return {name: redact_value(item, name) for name, item in value.items()}
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return value


def format_curl(endpoint: str, method: str, payload_path: Path) -> str:
    headers = json.loads(os.environ.get("HC_API_HEADERS_JSON", "{}"))
    if not isinstance(headers, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in headers.items()
    ):
        raise ValueError("HC_API_HEADERS_JSON must be a JSON object of strings")

    payload = redact_value(json.loads(payload_path.read_text()))
    lines = [
        "curl_args=(",
        f"  --request {shlex.quote(method)}",
        f"  --url {shlex.quote(redact_url(endpoint))}",
    ]
    for name, value in headers.items():
        safe_value = (
            value if name.lower() in {"content-type", "accept"} else "<REDACTED>"
        )
        lines.append(f"  --header {shlex.quote(f'{name}: {safe_value}')}")
    if os.environ.get("HC_API_USERNAME"):
        lines.append(f"  --user {shlex.quote('<REDACTED>')}")
    lines.extend(["  --data-binary @-", ")", "", "curl \"${curl_args[@]}\" <<'JSON'"])
    return (
        "# Replace redacted credentials with valid values before running.\n"
        + "\n".join(lines)
        + "\n"
        + json.dumps(payload, indent=2)
        + "\nJSON\n"
    )


def write_support_curl(output_path: Path, command: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(command)
    output_path.chmod(0o600)


if __name__ == "__main__":
    if len(sys.argv) not in {4, 6}:
        raise SystemExit(
            "usage: format_hardcharger_curl.py ENDPOINT METHOD PAYLOAD "
            "[--output FILE]"
        )
    curl = format_curl(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
    if len(sys.argv) == 6:
        if sys.argv[4] != "--output":
            raise SystemExit("expected --output")
        write_support_curl(Path(sys.argv[5]), curl)
    else:
        print(curl, end="")
