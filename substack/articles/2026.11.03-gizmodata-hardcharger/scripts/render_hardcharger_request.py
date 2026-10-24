#!/usr/bin/env python3
"""Render the supplied GizmoData request shape with private local values."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

PLACEHOLDER = re.compile(r"^\$\{([A-Z0-9_]+)\}$")
REQUIRED = (
    "HC_REQUEST_ID",
    "HC_CLUSTER_NAME",
    "HC_CLUSTER_LABEL",
    "HC_PROJECT_ENVIRONMENT_ID",
    "HC_ADMIN_USERNAME",
    "HC_ADMIN_PASSWORD",
    "HC_COMPUTE_INSTANCE_SIZE",
    "HC_DATABASE_IMAGE",
    "HC_CLOUD_REGION_ID",
    "HC_DEFAULT_CATALOG_ID",
    "HC_DNS_LABEL",
    "HARDCHARGER_CLOUD_REGION",
)


def render(template_path: Path, output_path: Path, env: dict[str, str] | None = None) -> None:
    values = os.environ if env is None else env
    missing = [name for name in REQUIRED if not values.get(name)]
    if missing:
        raise ValueError(f"Missing Hardcharger request values: {', '.join(missing)}")
    payload: Any = json.loads(template_path.read_text())

    def substitute(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: substitute(item) for key, item in value.items()}
        if isinstance(value, list):
            return [substitute(item) for item in value]
        if isinstance(value, str):
            match = PLACEHOLDER.fullmatch(value)
            if match:
                return values[match.group(1)]
            if "${" in value:
                raise ValueError(f"Malformed placeholder in request template: {value!r}")
        return value

    rendered = substitute(payload)
    allow_any_ip_value = values.get("HC_ALLOW_ANY_IP", "false").lower()
    if allow_any_ip_value not in {"true", "false"}:
        raise ValueError("HC_ALLOW_ANY_IP must be true or false")
    try:
        ip_access_list = json.loads(values.get("HC_IP_ACCESS_LIST_JSON", "[]"))
    except json.JSONDecodeError as error:
        raise ValueError("HC_IP_ACCESS_LIST_JSON must contain valid JSON") from error
    if not isinstance(ip_access_list, list):
        raise ValueError("HC_IP_ACCESS_LIST_JSON must be a JSON array")
    if allow_any_ip_value == "false" and not ip_access_list:
        raise ValueError(
            "Add the documented client IP allowlist entries before provisioning "
            "with HC_ALLOW_ANY_IP=false"
        )
    rendered["allow_any_ip"] = allow_any_ip_value == "true"
    rendered["ip_access_list"] = ip_access_list
    if rendered["cloud_region"]["id"] != values["HC_CLOUD_REGION_ID"]:
        raise ValueError("The rendered Hardcharger cloud region did not match configuration")
    if values["HARDCHARGER_CLOUD_REGION"] != "us-east-2":
        raise ValueError("The supplied GizmoData request targets us-east-2")
    output_path.write_text(json.dumps(rendered, indent=2) + "\n")
    output_path.chmod(0o600)


if __name__ == "__main__":
    render(Path(sys.argv[1]), Path(sys.argv[2]))
