import json
from pathlib import Path

import pytest

from scripts.render_hardcharger_request import REQUIRED, render

TEMPLATE = Path("hardcharger/cluster-request.json.template")


def values() -> dict[str, str]:
    sample = {key: f"sample-{key.lower()}" for key in REQUIRED}
    sample.update(
        {
            "HC_REQUEST_ID": "00000000-0000-4000-8000-000000000001",
            "HC_CLOUD_REGION_ID": "9e30c6da-3174-5a8b-ab3f-a8ad72206e80",
            "HARDCHARGER_CLOUD_REGION": "us-east-2",
            "HC_ADMIN_PASSWORD": "not-a-real-password",
            "HC_ALLOW_ANY_IP": "false",
            "HC_IP_ACCESS_LIST_JSON": '["203.0.113.7/32"]',
        }
    )
    return sample


def test_rendered_payload_uses_supplied_schema_and_secure_ip_default(tmp_path: Path) -> None:
    output = tmp_path / "request.json"
    render(TEMPLATE, output, values())
    request = json.loads(output.read_text())
    assert request["id"] == "00000000-0000-4000-8000-000000000001"
    assert request["cloud_region"]["id"] == values()["HC_CLOUD_REGION_ID"]
    assert request["admin_password"] == "not-a-real-password"
    assert request["allow_any_ip"] is False
    assert request["database_engine"]["name"] == "duckdb"
    assert request["database_image"]["name"] == values()["HC_DATABASE_IMAGE"]
    assert output.stat().st_mode & 0o777 == 0o600


def test_renderer_rejects_missing_values(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Missing Hardcharger request values"):
        render(TEMPLATE, tmp_path / "request.json", {})


def test_renderer_rejects_other_hardcharger_region(tmp_path: Path) -> None:
    config = values()
    config["HARDCHARGER_CLOUD_REGION"] = "us-east-1"
    with pytest.raises(ValueError, match="targets us-east-2"):
        render(TEMPLATE, tmp_path / "request.json", config)


def test_renderer_requires_ip_allowlist_when_wildcard_access_disabled(tmp_path: Path) -> None:
    config = values()
    config["HC_IP_ACCESS_LIST_JSON"] = "[]"
    with pytest.raises(ValueError, match="allowlist entries"):
        render(TEMPLATE, tmp_path / "request.json", config)
