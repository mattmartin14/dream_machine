import json
from pathlib import Path

import pytest

from scripts.format_hardcharger_curl import format_curl, write_support_curl
from scripts.render_hardcharger_request import REQUIRED, render

TEMPLATE = Path("hardcharger/cluster-request.json.template")


def values() -> dict[str, str]:
    sample = {key: f"sample-{key.lower()}" for key in REQUIRED}
    sample.update(
        {
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
    assert "id" not in request
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


def test_support_curl_redacts_credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = tmp_path / "request.json"
    payload.write_text(
        json.dumps(
            {
                "admin_password": "private-password",
                "nested": {"client_secret": "private-secret"},
                "database_image": {"name": "hardcharger-2.3.0"},
            }
        )
    )
    monkeypatch.setenv(
        "HC_API_HEADERS_JSON",
        json.dumps(
            {
                "Authorization": "Bearer private-api-key",
                "Content-Type": "application/json",
                "X-Api-Key": "private-api-key",
            }
        ),
    )
    monkeypatch.setenv("HC_API_USERNAME", "")

    command = format_curl(
        "https://app.gizmodata.com/api/database-clusters/?wait=false",
        "POST",
        payload,
    )

    assert "private-password" not in command
    assert "private-secret" not in command
    assert "private-api-key" not in command
    assert "admin_password" in command
    assert "hardcharger-2.3.0" in command
    assert "Authorization: <REDACTED>" in command
    assert "Content-Type: application/json" in command
    assert "curl_args=(\n  --request POST\n  --url " in command
    assert 'curl "${curl_args[@]}" <<\'JSON\'' in command
    assert "\n  \"admin_password\": \"<REDACTED>\"" in command
    assert "\\" not in command


def test_support_curl_is_saved_as_plain_text(tmp_path: Path) -> None:
    output_path = tmp_path / "cluster-support-curl.txt"
    command = "curl --request POST --data-binary @- <<'JSON'\n{\n  \"id\": \"example\"\n}\nJSON\n"

    write_support_curl(output_path, command)

    assert output_path.read_text() == command
    assert output_path.stat().st_mode & 0o777 == 0o600


def test_rendered_payload_mounts_persisted_database_and_secret(tmp_path: Path) -> None:
    config = values() | {
        "HC_DATABASE_ID": "db-1",
        "HC_PERSIST_DB_NAME": "order_stats",
        "HC_S3_SECRET_ID": "secret-1",
    }
    output = tmp_path / "request.json"
    render(TEMPLATE, output, config)
    request = json.loads(output.read_text())
    assert request["database_mounts"] == [
        {"database_id": "db-1", "mount_point": "order_stats", "read_only": False}
    ]
    assert request["secrets_to_mount"] == ["secret-1"]
