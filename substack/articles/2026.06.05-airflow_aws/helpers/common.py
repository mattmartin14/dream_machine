import logging
import os
import json
from datetime import datetime, timezone
from typing import List

import boto3
import duckdb
import requests
from botocore.exceptions import ClientError

_SLACK_WEBHOOK_CACHE: str | None = None

def copy_local_file_to_s3(local_file_path: str, s3_target_path: str) -> None:
    s3 = boto3.client("s3")
    bucket_name, key = s3_target_path.replace("s3://", "").split("/", 1)
    s3.upload_file(local_file_path, bucket_name, key)

def establish_duckdb_connection(aws_region: str, extension_directory: str) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with robust aws/httpfs extension setup."""

    cn = duckdb.connect()

    local_fallback_dir = os.path.join(os.path.expanduser("~"), ".duckdb", "extensions")
    candidate_dirs = [extension_directory]
    if local_fallback_dir not in candidate_dirs:
        candidate_dirs.append(local_fallback_dir)

    attempt_errors: List[str] = []
    initialized = False
    for candidate_dir in candidate_dirs:
        try:
            os.makedirs(candidate_dir, exist_ok=True)
            cn.execute(f"SET extension_directory='{candidate_dir}';")
            cn.execute("load httpfs; load aws;")
            initialized = True
            break
        except (duckdb.Error, OSError) as exc:
            attempt_errors.append(f"{candidate_dir}: {str(exc)}")

    if not initialized:
        cn.close()
        raise RuntimeError(
            "Failed to initialize DuckDB aws/httpfs extensions; attempted directories: "
            + " | ".join(attempt_errors)
        )

    cn.execute(f"SET s3_region='{aws_region}';")
    cn.execute("CREATE OR REPLACE SECRET s3_default (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    return cn


def send_slack_notification(
    level: str,
    message: str,
    title: str | None = None,
    fields: dict[str, str | int | float] | None = None,
) -> bool:
    """Send a Slack webhook notification and return whether delivery succeeded."""
    webhook_url = _get_slack_webhook_url()
    if not webhook_url:
        logging.info("slack_notification_skipped missing_webhook_secret")
        return False

    payload = _build_slack_payload(level=level, message=message, title=title, fields=fields)

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info("slack_notification_sent %s", level.lower())
        return True
    except requests.RequestException as exc:
        logging.warning("slack_notification_failed %s", str(exc))
        return False


def _build_slack_payload(
    level: str,
    message: str,
    title: str | None,
    fields: dict[str, str | int | float] | None,
) -> dict:
    normalized_level = level.strip().lower()
    level_label = normalized_level.upper()

    emoji_map = {
        "info": ":large_blue_circle:",
        "warning": ":warning:",
        "error": ":red_circle:",
        "success": ":white_check_mark:",
    }
    color_map = {
        "info": "#2EB67D",
        "warning": "#ECB22E",
        "error": "#E01E5A",
        "success": "#2EB67D",
    }
    emoji = emoji_map.get(normalized_level, ":speech_balloon:")
    color = color_map.get(normalized_level, "#1D9BD1")

    card_title = title or "ETL Notification"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} {card_title}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Severity*\n{level_label}"},
                {"type": "mrkdwn", "text": f"*Timestamp*\n{now_iso}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Details*\n{message}",
            },
        },
    ]

    if fields:
        field_entries = []
        for key, value in fields.items():
            field_entries.append(
                {
                    "type": "mrkdwn",
                    "text": f"*{key}*\n{value}",
                }
            )

        for start_idx in range(0, len(field_entries), 10):
            blocks.append(
                {
                    "type": "section",
                    "fields": field_entries[start_idx : start_idx + 10],
                }
            )

    return {
        # Keep plain text fallback for clients/surfaces that do not render blocks.
        "text": f"[{level_label}] {card_title}: {message}",
        "attachments": [
            {
                "color": color,
                "fallback": f"[{level_label}] {card_title}: {message}",
                "blocks": blocks,
            }
        ],
    }


def _get_slack_webhook_url() -> str | None:
    global _SLACK_WEBHOOK_CACHE
    if _SLACK_WEBHOOK_CACHE:
        return _SLACK_WEBHOOK_CACHE

    secret_name = os.getenv("SLACK_WEBHOOK_SECRET_NAME", "slack_webhook_v1")
    region_name = os.getenv("AWS_REGION", "us-east-1")
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        logging.warning("slack_webhook_secret_fetch_failed %s", str(exc))
        return None

    secret_string = response.get("SecretString")
    if not secret_string:
        logging.warning("slack_webhook_secret_missing_string %s", secret_name)
        return None

    secret_string = secret_string.strip()
    if secret_string.startswith("{"):
        try:
            secret_payload = json.loads(secret_string)
        except json.JSONDecodeError:
            logging.warning("slack_webhook_secret_json_decode_failed %s", secret_name)
            return None

        webhook_url = secret_payload.get("slack_webhook")
      
    else:
        webhook_url = secret_string

    if not webhook_url:
        logging.warning("slack_webhook_secret_missing_url_key %s", secret_name)
        return None

    _SLACK_WEBHOOK_CACHE = webhook_url
    return _SLACK_WEBHOOK_CACHE
