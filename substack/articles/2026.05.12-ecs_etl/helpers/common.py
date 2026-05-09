import logging
import os
import json
from typing import List

import boto3
import duckdb
import requests
from botocore.exceptions import ClientError


_SLACK_WEBHOOK_CACHE: str | None = None


def _load_or_install_extensions(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("LOAD httpfs;")
        con.execute("LOAD aws;")
        return
    except duckdb.Error:
        pass

    con.execute("INSTALL httpfs;")
    con.execute("INSTALL aws;")
    con.execute("LOAD httpfs;")
    con.execute("LOAD aws;")


def delete_objects_with_prefix(bucket: str, prefix: str) -> int:
    """Delete all objects under an S3 prefix and return deleted object count."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    keys: List[dict] = []
    deleted = 0

    for page in pages:
        for obj in page.get("Contents", []):
            keys.append({"Key": obj["Key"]})
            if len(keys) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(keys)
                keys = []

    if keys:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        deleted += len(keys)

    return deleted


def establish_duckdb_connection(aws_region: str, extension_directory: str) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with robust aws/httpfs extension setup."""
    con = duckdb.connect(":memory:")

    local_fallback_dir = os.path.join(os.path.expanduser("~"), ".duckdb", "extensions")
    candidate_dirs = [extension_directory]
    if local_fallback_dir not in candidate_dirs:
        candidate_dirs.append(local_fallback_dir)

    attempt_errors: List[str] = []
    initialized = False
    for candidate_dir in candidate_dirs:
        try:
            os.makedirs(candidate_dir, exist_ok=True)
            con.execute(f"SET extension_directory='{candidate_dir}';")
            _load_or_install_extensions(con)
            initialized = True
            break
        except (duckdb.Error, OSError) as exc:
            attempt_errors.append(f"{candidate_dir}: {str(exc)}")

    if not initialized:
        con.close()
        raise RuntimeError(
            "Failed to initialize DuckDB aws/httpfs extensions; attempted directories: "
            + " | ".join(attempt_errors)
        )

    con.execute(f"SET s3_region='{aws_region}';")
    con.execute("CREATE OR REPLACE SECRET s3_default (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    return con


def send_slack_notification(level: str, message: str) -> bool:
    """Send a Slack webhook notification and return whether delivery succeeded."""
    webhook_url = _get_slack_webhook_url()
    if not webhook_url:
        logging.info("slack_notification_skipped missing_webhook_secret")
        return False

    payload = {
        "text": f"[{level.upper()}] {message}",
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info("slack_notification_sent %s", level.lower())
        return True
    except requests.RequestException as exc:
        logging.warning("slack_notification_failed %s", str(exc))
        return False


def _get_slack_webhook_url() -> str | None:
    global _SLACK_WEBHOOK_CACHE
    if _SLACK_WEBHOOK_CACHE:
        return _SLACK_WEBHOOK_CACHE

    secret_name = os.getenv("SLACK_WEBHOOK_SECRET_NAME", "slack_webhook_test")
    region_name = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

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

        webhook_url = (
            secret_payload.get("SLACK_WEBHOOK")
            or secret_payload.get("slack_webhook")
            or secret_payload.get("webhook_url")
            or secret_payload.get("url")
        )
    else:
        webhook_url = secret_string

    if not webhook_url:
        logging.warning("slack_webhook_secret_missing_url_key %s", secret_name)
        return None

    _SLACK_WEBHOOK_CACHE = webhook_url
    return _SLACK_WEBHOOK_CACHE
