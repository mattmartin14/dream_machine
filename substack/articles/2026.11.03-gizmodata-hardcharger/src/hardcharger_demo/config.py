"""Environment configuration and secret loading."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class Settings:
    aws_region: str
    hardcharger_uri: str
    hardcharger_username: str
    hardcharger_password: str | None
    hardcharger_secret_arn: str | None
    hardcharger_secret_json_key: str
    raw_bucket: str
    raw_prefix: str
    ducklake_catalog: str
    ducklake_schema: str
    daily_table: str
    nation_table: str
    query_timeout_seconds: int
    log_level: str
    lock_table: str | None

    @classmethod
    def from_env(cls) -> Settings:
        secret_json_key = os.getenv("HARDCHARGER_SECRET_JSON_KEY", "password")
        timeout = int(os.getenv("QUERY_TIMEOUT_SECONDS", "900"))
        if timeout < 1:
            raise ValueError("QUERY_TIMEOUT_SECONDS must be greater than zero")
        return cls(
            aws_region=os.getenv("AWS_REGION", "us-east-2"),
            hardcharger_uri=os.getenv("HARDCHARGER_URI", ""),
            hardcharger_username=os.getenv("HARDCHARGER_USERNAME", ""),
            hardcharger_password=os.getenv("HARDCHARGER_PASSWORD"),
            hardcharger_secret_arn=os.getenv("HARDCHARGER_SECRET_ARN"),
            hardcharger_secret_json_key=secret_json_key,
            raw_bucket=os.getenv("RAW_BUCKET", ""),
            raw_prefix=os.getenv("RAW_PREFIX", "tpch").strip("/"),
            ducklake_catalog=os.getenv("DUCKLAKE_CATALOG", "analytics"),
            ducklake_schema=os.getenv("DUCKLAKE_SCHEMA", "main"),
            daily_table=os.getenv("DUCKLAKE_DAILY_TABLE", "daily_sales"),
            nation_table=os.getenv("DUCKLAKE_NATION_TABLE", "daily_nation_sales"),
            query_timeout_seconds=timeout,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            lock_table=os.getenv("DDB_LOCK_TABLE") or None,
        )

    def require_etl(self) -> None:
        required = {
            "RAW_BUCKET": self.raw_bucket,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        if not self.hardcharger_secret_arn and (
            not self.hardcharger_uri
            or not self.hardcharger_username
            or not self.hardcharger_password
        ):
            raise ValueError(
                "Set HARDCHARGER_SECRET_ARN or provide HARDCHARGER_URI, "
                "HARDCHARGER_USERNAME, and HARDCHARGER_PASSWORD"
            )

    def connection_credentials(self, secrets_client: Any | None = None) -> tuple[str, str, str]:
        uri = self.hardcharger_uri
        username = self.hardcharger_username
        password = self.hardcharger_password
        if self.hardcharger_secret_arn and (
            not uri or not username or not password
        ):
            if secrets_client is None:
                import boto3

                secrets_client = boto3.client("secretsmanager", region_name=self.aws_region)
            response = secrets_client.get_secret_value(SecretId=self.hardcharger_secret_arn)
            raw = response.get("SecretString")
            if not raw:
                raise ValueError("Secrets Manager secret has no SecretString value")
            try:
                secret = json.loads(raw)
            except json.JSONDecodeError as error:
                raise ValueError("Hardcharger secret must be a JSON object") from error
            uri = uri or secret.get("uri", "")
            username = username or secret.get("username", "")
            password = password or secret.get(self.hardcharger_secret_json_key)
        if not isinstance(uri, str) or not uri:
            raise ValueError("Hardcharger connection URI is missing")
        if not isinstance(username, str) or not username:
            raise ValueError("Hardcharger username is missing")
        if not isinstance(password, str) or not password:
            raise ValueError("Hardcharger password is missing")
        return uri, username, password


def select_processing_dates(
    *,
    processing_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    today: date | None = None,
) -> list[date]:
    """Resolve one date, an inclusive range, or yesterday as the scheduled default."""
    if processing_date and (start_date or end_date):
        raise ValueError("Choose PROCESSING_DATE or a date range, not both")
    if bool(start_date) != bool(end_date):
        raise ValueError("Both PROCESSING_START_DATE and PROCESSING_END_DATE are required")
    if processing_date:
        return [date.fromisoformat(processing_date)]
    if start_date and end_date:
        first, last = date.fromisoformat(start_date), date.fromisoformat(end_date)
        if last < first:
            raise ValueError("Processing end date must be on or after start date")
        return [date.fromordinal(day) for day in range(first.toordinal(), last.toordinal() + 1)]
    return [(today or datetime.now(UTC).date()) - timedelta(days=1)]
