"""DynamoDB-backed exclusive lock for scheduled ETL invocations."""

from __future__ import annotations

from typing import Any


class EtlLock:
    def __init__(
        self,
        table_name: str,
        owner_id: str,
        *,
        client: Any | None = None,
        region: str = "us-east-2",
    ) -> None:
        if client is None:
            import boto3

            client = boto3.client("dynamodb", region_name=region)
        self._client = client
        self._table_name = table_name
        self._owner_id = owner_id
        self._acquired = False

    def __enter__(self) -> EtlLock:
        try:
            self._client.put_item(
                TableName=self._table_name,
                Item={
                    "lock_key": {"S": "daily-etl"},
                    "owner_id": {"S": self._owner_id},
                },
                ConditionExpression="attribute_not_exists(lock_key)",
            )
        except self._client.exceptions.ConditionalCheckFailedException as error:
            raise RuntimeError("Another ETL invocation holds the distributed lock") from error
        self._acquired = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        if self._acquired:
            self._client.delete_item(
                TableName=self._table_name,
                Key={"lock_key": {"S": "daily-etl"}},
                ConditionExpression="owner_id = :owner",
                ExpressionAttributeValues={":owner": {"S": self._owner_id}},
            )
            self._acquired = False
        return False
