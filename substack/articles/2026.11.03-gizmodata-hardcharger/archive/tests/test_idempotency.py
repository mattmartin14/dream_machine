from datetime import date
from typing import Any

import pytest

from hardcharger_demo.config import Settings
from hardcharger_demo.etl import _daily_sql, _run_date


class FakeCursor:
    def __init__(self, *, fail_on_insert: bool = False) -> None:
        self.statements: list[tuple[str, list[Any] | None]] = []
        self.fail_on_insert = fail_on_insert

    def execute(self, operation: str, parameters: list[Any] | None = None) -> None:
        self.statements.append((operation, parameters))
        if self.fail_on_insert and operation.lstrip().startswith("INSERT"):
            raise RuntimeError("insert failed")

    def fetchone(self) -> tuple[object, ...]:
        sql = self.statements[-1][0]
        if "SELECT CAST(order_date AS VARCHAR)" in sql:
            return ("2026-10-20", 2, 5, 100, 10, 90, 45)
        if "COALESCE(SUM(order_count)" in sql:
            return (2, 5, 100, 10, 90)
        return (1,)

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, *, fail_on_insert: bool = False) -> None:
        self._cursor = FakeCursor(fail_on_insert=fail_on_insert)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def settings() -> Settings:
    return Settings(
        aws_region="us-east-2",
        hardcharger_uri="hardcharger://example.test",
        hardcharger_username="etl",
        hardcharger_password="secret",
        hardcharger_secret_arn=None,
        hardcharger_secret_json_key="password",
        raw_bucket="raw-bucket",
        raw_prefix="tpch",
        ducklake_catalog="analytics",
        ducklake_schema="main",
        daily_table="daily_sales",
        nation_table="daily_nation_sales",
        query_timeout_seconds=60,
        log_level="INFO",
        lock_table=None,
    )


def test_daily_sql_uses_expected_tpc_h_metrics_and_bound_date() -> None:
    daily_sql, nation_sql, value = _daily_sql(settings(), date(2026, 10, 20))
    assert "SUM(l.l_extendedprice) AS gross_revenue" in daily_sql
    assert "SUM(l.l_extendedprice * l.l_discount) AS discount_amount" in daily_sql
    assert "SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue" in daily_sql
    assert "CAST(? AS DATE)" in daily_sql
    assert "CAST(n.n_regionkey AS VARCHAR)" in nation_sql
    assert value == "2026-10-20"
    assert "raw-bucket/tpch/orders/*.parquet" in daily_sql


def test_daily_replacement_deletes_before_inserting_and_commits() -> None:
    connection = FakeConnection()
    rows, _duration = _run_date(connection, settings(), date(2026, 10, 20))
    statements = [sql.lstrip() for sql, _params in connection._cursor.statements]
    first_delete = next(i for i, sql in enumerate(statements) if sql.startswith("DELETE"))
    first_insert = next(i for i, sql in enumerate(statements) if sql.startswith("INSERT"))
    assert first_delete < first_insert
    assert sum(sql.startswith("DELETE") for sql in statements) == 2
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert rows == 1


def test_failed_insert_rolls_back_both_daily_targets() -> None:
    connection = FakeConnection(fail_on_insert=True)
    with pytest.raises(RuntimeError, match="insert failed"):
        _run_date(connection, settings(), date(2026, 10, 20))
    assert connection.commits == 0
    assert connection.rollbacks == 1
