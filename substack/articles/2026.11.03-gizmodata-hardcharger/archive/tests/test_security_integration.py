import os

import pytest
from adbc_driver_hardcharger import dbapi

REQUIRED = (
    "HARDCHARGER_URI",
    "TEST_FINANCE_USERNAME",
    "TEST_FINANCE_PASSWORD",
    "TEST_REGION_USERNAME",
    "TEST_REGION_PASSWORD",
    "TEST_REGION_KEY",
    "TEST_REGION3_USERNAME",
    "TEST_REGION3_PASSWORD",
    "TEST_REGION3_KEY",
    "TEST_REPORTING_USERNAME",
    "TEST_REPORTING_PASSWORD",
)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        any(not os.getenv(name) for name in REQUIRED),
        reason="requires configured Hardcharger identities and security demo tables",
    ),
]


def connection(username_env: str, password_env: str):
    return dbapi.connect(
        uri=os.environ["HARDCHARGER_URI"],
        username=os.environ[username_env],
        password=os.environ[password_env],
    )


def test_finance_can_see_regions_and_region_user_is_restricted() -> None:
    with connection("TEST_FINANCE_USERNAME", "TEST_FINANCE_PASSWORD") as finance:
        cursor = finance.cursor()
        cursor.execute("SELECT DISTINCT regionkey FROM analytics.main.daily_nation_sales")
        finance_regions = {row[0] for row in cursor.fetchall()}
        cursor.close()
    with connection("TEST_REGION_USERNAME", "TEST_REGION_PASSWORD") as regional:
        cursor = regional.cursor()
        cursor.execute("SELECT DISTINCT regionkey FROM analytics.main.daily_nation_sales")
        regional_regions = {row[0] for row in cursor.fetchall()}
        cursor.close()
    with connection("TEST_REGION3_USERNAME", "TEST_REGION3_PASSWORD") as regional3:
        cursor = regional3.cursor()
        cursor.execute("SELECT DISTINCT regionkey FROM analytics.main.daily_nation_sales")
        regional3_regions = {row[0] for row in cursor.fetchall()}
        cursor.close()
    assert finance_regions
    assert regional_regions == {os.environ["TEST_REGION_KEY"]}
    assert regional3_regions == {os.environ["TEST_REGION3_KEY"]}
    assert regional_regions.isdisjoint(regional3_regions)


def test_reporting_columns_are_masked_and_writes_are_denied() -> None:
    with connection("TEST_REPORTING_USERNAME", "TEST_REPORTING_PASSWORD") as reporting:
        cursor = reporting.cursor()
        cursor.execute(
            "SELECT c_name, c_address, c_phone "
            "FROM analytics.main.customer_security_demo LIMIT 5"
        )
        records = cursor.fetchall()
        assert records
        assert all(value == "REDACTED" for row in records for value in row)
        with pytest.raises(dbapi.ProgrammingError):
            cursor.execute("DELETE FROM analytics.main.daily_sales WHERE FALSE")
        cursor.close()
