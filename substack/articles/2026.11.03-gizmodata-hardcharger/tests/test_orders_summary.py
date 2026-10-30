import pytest

from hardcharger_demo.orders_summary import build_sql


def test_build_sql_reads_source_and_creates_table():
    sql = build_sql("s3://bucket/tpch/orders.parquet", "orders_summary")
    assert "CREATE OR REPLACE TABLE orders_summary AS" in sql
    assert "read_parquet('s3://bucket/tpch/orders.parquet')" in sql


def test_build_sql_rejects_bad_inputs():
    with pytest.raises(ValueError):
        build_sql("/local/file.parquet", "t")
    with pytest.raises(ValueError):
        build_sql("s3://b/o.parquet", "t; DROP TABLE x")
