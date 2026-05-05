from typing import List

import boto3
import duckdb


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
    """Create an in-memory DuckDB connection with pre-bundled aws/httpfs extensions loaded."""
    con = duckdb.connect(":memory:")
    con.execute(f"SET extension_directory='{extension_directory}';")
    con.execute("LOAD httpfs;")
    con.execute("LOAD aws;")
    con.execute(f"SET s3_region='{aws_region}';")
    con.execute("CREATE OR REPLACE SECRET s3_default (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    return con
