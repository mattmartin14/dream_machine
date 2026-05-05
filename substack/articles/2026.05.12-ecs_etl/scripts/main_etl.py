import json
import logging
import os
import sys
from datetime import UTC, datetime
from helpers.common import establish_duckdb_connection


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def run_etl() -> str:
    bucket = env("S3_BUCKET")
    input_prefix = env("S3_INPUT_PREFIX", "raw/")
    output_prefix = env("S3_OUTPUT_PREFIX", "processed/")
    aws_region = env("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    duckdb_extension_dir = env("DUCKDB_EXTENSION_DIR", "/opt/duckdb/extensions")

    input_uri = f"s3://{bucket}/{input_prefix}*.parquet"
    run_ts = datetime.now(UTC)
    run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
    output_uri = f"s3://{bucket}/{output_prefix}run_id={run_id}/data.parquet"

    logging.info("etl_start %s", json.dumps({"bucket": bucket, "input_uri": input_uri, "output_uri": output_uri}))

    con = establish_duckdb_connection(
        aws_region=aws_region,
        extension_directory=duckdb_extension_dir,
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE source_data AS
        SELECT *
        FROM read_parquet('{input_uri}');
        """
    )

    source_count = con.execute("SELECT COUNT(*) FROM source_data;").fetchone()[0]

    con.execute(
        """
        CREATE OR REPLACE TABLE transformed_data AS
        SELECT
            *,
            CURRENT_TIMESTAMP AS processed_at_utc
        FROM source_data;
        """
    )

    transformed_count = con.execute("SELECT COUNT(*) FROM transformed_data;").fetchone()[0]

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM transformed_data
        ) TO '{output_uri}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    logging.info(
        "etl_complete %s",
        json.dumps(
            {
                "source_row_count": source_count,
                "transformed_row_count": transformed_count,
                "output_uri": output_uri,
            }
        ),
    )

    con.close()
    return output_uri


def main() -> int:
    setup_logging()
    try:
        run_etl()
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.exception("etl_failed %s", json.dumps({"error": str(exc)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
