import json
import logging
import os
import runpy
import sys

import boto3


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


def download_script(script_bucket: str, script_key: str, local_path: str) -> None:
    logging.info(
        "runner_download_start %s",
        json.dumps({"script_bucket": script_bucket, "script_key": script_key, "local_path": local_path}),
    )

    s3 = boto3.client("s3")
    s3.download_file(script_bucket, script_key, local_path)

    logging.info("runner_download_complete %s", json.dumps({"script_key": script_key}))


def main() -> int:
    setup_logging()

    try:
        # Prefer the new variable name but support legacy S3_BUCKET.
        script_bucket = os.getenv("S3_SCRIPT_BUCKET") or os.getenv("S3_BUCKET")
        if not script_bucket:
            raise ValueError("Missing required environment variable: S3_SCRIPT_BUCKET or S3_BUCKET")
        script_key = env("S3_SCRIPT_KEY", "etl/scripts/sales_etl.py")
        local_script_path = env("LOCAL_SCRIPT_PATH", "/tmp/runtime_etl.py")

        download_script(script_bucket, script_key, local_script_path)
        runpy.run_path(local_script_path, run_name="__main__")

        logging.info("runner_complete %s", json.dumps({"script_key": script_key}))
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.exception("runner_failed %s", json.dumps({"error": str(exc)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
