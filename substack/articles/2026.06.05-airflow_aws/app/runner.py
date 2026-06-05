import json
import logging
import os
import runpy
import sys
from urllib.parse import urlparse

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


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path or parsed.path == "/":
        raise ValueError(f"Invalid S3 URI for runtime script: {s3_uri}")

    script_bucket = parsed.netloc
    script_key = parsed.path.lstrip("/")
    if not script_key:
        raise ValueError(f"Invalid S3 URI for runtime script: {s3_uri}")

    return script_bucket, script_key


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
        if len(sys.argv) < 2 or not sys.argv[1].strip():
            raise ValueError("Missing required script argument. Usage: runner.py s3://bucket/path/script.py")

        script_s3_uri = sys.argv[1].strip()
        script_bucket, script_key = parse_s3_uri(script_s3_uri)
        local_script_path = env("LOCAL_SCRIPT_PATH", "/tmp/runtime_etl.py")

        download_script(script_bucket, script_key, local_script_path)
        runpy.run_path(local_script_path, run_name="__main__")

        logging.info("runner_complete %s", json.dumps({"script_s3_uri": script_s3_uri, "script_key": script_key}))
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.exception("runner_failed %s", json.dumps({"error": str(exc)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
