from app.runner import parse_s3_uri


def assert_raises_value_error(value: str) -> None:
    try:
        parse_s3_uri(value)
    except ValueError:
        return
    raise AssertionError(f"Expected ValueError for URI: {value}")


def main() -> None:
    bucket, key = parse_s3_uri("s3://my-bucket/etl/scripts/job.py")
    assert bucket == "my-bucket"
    assert key == "etl/scripts/job.py"

    assert_raises_value_error("")
    assert_raises_value_error("s3://")
    assert_raises_value_error("s3://bucket")
    assert_raises_value_error("https://bucket/key.py")


if __name__ == "__main__":
    main()