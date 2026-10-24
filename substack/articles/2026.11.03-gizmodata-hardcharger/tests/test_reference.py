import json
from pathlib import Path

import pyarrow.parquet as pq

from scripts.generate_reference import generate
from scripts.upload_tpch import upload


def test_reference_dataset_is_small_and_schema_aligned(tmp_path: Path) -> None:
    output = generate(tmp_path / "customer_segment.parquet")
    table = pq.read_table(output)
    assert table.num_rows == 25
    assert table.column_names == ["nationkey", "segment"]
    assert str(table.schema.field("nationkey").type) == "int32"
    assert set(table.column("segment").to_pylist()) == {"synthetic_a", "synthetic_b"}


def test_upload_records_results_and_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "tpch"
    table_dir = data_dir / "orders"
    table_dir.mkdir(parents=True)
    table_file = table_dir / "orders.parquet"
    table_file.write_bytes(b"parquet")
    (data_dir / "manifest.json").write_text(
        json.dumps({"tables": {"orders": {"file": "orders/orders.parquet", "rows": 1}}})
    )

    class FakeS3:
        def __init__(self) -> None:
            self.uploads: list[tuple[str, str, str]] = []

        def upload_file(self, filename: str, bucket: str, key: str) -> None:
            self.uploads.append((filename, bucket, key))

    client = FakeS3()
    result = upload(data_dir, "bucket", "tpch", client=client)
    assert result["files_uploaded"] == 2
    assert client.uploads[-1][2] == "tpch/manifest.json"
    assert (data_dir / "upload-manifest.json").is_file()
