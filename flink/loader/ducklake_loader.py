"""Consumes deduped events from Kafka and appends them to a DuckLake table in batches.

Batches are converted to a columnar pyarrow Table before insertion (rather than
row-by-row `executemany`), and handed to DuckDB via its zero-copy Arrow scan
support, which avoids per-row Python<->SQL binding overhead."""
import json
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events-deduped"
GROUP_ID = "ducklake-loader"

REPO_ROOT = Path(__file__).resolve().parent.parent
CATALOG_PATH = REPO_ROOT / "ducklake" / "catalog.ducklake"
DATA_PATH = REPO_ROOT / "ducklake" / "data"

BATCH_SIZE = 50
BATCH_TIMEOUT_SECONDS = 5.0

EVENTS_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_time", pa.timestamp("us")),
    ("user_id", pa.int32()),
    ("event_type", pa.string()),
    ("payload", pa.string()),
])


def get_ducklake_connection() -> duckdb.DuckDBPyConnection:
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(f"""
        ATTACH 'ducklake:{CATALOG_PATH}' AS lake (DATA_PATH '{DATA_PATH}')
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS lake.events (
            event_id VARCHAR,
            event_time TIMESTAMP,
            user_id INTEGER,
            event_type VARCHAR,
            payload VARCHAR
        )
    """)
    return con


def batch_to_arrow(batch: list[dict]) -> pa.Table:
    columns = {field.name: [e[field.name] for e in batch] for field in EVENTS_SCHEMA}
    # Kafka messages carry event_time as an ISO-8601 string, not a datetime.
    columns["event_time"] = [datetime.fromisoformat(t) for t in columns["event_time"]]
    return pa.Table.from_pydict(columns, schema=EVENTS_SCHEMA)


def insert_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]):
    arrow_table = batch_to_arrow(batch)
    # DuckDB scans the Arrow table zero-copy when referenced by name in SQL.
    con.execute("INSERT INTO lake.events SELECT * FROM arrow_table")


def main():
    con = get_ducklake_connection()
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    batch: list[dict] = []
    last_flush = time.time()
    total_inserted = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is not None and not msg.error() and (value := msg.value()) is not None:
                batch.append(json.loads(value))

            should_flush = batch and (
                len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_TIMEOUT_SECONDS
            )
            if should_flush:
                insert_batch(con, batch)
                total_inserted += len(batch)
                print(f"inserted batch of {len(batch)} rows (total: {total_inserted})")
                consumer.commit()
                batch = []
                last_flush = now
    except KeyboardInterrupt:
        pass
    finally:
        if batch:
            insert_batch(con, batch)
            consumer.commit()
            print(f"inserted final batch of {len(batch)} rows")
        consumer.close()
        con.close()


if __name__ == "__main__":
    main()
