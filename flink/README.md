# Kafka → Flink Dedup → DuckLake Demo

A small local pipeline:

```
producer.py --> Kafka "events" --> Flink dedup job --> Kafka "events-deduped" --> ducklake_loader.py --> DuckLake table
```

- **producer.py** emits fake event-log JSON to Kafka, occasionally re-sending the same `event_id` to simulate duplicate delivery.
- **dedup_job.py** (PyFlink SQL) drops duplicate `event_id`s (keep-first by `event_time`) and republishes to a second topic.
- **ducklake_loader.py** batches the deduped events and appends them into a [DuckLake](https://ducklake.select) table stored locally under `ducklake/`.

Kafka and Flink run in Docker. The producer/loader run locally in a Python 3.13 venv managed by `uv` (PyFlink itself doesn't yet support Python 3.13, so the Flink containers install their own Python 3.11 internally — this doesn't affect your local env).

## Setup

```bash
# 1. Start Kafka + Flink
docker compose up -d --build

# 2. Create topics up front (Flink's source fails if a topic doesn't exist yet)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events-deduped --partitions 1 --replication-factor 1 --if-not-exists

# 3. Install local Python deps
uv sync

# 4. Submit the Flink dedup job
docker exec flink-jobmanager ./bin/flink run -py /opt/flink_job/dedup_job.py
```

Check the job is running at http://localhost:8081.

## Run the pipeline

In separate terminals:

```bash
# Terminal A: generate events (runs for 30s by default)
uv run python producer/producer.py

# Terminal B: consume deduped events into DuckLake
uv run python loader/ducklake_loader.py
```

Stop the loader with Ctrl-C once the producer finishes and the batches stop growing.

## Verify deduplication

```bash
uv run python -c "
import duckdb
con = duckdb.connect()
con.execute(\"INSTALL ducklake; LOAD ducklake\")
con.execute("ATTACH 'ducklake:$(pwd)/ducklake/catalog.ducklake' AS lake (DATA_PATH '$(pwd)/ducklake/data')")
print(con.execute('SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_ids FROM lake.events').fetchall())
"
```

`rows` should equal `distinct_ids` — duplicates were removed upstream by Flink before ever reaching DuckLake. DuckLake pins the exact `DATA_PATH` string used on the first `ATTACH`, so always attach with the same (absolute) path the loader used.

## Notes / scope

- Single-node Kafka (KRaft) and single-node Flink — not for production scale.
- No schema registry/Avro, no auth, no end-to-end exactly-once guarantees across all three hops.
- Dedup keeps the *first* occurrence of each `event_id`; Flink state for seen IDs expires after 1 hour (`table.exec.state.ttl`) to bound memory growth.
