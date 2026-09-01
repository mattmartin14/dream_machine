"""PyFlink SQL job: reads raw events from Kafka, drops duplicate event_ids
(keep-first by event_time), and republishes the deduped stream via upsert-kafka."""
from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
SOURCE_TOPIC = "events"
SINK_TOPIC = "events-deduped"


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Bound dedup operator state growth instead of retaining every event_id forever.
    t_env.get_config().set("table.exec.state.ttl", "1 h")

    t_env.execute_sql(f"""
        CREATE TABLE events (
            event_id STRING,
            event_time TIMESTAMP(3),
            user_id INT,
            event_type STRING,
            payload STRING,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'dedup-job',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # upsert-kafka (not plain kafka) because ROW_NUMBER()=1 dedup produces a
    # changelog stream with retractions, which requires a keyed upsert sink.
    t_env.execute_sql(f"""
        CREATE TABLE events_deduped (
            event_id STRING,
            event_time TIMESTAMP(3),
            user_id INT,
            event_type STRING,
            payload STRING,
            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'key.format' = 'json',
            'value.format' = 'json',
            'value.json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO events_deduped
        SELECT event_id, event_time, user_id, event_type, payload
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY event_time ASC
                ) AS rn
            FROM events
        )
        WHERE rn = 1
    """)


if __name__ == "__main__":
    main()
