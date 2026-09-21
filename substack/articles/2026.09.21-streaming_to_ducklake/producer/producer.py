"""Emits fake event-log messages to Kafka, occasionally re-sending the same
event_id to simulate the duplicate-delivery problem Flink will dedup downstream."""
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]
DUPLICATE_PROBABILITY = 0.3  # chance to re-emit the previous event_id

fake = Faker()


def make_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        # Flink's JSON format expects a plain (no-offset) ISO-8601 timestamp for TIMESTAMP(3).
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "user_id": fake.random_int(min=1, max=1000),
        "event_type": random.choice(EVENT_TYPES),
        "payload": fake.sentence(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def main(duration_seconds: int = 30, rate_per_second: float = 5.0):
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    last_event = make_event()
    end_time = time.time() + duration_seconds
    sent = 0

    while time.time() < end_time:
        event = last_event if random.random() < DUPLICATE_PROBABILITY else make_event()
        last_event = event

        producer.produce(
            TOPIC,
            key=event["event_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1
        time.sleep(1.0 / rate_per_second)

    producer.flush()
    print(f"sent {sent} messages to topic '{TOPIC}'")


if __name__ == "__main__":
    main()
