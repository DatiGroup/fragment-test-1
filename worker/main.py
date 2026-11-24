#!/usr/bin/env python3
"""Simple worker that increments the API counter every API_INTERVAL seconds.

Behavior:
- GET /counter to read current value
- POST /counter with {"value": current+1}

Configuration via environment variables (see `.env.example`):
- API_URL (default: http://localhost:8000)
- API_INTERVAL (seconds, default: 30)
- API_RETRIES (number of retries on failure, default: 3)
- API_SLEEP_ON_ERROR (seconds to wait after a failure before retrying, default: 5)
- KAFKA_BOOTSTRAP_SERVERS (comma-separated, optional)
- KAFKA_TOPIC (default: f1.events)
- FRAGMENT_ID (default: test-fragment-1)
"""

import os
import time
import signal
import logging
import json
import uuid
from typing import Optional
from datetime import datetime, timezone

from dotenv import load_dotenv
import requests

# optional kafka client
try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None

load_dotenv()

# API configuration
API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
COUNTER_GET = os.getenv("API_COUNTER_GET_ENDPOINT", f"{API_URL}/counter")
COUNTER_POST = os.getenv("API_COUNTER_POST_ENDPOINT", f"{API_URL}/counter")
API_INTERVAL = int(os.getenv("API_INTERVAL", "30"))
API_RETRIES = int(os.getenv("API_RETRIES", "3"))
API_SLEEP_ON_ERROR = int(os.getenv("API_SLEEP_ON_ERROR", "5"))

# Kafka configuration (optional)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")  # comma-separated
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1.events")
KAFKA_TOPIC_MONITORING = os.getenv("KAFKA_TOPIC_MONITORING", "monitoring.ingest-events")
FRAGMENT_ID = os.getenv("FRAGMENT_ID", "test-fragment-1")
FRAGMENT_NAME = os.getenv("FRAGMENT_NAME", "")

# This fragment's own API admits its worker by the instance's internal token,
# which the platform generates per fragment instance (Secret, see k8s manifest).
FRAGMENT_INTERNAL_TOKEN = os.getenv("FRAGMENT_INTERNAL_TOKEN", "")

session = requests.Session()
if FRAGMENT_INTERNAL_TOKEN:
    session.headers["X-Fragment-Token"] = FRAGMENT_INTERNAL_TOKEN
stop_requested = False
producer = None


def _signal_handler(sig, frame):
    global stop_requested
    logging.info("Shutdown signal received")
    stop_requested = True


def get_counter_value() -> Optional[int]:
    resp = session.get(COUNTER_GET, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return int(data.get("value", 0))


def post_counter_value(value: int):
    resp = session.post(COUNTER_POST, json={"value": value}, timeout=5)
    resp.raise_for_status()
    return resp.json()


def publish_counter_event(old: int, new: int) -> bool:
    """Publish a counter event to Kafka. Returns True on success, False otherwise."""
    global producer
    if not KAFKA_BOOTSTRAP_SERVERS:
        logging.debug("KAFKA_BOOTSTRAP_SERVERS not set; skipping Kafka publish")
        return False
    if KafkaProducer is None:
        logging.warning("kafka-python is not installed; skipping Kafka publish")
        return False

    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=10,
                acks="all",
            )
        except Exception as exc:
            logging.warning("Failed to create Kafka producer: %s", exc)
            return False

    event = {
        "event_id": str(uuid.uuid4()),
        "fragment_id": FRAGMENT_ID,
        "counter": new,
        "delta": new - old,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {"source": "worker"},
    }
    try:
        fut = producer.send(KAFKA_TOPIC, event)
        fut.get(timeout=10)
        logging.info("Published Kafka event to %s: %s", KAFKA_TOPIC, event)
        return True
    except Exception as exc:
        logging.warning("Failed to publish Kafka event: %s", exc)
        return False


def publish_monitoring_event(status: str, event_type: str = "heartbeat",
                             message: str = None, records_processed: int = None) -> bool:
    """Publish a monitoring event to the monitoring.ingest-events Kafka topic."""
    global producer
    if not KAFKA_BOOTSTRAP_SERVERS:
        logging.debug("KAFKA_BOOTSTRAP_SERVERS not set; skipping monitoring publish")
        return False
    if producer is None:
        logging.debug("Kafka producer not initialized; skipping monitoring publish")
        return False

    event = {
        "source": FRAGMENT_ID,
        "source_name": FRAGMENT_NAME or None,
        "event_type": event_type,
        "status": status,
        "message": message,
        "records_processed": records_processed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        fut = producer.send(KAFKA_TOPIC_MONITORING, event)
        fut.get(timeout=10)
        logging.info("Published monitoring event to %s: source=%s status=%s", KAFKA_TOPIC_MONITORING, FRAGMENT_ID, status)
        return True
    except Exception as exc:
        logging.warning("Failed to publish monitoring event: %s", exc)
        return False


_last_status = "ok"  # Track last reported status for state-transition detection


def attempt_increment():
    global _last_status
    for attempt in range(1, API_RETRIES + 1):
        try:
            current = get_counter_value()
            new = current + 1
            # publish to kafka first; if it fails we still try to update the API
            publish_counter_event(current, new)
            post_counter_value(new)
            logging.info("Incremented counter: %d -> %d", current, new)

            # Publish monitoring heartbeat on success
            event_type = "status_change" if _last_status != "ok" else "heartbeat"
            publish_monitoring_event("ok", event_type=event_type, records_processed=1)
            _last_status = "ok"
            return True
        except Exception as exc:
            logging.warning("Attempt %d failed: %s", attempt, exc)
            if attempt < API_RETRIES:
                time.sleep(API_SLEEP_ON_ERROR)

    logging.error("All %d attempts failed; will wait before retrying", API_RETRIES)

    # Publish monitoring error event on state transition
    if _last_status != "error":
        publish_monitoring_event("error", event_type="status_change", message="All retry attempts failed")
    _last_status = "error"
    return False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Worker starting (API=%s; POST=%s; interval=%ss)", API_URL, COUNTER_POST, API_INTERVAL)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    while not stop_requested:
        success = attempt_increment()
        # if failed, give a short backoff
        if not success:
            time.sleep(API_SLEEP_ON_ERROR)
        # sleep in 1-second increments so we can stop quickly
        for _ in range(API_INTERVAL):
            if stop_requested:
                break
            time.sleep(1)

    # graceful shutdown: flush/close Kafka producer if present
    try:
        if producer is not None:
            producer.flush()
            producer.close()
            logging.info("Kafka producer flushed and closed")
    except Exception as exc:
        logging.warning("Error closing Kafka producer: %s", exc)

    logging.info("Worker shutting down")


if __name__ == "__main__":
    main()
