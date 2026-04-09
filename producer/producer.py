"""
producer.py — reads IOT-temp.csv row by row, enriches each record with
simulated humidity/pressure, validates against the sensor schema, and
publishes to Kafka. Invalid rows go to the dead-letter queue.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from threading import Event
from typing import Generator

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from config import Config, make_sensor_id, parse_csv_timestamp, validate_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("producer")

_stop_event = Event()


def _handle_sigterm(signum, frame) -> None:  # noqa: ARG001
    log.info("SIGTERM received — finishing current message then stopping")
    _stop_event.set()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)


def _wait_for_kafka(cfg: Config) -> KafkaProducer:
    log.info("Waiting for Kafka broker at %s …", cfg.kafka_broker)
    for attempt in range(1, cfg.broker_wait_attempts + 1):
        if _stop_event.is_set():
            sys.exit(0)
        try:
            producer = KafkaProducer(
                bootstrap_servers=cfg.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=cfg.acks,
                retries=cfg.retries,
                linger_ms=cfg.linger_ms,
                request_timeout_ms=cfg.request_timeout_ms,
                max_block_ms=cfg.max_block_ms,
            )
            log.info("Connected to Kafka broker after %d attempt(s)", attempt)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d) — retrying in %.1fs",
                attempt, cfg.broker_wait_attempts, cfg.broker_wait_delay_s,
            )
            time.sleep(cfg.broker_wait_delay_s)

    log.error("Kafka broker unreachable after %d attempts — aborting", cfg.broker_wait_attempts)
    sys.exit(1)


def _on_send_success(record_metadata) -> None:
    pass


def _on_send_error(exc: Exception) -> None:
    log.error("Kafka delivery error: %s", exc)


def _iter_csv_forever(filepath: str) -> Generator[dict, None, None]:
    """Yield CSV rows indefinitely, looping back to the start when the file ends."""
    if not os.path.exists(filepath):
        log.error("Dataset file not found: %s", filepath)
        sys.exit(1)

    cycle = 0
    while not _stop_event.is_set():
        cycle += 1
        log.info("Starting CSV pass #%d  (%s)", cycle, filepath)
        with open(filepath, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if _stop_event.is_set():
                    return
                yield row


_rng = random.Random(42)


def _simulate_humidity(temperature: float, location: str) -> float:
    if location.lower() == "in":
        base, slope, spread = 55.0, -0.4, 4.0
    else:
        base, slope, spread = 70.0, -0.3, 8.0
    raw = base + slope * (temperature - 25.0) + _rng.gauss(0, spread)
    return round(min(max(raw, 10.0), 99.0), 1)


def _simulate_pressure(temperature: float) -> float:
    base = 1013.25 - 0.05 * (temperature - 25.0)
    raw  = base + _rng.gauss(0, 2.5)
    return round(min(max(raw, 990.0), 1040.0), 1)


def _maybe_inject_anomaly(msg: dict, anomaly_rate: float) -> dict:
    """Randomly inject an out-of-range temperature to simulate equipment faults."""
    if _rng.random() >= anomaly_rate:
        return msg
    anomaly_msg = dict(msg)
    if _rng.random() < 0.7:
        anomaly_msg["temperature"] = round(_rng.uniform(75.0, 99.0), 1)
        anomaly_msg["humidity"]    = round(_rng.uniform(10.0, 25.0), 1)
    else:
        anomaly_msg["temperature"] = round(_rng.uniform(-35.0, -5.0), 1)
        anomaly_msg["humidity"]    = round(_rng.uniform(80.0, 99.0), 1)
    return anomaly_msg


def _enrich_row(raw: dict, anomaly_rate: float) -> dict | None:
    try:
        room     = raw["room_id/id"].strip()
        location = raw["out/in"].strip()
        temp_str = raw["temp"].strip()
        date_str = raw["noted_date"].strip()

        if not room or not location or not temp_str or not date_str:
            return None

        temperature = float(temp_str)
        msg = {
            "sensor_id":   make_sensor_id(room, location),
            "timestamp":   parse_csv_timestamp(date_str),
            "temperature": round(temperature, 1),
            "humidity":    _simulate_humidity(temperature, location),
            "pressure":    _simulate_pressure(temperature),
            "location":    location,
        }
        return _maybe_inject_anomaly(msg, anomaly_rate)
    except (KeyError, ValueError):
        return None


def _build_dlq_payload(raw_row: dict, error: str) -> dict:
    return {
        "error":     error,
        "raw_row":   raw_row,
        "failed_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def run(cfg: Config) -> None:
    producer = _wait_for_kafka(cfg)
    sent, dlq_sent, errors = 0, 0, 0
    t_start = time.monotonic()
    delay_s = cfg.delay_ms / 1000.0

    log.info(
        "Producer started — topic=%s  dlq=%s  delay=%dms  anomaly_rate=%.0f%%",
        cfg.kafka_topic, cfg.kafka_dlq_topic,
        cfg.delay_ms, cfg.anomaly_rate * 100,
    )

    for raw_row in _iter_csv_forever(cfg.data_file):
        if _stop_event.is_set():
            break

        msg = _enrich_row(raw_row, cfg.anomaly_rate)

        if msg is None:
            producer.send(
                cfg.kafka_dlq_topic,
                value=_build_dlq_payload(raw_row, "row parse error"),
                key=None,
            ).add_errback(_on_send_error)
            dlq_sent += 1
            continue

        valid, reason = validate_message(msg)

        if valid:
            try:
                producer.send(
                    cfg.kafka_topic,
                    value=msg,
                    key=msg["sensor_id"],
                ).add_callback(_on_send_success).add_errback(_on_send_error)
                sent += 1
            except KafkaError as exc:
                log.error("Failed to enqueue message: %s", exc)
                errors += 1
        else:
            producer.send(
                cfg.kafka_dlq_topic,
                value=_build_dlq_payload(raw_row, reason),
                key=msg.get("sensor_id"),
            ).add_errback(_on_send_error)
            dlq_sent += 1

        if sent > 0 and sent % 1000 == 0:
            elapsed = time.monotonic() - t_start or 1e-9
            log.info(
                "Progress: sent=%d  dlq=%d  errors=%d  rate=%.1f msg/s",
                sent, dlq_sent, errors, sent / elapsed,
            )

        if delay_s > 0:
            _stop_event.wait(timeout=delay_s)

    log.info("Flushing remaining messages …")
    try:
        producer.flush(timeout=30)
    except KafkaError as exc:
        log.warning("Flush error during shutdown: %s", exc)
    finally:
        producer.close()

    elapsed = time.monotonic() - t_start
    log.info(
        "Producer stopped — sent=%d  dlq=%d  errors=%d  elapsed=%.1fs",
        sent, dlq_sent, errors, elapsed,
    )


if __name__ == "__main__":
    cfg = Config()
    log.info("Config: broker=%s  topic=%s  file=%s", cfg.kafka_broker, cfg.kafka_topic, cfg.data_file)
    run(cfg)
