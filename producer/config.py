"""
config.py — environment-driven configuration and message validation
for the IoT sensor Kafka producer.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class Config:
    # Kafka
    kafka_broker:    str   = field(default_factory=lambda: os.getenv("KAFKA_BROKER",    "kafka:9092"))
    kafka_topic:     str   = field(default_factory=lambda: os.getenv("KAFKA_TOPIC",     "sensor-data"))
    kafka_dlq_topic: str   = field(default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "sensor-data-dlq"))

    # Producer behaviour
    delay_ms:       int   = field(default_factory=lambda: int(float(os.getenv("PRODUCER_DELAY_MS",    "100"))))
    batch_size:     int   = field(default_factory=lambda: int(os.getenv("PRODUCER_BATCH_SIZE",        "10")))
    anomaly_rate:   float = field(default_factory=lambda: float(os.getenv("PRODUCER_ANOMALY_RATE",    "0.05")))
    data_file:      str   = field(default_factory=lambda: os.getenv("PRODUCER_DATA_FILE",
                                                                     "/app/data/sensor_data.csv"))

    # Kafka producer tuning
    acks:               str = "all"
    retries:            int = 5
    linger_ms:          int = 5
    request_timeout_ms: int = 30_000
    max_block_ms:       int = 60_000

    broker_wait_attempts: int   = 20
    broker_wait_delay_s:  float = 3.0


# Sensor value bounds
_SENSOR_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,63}$")
_TEMP_MIN,     _TEMP_MAX     = -40.0, 100.0
_HUMIDITY_MIN, _HUMIDITY_MAX =   0.0, 100.0
_PRESSURE_MIN, _PRESSURE_MAX = 870.0, 1085.0


def validate_message(msg: dict) -> tuple[bool, str]:
    """Return (True, '') if valid, or (False, reason) on first failure."""
    required = ("sensor_id", "timestamp", "temperature", "humidity", "pressure")
    for field_name in required:
        if field_name not in msg:
            return False, f"missing field: {field_name}"
        if msg[field_name] is None:
            return False, f"null field: {field_name}"

    sid = msg["sensor_id"]
    if not isinstance(sid, str) or not _SENSOR_ID_RE.match(sid):
        return False, f"invalid sensor_id format: {sid!r}"

    ts = msg["timestamp"]
    if not isinstance(ts, str):
        return False, f"timestamp must be a string, got {type(ts).__name__}"
    try:
        datetime.fromisoformat(ts)
    except ValueError:
        return False, f"unparseable timestamp: {ts!r}"

    temp = msg["temperature"]
    if not isinstance(temp, (int, float)):
        return False, f"temperature must be numeric, got {type(temp).__name__}"
    if not (_TEMP_MIN <= temp <= _TEMP_MAX):
        return False, f"temperature out of range [{_TEMP_MIN}, {_TEMP_MAX}]: {temp}"

    hum = msg["humidity"]
    if not isinstance(hum, (int, float)):
        return False, f"humidity must be numeric, got {type(hum).__name__}"
    if not (_HUMIDITY_MIN <= hum <= _HUMIDITY_MAX):
        return False, f"humidity out of range [{_HUMIDITY_MIN}, {_HUMIDITY_MAX}]: {hum}"

    prs = msg["pressure"]
    if not isinstance(prs, (int, float)):
        return False, f"pressure must be numeric, got {type(prs).__name__}"
    if not (_PRESSURE_MIN <= prs <= _PRESSURE_MAX):
        return False, f"pressure out of range [{_PRESSURE_MIN}, {_PRESSURE_MAX}]: {prs}"

    return True, ""


# IOT-temp.csv date format: "DD-MM-YYYY HH:MM"
_DATE_FMT = "%d-%m-%Y %H:%M"


def parse_csv_timestamp(raw: str) -> str:
    """Convert "08-12-2018 09:30" to an ISO-8601 UTC string."""
    dt = datetime.strptime(raw.strip(), _DATE_FMT)
    return dt.replace(tzinfo=timezone.utc).isoformat()


def make_sensor_id(room: str, location: str) -> str:
    """Example: "Room Admin", "In"  →  "room_admin_in" """
    room_clean = re.sub(r"[^a-z0-9]+", "_", room.strip().lower()).strip("_")
    return f"{room_clean}_{location.strip().lower()}"
