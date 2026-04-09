"""
models.py — Pydantic response models and DB configuration for the IoT API.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class DBConfig:
    host:     str = os.environ.get("POSTGRES_HOST",     "timescaledb")
    port:     int = int(os.environ.get("POSTGRES_PORT", "5432"))
    user:     str = os.environ.get("POSTGRES_USER",     "pipeline_user")
    password: str = os.environ.get("POSTGRES_PASSWORD", "")
    dbname:   str = os.environ.get("POSTGRES_DB",       "iot_sensors")

    pool_min: int = 2
    pool_max: int = 5

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.dbname} user={self.user} "
            f"password={self.password} "
            "connect_timeout=5 "
            "application_name=iot-api"
        )


from pydantic import BaseModel, Field  # noqa: E402


class HealthResponse(BaseModel):
    status:    str
    database:  str
    db_version: Optional[str] = None
    timestamp: datetime


class SensorSummary(BaseModel):
    sensor_id:      str
    total_readings: int
    first_seen:     Optional[datetime] = None
    last_seen:      Optional[datetime] = None


class SensorsResponse(BaseModel):
    sensors: list[SensorSummary]
    count:   int


class AggregateWindow(BaseModel):
    sensor_id:    str
    window_start: datetime
    window_end:   datetime
    avg_temp:     Optional[float] = None
    min_temp:     Optional[float] = None
    max_temp:     Optional[float] = None
    stddev_temp:  Optional[float] = None
    avg_humidity: Optional[float] = None
    min_humidity: Optional[float] = None
    max_humidity: Optional[float] = None
    reading_count: int


class MetricsResponse(BaseModel):
    sensor_id: str
    latest:    Optional[AggregateWindow] = None


class HistoryResponse(BaseModel):
    sensor_id: str
    hours:     float
    windows:   list[AggregateWindow]
    count:     int
