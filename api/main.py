"""
main.py — FastAPI REST server for the IoT streaming pipeline.

Endpoints:
  GET /health               — liveness + DB connectivity check
  GET /sensors              — list all sensor IDs with reading counts
  GET /metrics              — latest 5-min aggregate for a sensor
  GET /metrics/history      — all windows within the last N hours
"""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Generator, Optional

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from models import (
    AggregateWindow,
    DBConfig,
    HealthResponse,
    HistoryResponse,
    MetricsResponse,
    SensorSummary,
    SensorsResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("iot-api")

_db_config = DBConfig()
_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


def _create_pool(cfg: DBConfig) -> psycopg2.pool.ThreadedConnectionPool:
    log.info("Creating DB pool — host=%s db=%s user=%s", cfg.host, cfg.dbname, cfg.user)
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=cfg.pool_min,
        maxconn=cfg.pool_max,
        dsn=cfg.dsn,
    )


@contextmanager
def _get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    assert _pool is not None, "DB pool not initialised"
    conn = _pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def _conn_dep() -> Generator[psycopg2.extensions.connection, None, None]:
    if _pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection pool not available",
        )
    with _get_conn() as conn:
        yield conn


DBConn = Annotated[psycopg2.extensions.connection, Depends(_conn_dep)]


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global _pool
    import time as _time

    for attempt in range(1, 11):
        try:
            _pool = _create_pool(_db_config)
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    row = cur.fetchone()
                    log.info("DB connected (attempt %d) — %s", attempt, row[0] if row else "?")
            break
        except psycopg2.Error as exc:
            log.warning("DB not ready (attempt %d/10): %s", attempt, exc)
            if _pool is not None:
                try:
                    _pool.closeall()
                except Exception:
                    pass
                _pool = None
            if attempt < 10:
                _time.sleep(3)
    else:
        log.error("Could not connect to TimescaleDB after 10 attempts — running degraded")

    yield

    if _pool is not None:
        _pool.closeall()
        log.info("DB pool closed")


app = FastAPI(
    title="IoT Streaming Pipeline API",
    description="REST interface for real-time IoT sensor data stored in TimescaleDB.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _row_to_window(row: dict) -> AggregateWindow:
    return AggregateWindow(
        sensor_id=row["sensor_id"],
        window_start=row["window_start"],
        window_end=row["window_end"],
        avg_temp=row["avg_temp"],
        min_temp=row["min_temp"],
        max_temp=row["max_temp"],
        stddev_temp=row["stddev_temp"],
        avg_humidity=row["avg_humidity"],
        min_humidity=row["min_humidity"],
        max_humidity=row["max_humidity"],
        reading_count=row["reading_count"],
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health(conn: DBConn) -> HealthResponse:
    db_status   = "connected"
    db_version  = None
    http_status = status.HTTP_200_OK

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            row = cur.fetchone()
            db_version = row[0] if row else None
    except psycopg2.Error as exc:
        log.warning("Health check DB query failed: %s", exc)
        db_status   = str(exc)
        http_status = status.HTTP_503_SERVICE_UNAVAILABLE

    response = HealthResponse(
        status="ok" if db_status == "connected" else "degraded",
        database=db_status,
        db_version=db_version,
        timestamp=datetime.now(tz=timezone.utc),
    )

    if http_status != status.HTTP_200_OK:
        raise HTTPException(status_code=http_status, detail=response.model_dump())

    return response


@app.get("/sensors", response_model=SensorsResponse, tags=["sensors"])
def list_sensors(conn: DBConn) -> SensorsResponse:
    sql = """
        SELECT
            sensor_id,
            COUNT(*)        AS total_readings,
            MIN(timestamp)  AS first_seen,
            MAX(timestamp)  AS last_seen
        FROM sensor_readings
        GROUP BY sensor_id
        ORDER BY sensor_id
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    sensors = [
        SensorSummary(
            sensor_id=row["sensor_id"],
            total_readings=row["total_readings"],
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
        )
        for row in rows
    ]
    return SensorsResponse(sensors=sensors, count=len(sensors))


@app.get("/metrics", response_model=MetricsResponse, tags=["metrics"])
def latest_metrics(
    conn: DBConn,
    sensor_id: Annotated[str, Query(min_length=1, max_length=64)],
) -> MetricsResponse:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM sensor_readings WHERE sensor_id = %s LIMIT 1", (sensor_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Sensor '{sensor_id}' not found")

    latest_sql = """
        SELECT sensor_id, window_start, window_end,
               avg_temp, min_temp, max_temp, stddev_temp,
               avg_humidity, min_humidity, max_humidity, reading_count
        FROM sensor_aggregates
        WHERE sensor_id = %s
        ORDER BY window_start DESC
        LIMIT 1
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(latest_sql, (sensor_id,))
        row = cur.fetchone()

    return MetricsResponse(sensor_id=sensor_id, latest=_row_to_window(row) if row else None)


@app.get("/metrics/history", response_model=HistoryResponse, tags=["metrics"])
def metrics_history(
    conn: DBConn,
    sensor_id: Annotated[str, Query(min_length=1, max_length=64)],
    hours: Annotated[float, Query(gt=0, le=8760)] = 24.0,
) -> HistoryResponse:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM sensor_readings WHERE sensor_id = %s LIMIT 1", (sensor_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Sensor '{sensor_id}' not found")

    # Look-back relative to the most recent window in DB, not wall-clock time.
    # This ensures the endpoint works correctly with historical datasets.
    history_sql = """
        SELECT sensor_id, window_start, window_end,
               avg_temp, min_temp, max_temp, stddev_temp,
               avg_humidity, min_humidity, max_humidity, reading_count
        FROM sensor_aggregates
        WHERE sensor_id = %(sensor_id)s
          AND window_start >= (
              SELECT MAX(window_start) - (%(hours)s * INTERVAL '1 hour')
              FROM   sensor_aggregates
              WHERE  sensor_id = %(sensor_id)s
          )
        ORDER BY window_start ASC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(history_sql, {"sensor_id": sensor_id, "hours": hours})
        rows = cur.fetchall()

    windows = [_row_to_window(row) for row in rows]
    return HistoryResponse(sensor_id=sensor_id, hours=hours, windows=windows, count=len(windows))
