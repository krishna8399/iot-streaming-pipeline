-- TimescaleDB schema for IoT streaming pipeline
-- Runs automatically on first container start via /docker-entrypoint-initdb.d/

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Raw per-event data written by the Spark streaming job
CREATE TABLE IF NOT EXISTS sensor_readings (
    id          SERIAL,
    sensor_id   VARCHAR(64)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    temperature FLOAT,
    humidity    FLOAT,
    pressure    FLOAT,
    created_at  TIMESTAMPTZ   DEFAULT NOW()
);

SELECT create_hypertable(
    'sensor_readings',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_sr_sensor_time
    ON sensor_readings (sensor_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_sr_temp_outlier
    ON sensor_readings (timestamp DESC)
    WHERE temperature > 80 OR temperature < -20;

-- 5-minute window aggregates written by Spark Structured Streaming
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    sensor_id       VARCHAR(64)   NOT NULL,
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    avg_temp        FLOAT,
    min_temp        FLOAT,
    max_temp        FLOAT,
    stddev_temp     FLOAT,
    avg_humidity    FLOAT,
    min_humidity    FLOAT,
    max_humidity    FLOAT,
    reading_count   INTEGER,
    PRIMARY KEY (sensor_id, window_start)
);

SELECT create_hypertable(
    'sensor_aggregates',
    'window_start',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_sa_sensor_window
    ON sensor_aggregates (sensor_id, window_start DESC);

-- Continuous aggregate for Grafana dashboard queries
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_hourly
WITH (timescaledb.continuous) AS
SELECT
    sensor_id,
    time_bucket('1 hour', timestamp)  AS bucket,
    AVG(temperature)                   AS avg_temp,
    MIN(temperature)                   AS min_temp,
    MAX(temperature)                   AS max_temp,
    AVG(humidity)                      AS avg_humidity,
    COUNT(*)                           AS reading_count
FROM sensor_readings
GROUP BY sensor_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'sensor_hourly',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE
);

GRANT SELECT, INSERT, UPDATE ON sensor_readings    TO CURRENT_USER;
GRANT SELECT, INSERT, UPDATE ON sensor_aggregates  TO CURRENT_USER;
GRANT SELECT                  ON sensor_hourly      TO CURRENT_USER;
