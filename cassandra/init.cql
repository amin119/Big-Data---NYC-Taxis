-- NYC Taxi Pipeline — Cassandra Schema
-- Run: docker exec -i cassandra cqlsh < cassandra/init.cql

CREATE KEYSPACE IF NOT EXISTS taxi_streaming
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE taxi_streaming;

-- ── Streaming: 5-min window stats per pickup zone ─────────────────────────────
-- Written by Spark Streaming every 30 s.
-- Grafana queries: SELECT * FROM trip_stats_by_window WHERE zone_id = ? LIMIT 50
CREATE TABLE IF NOT EXISTS trip_stats_by_window (
    zone_id          int,
    window_start     timestamp,
    trip_count       bigint,
    avg_fare         double,
    avg_distance     double,
    anomaly_count    bigint,
    weather_label    text,
    multiplier       double,
    predicted_demand int,
    PRIMARY KEY ((zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- ── Streaming: live weather snapshots ────────────────────────────────────────
-- Written by Spark Streaming every ~25 s (weather topic cadence).
-- Grafana queries: SELECT * FROM weather_snapshots WHERE source = 'open-meteo' LIMIT 1
CREATE TABLE IF NOT EXISTS weather_snapshots (
    source        text,
    recorded_at   timestamp,
    temperature_c double,
    precipitation double,
    windspeed     double,
    weathercode   int,
    is_raining    boolean,
    is_cold       boolean,
    weather_label text,
    PRIMARY KEY (source, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

-- ── Batch: daily aggregates per zone (written by Phase 5 Spark batch job) ────
-- Grafana queries: SELECT * FROM daily_stats WHERE zone_id = ?
CREATE TABLE IF NOT EXISTS daily_stats (
    zone_id       int,
    date          date,
    trip_count    bigint,
    total_revenue double,
    avg_tip       double,
    PRIMARY KEY ((zone_id), date)
) WITH CLUSTERING ORDER BY (date DESC);
