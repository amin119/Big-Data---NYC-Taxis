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
    surge_score      int,
    z_score          double,
    PRIMARY KEY ((zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- ── Streaming: live weather snapshots ────────────────────────────────────────
-- Written by Spark Streaming every ~25 s (weather topic cadence).
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
CREATE TABLE IF NOT EXISTS daily_stats (
    zone_id       int,
    date          date,
    trip_count    bigint,
    total_revenue double,
    avg_tip       double,
    PRIMARY KEY ((zone_id), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- ── Live zone map: one row per zone, single partition for full-table read ────
-- Written by load_zone_centroids.py (lat/lon), updated by Spark Streaming.
CREATE TABLE IF NOT EXISTS zone_map_stats (
    snapshot         text,
    zone_id          int,
    lat              double,
    lon              double,
    zone_name        text,
    borough          text,
    trip_count       bigint,
    predicted_demand int,
    weather_label    text,
    surge_score      int,
    updated_at       timestamp,
    PRIMARY KEY (snapshot, zone_id)
);

-- ── Surge + Anomaly: pre-computed baseline per (zone, hour, day_of_week, weather) ──
-- Written once by scripts/compute_baseline.py from 3 years of R2 data.
-- Read at streaming job startup and held in driver memory as a lookup dict.
CREATE TABLE IF NOT EXISTS zone_baselines (
    zone_id       int,
    hour          int,
    day_of_week   int,
    weather_label text,
    mean_count    double,
    std_count     double,
    sample_size   int,
    PRIMARY KEY ((zone_id), hour, day_of_week, weather_label)
);

-- ── Surge + Anomaly: append-only anomaly event log ───────────────────────────
-- Written by Spark Streaming when |z_score| > 2.5.
-- Served by zone_api.py /anomalies endpoint → Grafana Infinity table panel.
CREATE TABLE IF NOT EXISTS anomaly_events (
    zone_id        int,
    event_time     timestamp,
    z_score        double,
    surge_score    int,
    classification text,
    trip_count     bigint,
    baseline_mean  double,
    weather_label  text,
    zone_name      text,
    PRIMARY KEY ((zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
