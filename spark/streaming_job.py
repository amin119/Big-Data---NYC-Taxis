"""
NYC Taxi Pipeline — Spark Structured Streaming  (Phase 3)

Reads from two Kafka topics:
  taxi-trips   → 5-min tumbling window aggregations per pickup zone
  taxi-weather → in-memory weather state + persisted to Cassandra

Applies a weather-based demand multiplier to each window result,
then writes everything to Cassandra for Grafana to visualise.

Run (from project root, with .venv active):
  python spark/streaming_job.py

First run downloads ~200 MB of JARs into ~/.ivy2 — subsequent runs are instant.
Prerequisites:
  - docker compose up -d  (Kafka + Cassandra must be healthy)
  - python scripts/producer.py  (must be running in another terminal)
  - docker exec -i cassandra cqlsh < cassandra/init.cql  (run once)
"""

import os
import sys
import json
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg,
    sum as spark_sum, when, to_timestamp, lit,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, BooleanType,
)

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER",      "localhost:9092")
TOPIC_TRIPS        = os.getenv("TOPIC_TRIPS",        "taxi-trips")
TOPIC_WEATHER      = os.getenv("TOPIC_WEATHER",      "taxi-weather")
CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT     = os.getenv("CASSANDRA_PORT",     "9042")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")
CHECKPOINT_DIR     = "checkpoints/streaming"

# ── Shared weather state ───────────────────────────────────────────────────────
# Updated by the weather stream's foreachBatch; read by the trips stream.
_weather = {
    "temperature_2m": 15.0,
    "precipitation":  0.0,
    "windspeed_10m":  10.0,
    "weathercode":    0,
    "is_raining":     False,
    "is_cold":        False,
    "weather_label":  "clear_mild",
}
_lock = threading.Lock()

# ── Cumulative zone trip totals (accumulates across all micro-batches) ─────────
# Keyed by zone_id (int). Persists for the lifetime of the streaming job so the
# heatmap only ever grows brighter — no sawtooth resets at window boundaries.
_zone_totals: dict = {}
_zone_totals_lock = threading.Lock()
RESET_FLAG = "/tmp/zone_reset.flag"

# ── Zone centroids (loaded once at startup from local JSON file) ───────────────
# zone_id → {"lat": float, "lon": float, "zone_name": str, "borough": str}
# File is written by scripts/load_zone_centroids.py
_centroids: dict = {}
CENTROIDS_FILE = "zone_centroids.json"


def load_centroids() -> None:
    global _centroids
    if not os.path.exists(CENTROIDS_FILE):
        print(f"  WARNING: {CENTROIDS_FILE} not found — run scripts/load_zone_centroids.py first")
        return
    with open(CENTROIDS_FILE) as f:
        data = json.load(f)
    _centroids = {int(k): v for k, v in data.items()}
    print(f"  Loaded {len(_centroids)} zone centroids from {CENTROIDS_FILE}")


def get_weather() -> dict:
    with _lock:
        return dict(_weather)


def update_weather(row) -> None:
    with _lock:
        _weather.update({
            "temperature_2m": float(row.temperature_2m or 15.0),
            "precipitation":  float(row.precipitation  or 0.0),
            "windspeed_10m":  float(row.windspeed_10m  or 10.0),
            "weathercode":    int(row.weathercode       or 0),
            "is_raining":     bool(row.is_raining),
            "is_cold":        bool(row.is_cold),
            "weather_label":  str(row.weather_label     or "clear_mild"),
        })


def demand_multiplier(is_raining: bool, is_cold: bool) -> float:
    if is_raining and is_cold:
        return 1.4
    if is_raining:
        return 1.3
    if is_cold:
        return 1.1
    return 1.0


# ── Kafka message schemas ──────────────────────────────────────────────────────

TRIP_SCHEMA = StructType([
    StructField("VendorID",              IntegerType(), True),
    StructField("tpep_pickup_datetime",  StringType(),  True),
    StructField("tpep_dropoff_datetime", StringType(),  True),
    StructField("passenger_count",       IntegerType(), True),
    StructField("trip_distance",         FloatType(),   True),
    StructField("PULocationID",          IntegerType(), True),
    StructField("DOLocationID",          IntegerType(), True),
    StructField("fare_amount",           FloatType(),   True),
    StructField("tip_amount",            FloatType(),   True),
    StructField("total_amount",          FloatType(),   True),
    StructField("payment_type",          IntegerType(), True),
    StructField("trip_duration_min",     FloatType(),   True),
    StructField("hour_of_day",           IntegerType(), True),
    StructField("day_of_week",           StringType(),  True),
    StructField("is_fare_anomaly",       BooleanType(), True),
    StructField("is_distance_anomaly",   BooleanType(), True),
    StructField("ingested_at",           StringType(),  True),
])

WEATHER_SCHEMA = StructType([
    StructField("temperature_2m", FloatType(),   True),
    StructField("precipitation",  FloatType(),   True),
    StructField("windspeed_10m",  FloatType(),   True),
    StructField("weathercode",    IntegerType(), True),
    StructField("is_raining",     BooleanType(), True),
    StructField("is_cold",        BooleanType(), True),
    StructField("weather_label",  StringType(),  True),
    StructField("recorded_at",    StringType(),  True),
])


# ── Cassandra write helper ─────────────────────────────────────────────────────

def cass_write(df, table: str) -> None:
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=CASSANDRA_KEYSPACE)
        .mode("append")
        .save()
    )


# ── SparkSession ───────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NYCTaxiStreaming")
        .master("local[2]")
        .config("spark.driver.memory",            "2g")
        .config("spark.sql.shuffle.partitions",   "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        )
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )


# ── Weather stream ─────────────────────────────────────────────────────────────

def start_weather_stream(spark):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_WEATHER)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("d")
    ).select("d.*")

    def on_batch(df, epoch_id):
        rows = df.collect()
        if not rows:
            return

        # Update in-memory state with the latest message in this micro-batch
        update_weather(rows[-1])

        # Persist to Cassandra for Grafana weather panel
        out = df.select(
            lit("open-meteo").alias("source"),
            to_timestamp(col("recorded_at")).alias("recorded_at"),
            col("temperature_2m").cast("double").alias("temperature_c"),
            col("precipitation").cast("double"),
            col("windspeed_10m").cast("double").alias("windspeed"),
            col("weathercode"),
            col("is_raining"),
            col("is_cold"),
            col("weather_label"),
        )
        try:
            cass_write(out, "weather_snapshots")
            w = get_weather()
            print(
                f"  [weather epoch {epoch_id}] "
                f"{w['weather_label']} | "
                f"{w['temperature_2m']:.1f}°C | "
                f"rain={w['is_raining']} cold={w['is_cold']}"
            )
        except Exception as exc:
            print(f"  [weather] Cassandra write error: {exc}")

    return (
        parsed.writeStream
        .foreachBatch(on_batch)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/weather")
        .start()
    )


# ── Trips stream ───────────────────────────────────────────────────────────────

def start_trips_stream(spark):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_TRIPS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and add event_time column for windowing
    parsed = raw.select(
        from_json(col("value").cast("string"), TRIP_SCHEMA).alias("d")
    ).select(
        "d.*",
        to_timestamp(col("d.ingested_at")).alias("event_time"),
    )

    # 5-minute tumbling window aggregation per pickup zone
    windowed = (
        parsed
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("PULocationID").alias("zone_id"),
        )
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance"),
            spark_sum(
                when(col("is_fare_anomaly") | col("is_distance_anomaly"), 1).otherwise(0)
            ).alias("anomaly_count"),
        )
    )

    def on_batch(df, epoch_id):
        if df.rdd.isEmpty():
            return

        weather    = get_weather()
        multiplier = demand_multiplier(weather["is_raining"], weather["is_cold"])
        wlabel     = weather["weather_label"]

        out = df.select(
            col("zone_id"),
            col("window.start").alias("window_start"),
            col("trip_count"),
            spark_round(col("avg_fare"),     2).alias("avg_fare"),
            spark_round(col("avg_distance"), 4).alias("avg_distance"),
            col("anomaly_count"),
            lit(wlabel).alias("weather_label"),
            lit(float(multiplier)).alias("multiplier"),
            (col("trip_count") * lit(multiplier)).cast("int").alias("predicted_demand"),
        )

        try:
            cass_write(out, "trip_stats_by_window")
            rows = out.collect()
            n = len(rows)
            print(
                f"  [trips  epoch {epoch_id}] "
                f"{n} zones | "
                f"weather: {wlabel} ×{multiplier} | "
                f"window: {max(r.window_start for r in rows)}"
            )
            # Accumulate trip counts into the rolling totals dict
            with _zone_totals_lock:
                if os.path.exists(RESET_FLAG):
                    _zone_totals.clear()
                    os.remove(RESET_FLAG)
                    print(f"  [trips  epoch {epoch_id}] RESET — zone totals cleared")
                for r in rows:
                    zid = int(r.zone_id)
                    _zone_totals[zid] = _zone_totals.get(zid, 0) + int(r.trip_count)
                snapshot = dict(_zone_totals)

            # Write ALL known zones (not just those active this batch) so the
            # heatmap reflects cumulative activity and never shows stale zeros.
            if _centroids and snapshot:
                from pyspark.sql import Row
                now = datetime.now(timezone.utc)
                map_rows = [
                    Row(
                        snapshot="current",
                        zone_id=zid,
                        lat=_centroids[zid]["lat"],
                        lon=_centroids[zid]["lon"],
                        zone_name=_centroids[zid]["zone_name"],
                        borough=_centroids[zid]["borough"],
                        trip_count=total,
                        predicted_demand=int(total * multiplier),
                        weather_label=wlabel,
                        updated_at=now,
                    )
                    for zid, total in snapshot.items()
                    if zid in _centroids
                ]
                if map_rows:
                    cass_write(spark.createDataFrame(map_rows), "zone_map_stats")
        except Exception as exc:
            print(f"  [trips] Cassandra write error: {exc}")

    return (
        windowed.writeStream
        .foreachBatch(on_batch)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/trips")
        .outputMode("update")
        .start()
    )


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  NYC Taxi — Spark Structured Streaming  (Phase 3)")
    print(f"  Kafka     : {KAFKA_BROKER}")
    print(f"  Topics    : {TOPIC_TRIPS}  |  {TOPIC_WEATHER}")
    print(f"  Cassandra : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}")
    print(f"  Window    : 5 min tumbling  |  Trigger : 5 s")
    print(f"  Checkpoint: {CHECKPOINT_DIR}/")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("  Loading zone centroids ...")
    load_centroids()

    print("  Starting weather stream ...")
    weather_q = start_weather_stream(spark)

    print("  Starting trips stream ...")
    trips_q = start_trips_stream(spark)

    print("\n  Both streams active. Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n  Shutting down ...")
        trips_q.stop()
        weather_q.stop()
        spark.stop()
        print("  Stopped.")


def _init_cassandra_schema():
    """Create keyspace and tables using cassandra-driver if they don't exist."""
    try:
        from cassandra.io.asyncioreactor import AsyncioConnection  # must precede Cluster import
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT), connection_class=AsyncioConnection)
        session = cluster.connect()

        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.set_keyspace(CASSANDRA_KEYSPACE)

        session.execute("""
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
            ) WITH CLUSTERING ORDER BY (window_start DESC)
        """)

        session.execute("""
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
            ) WITH CLUSTERING ORDER BY (recorded_at DESC)
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                zone_id       int,
                date          date,
                trip_count    bigint,
                total_revenue double,
                avg_tip       double,
                PRIMARY KEY ((zone_id), date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)

        session.execute("""
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
                updated_at       timestamp,
                PRIMARY KEY (snapshot, zone_id)
            )
        """)

        cluster.shutdown()
    except Exception as exc:
        print(f"\n  WARNING: Could not auto-init schema ({exc})")
        print("  Run manually: docker exec -i cassandra cqlsh < cassandra/init.cql")


if __name__ == "__main__":
    main()
