"""
NYC Taxi Pipeline — Spark Structured Streaming  (Phase 3)

Reads from two Kafka topics:
  taxi-trips   → 5-min tumbling window aggregations per pickup zone
  taxi-weather → in-memory weather state + persisted to Cassandra

For every window × zone, computes:
  - Surge score  0-5  (trip_count vs pre-computed baseline mean)
  - Z-score           ((trip_count - mean) / std)
  - Anomaly event     written to anomaly_events when |Z| > 2.5,
                      classified as WEATHER_DRIVEN / TIME_ANOMALY / UNEXPLAINED

Run (from project root, with .venv active):
  python spark/streaming_job.py

Prerequisites:
  - docker compose up -d
  - docker exec -i cassandra cqlsh < cassandra/init.cql  (run once)
  - python scripts/load_zone_centroids.py                (run once)
  - python scripts/compute_baseline.py                   (run once)
  - python scripts/producer.py                           (in another terminal)
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

# Anomaly threshold — |Z| above this fires an anomaly event
ANOMALY_Z_THRESHOLD = float(os.getenv("ANOMALY_Z_THRESHOLD", "2.5"))

# Rush-hour windows used for TIME_ANOMALY classification (hour of day)
RUSH_HOURS = {7, 8, 9, 17, 18, 19}

# ── Shared weather state ───────────────────────────────────────────────────────
_weather = {
    "temperature_2m": 15.0,
    "precipitation":  0.0,
    "windspeed_10m":  10.0,
    "weathercode":    0,
    "is_raining":     False,
    "is_cold":        False,
    "weather_label":  "clear_mild",
    "recorded_at":    None,   # ISO string of last update — used for WEATHER_DRIVEN detection
}
_lock = threading.Lock()

# ── Cumulative zone trip totals ────────────────────────────────────────────────
_zone_totals: dict = {}
_zone_totals_lock  = threading.Lock()
RESET_FLAG         = "/tmp/zone_reset.flag"

# ── Zone centroids (loaded once from local JSON) ───────────────────────────────
_centroids: dict = {}
CENTROIDS_FILE = "zone_centroids.json"

# ── Baselines dict (loaded once from Cassandra at startup) ────────────────────
# key: (zone_id, hour, day_of_week, weather_label)  →  (mean, std)
_baselines: dict = {}


# ── Startup loaders ────────────────────────────────────────────────────────────

def load_centroids() -> None:
    global _centroids
    if not os.path.exists(CENTROIDS_FILE):
        print(f"  WARNING: {CENTROIDS_FILE} not found — run scripts/load_zone_centroids.py first")
        return
    with open(CENTROIDS_FILE) as f:
        data = json.load(f)
    _centroids = {int(k): v for k, v in data.items()}
    print(f"  Loaded {len(_centroids)} zone centroids")


def load_baselines() -> None:
    """Read zone_baselines from Cassandra into driver-memory dict."""
    global _baselines
    try:
        from cassandra.cluster import Cluster
        from cassandra.policies import RoundRobinPolicy
        cluster = Cluster(
            [CASSANDRA_HOST],
            port=int(CASSANDRA_PORT),
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4,
        )
        session = cluster.connect(CASSANDRA_KEYSPACE)
        rows = session.execute(
            "SELECT zone_id, hour, day_of_week, weather_label, mean_count, std_count "
            "FROM zone_baselines"
        )
        _baselines = {
            (r.zone_id, r.hour, r.day_of_week, r.weather_label): (r.mean_count, r.std_count)
            for r in rows
        }
        cluster.shutdown()
        print(f"  Loaded {len(_baselines):,} baseline entries from Cassandra")
        if not _baselines:
            print("  WARNING: zone_baselines is empty — run scripts/compute_baseline.py first")
            print("           Surge scores and Z-scores will be 0 until baselines are loaded.")
    except Exception as exc:
        print(f"  WARNING: Could not load baselines ({exc})")
        print("           Surge / anomaly detection will be inactive this run.")


# ── Weather helpers ────────────────────────────────────────────────────────────

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
            "recorded_at":    str(row.recorded_at       or ""),
        })


def demand_multiplier(is_raining: bool, is_cold: bool) -> float:
    if is_raining and is_cold:
        return 1.4
    if is_raining:
        return 1.3
    if is_cold:
        return 1.1
    return 1.0


# ── Surge + Anomaly helpers ────────────────────────────────────────────────────

def surge_score_from_ratio(ratio: float) -> int:
    """Map demand ratio (current / baseline_mean) to 0-5 surge score."""
    if ratio < 0.5:   return 0
    if ratio < 0.8:   return 1
    if ratio < 1.2:   return 2
    if ratio < 1.8:   return 3
    if ratio < 2.5:   return 4
    return 5


def classify_anomaly(hour: int, weather_changed: bool) -> str:
    """Return anomaly classification string."""
    if weather_changed:
        return "WEATHER_DRIVEN"
    if hour in RUSH_HOURS:
        return "TIME_ANOMALY"
    return "UNEXPLAINED"


def weather_recently_changed(recorded_at_str: str, window_minutes: int = 15) -> bool:
    """True if the last weather update arrived within window_minutes ago."""
    if not recorded_at_str:
        return False
    try:
        updated = datetime.fromisoformat(recorded_at_str.replace("Z", "+00:00"))
        delta   = (datetime.now(timezone.utc) - updated).total_seconds() / 60
        return delta <= window_minutes
    except Exception:
        return False


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
        .config("spark.driver.memory",          "2g")
        .config("spark.sql.shuffle.partitions", "4")
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
        update_weather(rows[-1])
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
                f"  [weather {epoch_id}] {w['weather_label']} | "
                f"{w['temperature_2m']:.1f}°C | "
                f"rain={w['is_raining']} cold={w['is_cold']}"
            )
        except Exception as exc:
            print(f"  [weather] Cassandra error: {exc}")

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

    parsed = raw.select(
        from_json(col("value").cast("string"), TRIP_SCHEMA).alias("d")
    ).select(
        "d.*",
        to_timestamp(col("d.ingested_at")).alias("event_time"),
    )

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

        weather        = get_weather()
        multiplier     = demand_multiplier(weather["is_raining"], weather["is_cold"])
        wlabel         = weather["weather_label"]
        wx_changed     = weather_recently_changed(weather.get("recorded_at", ""))

        rows           = df.collect()
        now            = datetime.now(timezone.utc)
        anomaly_rows   = []

        # ── Build enriched rows with surge + Z-score ───────────────────────────
        enriched = []
        for r in rows:
            zone_id    = int(r.zone_id)
            trip_count = int(r.trip_count)
            win_start  = r["window"]["start"]
            hour       = win_start.hour if win_start else now.hour
            dow        = win_start.weekday() if win_start else now.weekday()

            # Baseline lookup
            key = (zone_id, hour, dow, wlabel)
            baseline = _baselines.get(key)

            if baseline and baseline[0] > 0:
                mean, std = baseline
                z_score    = round((trip_count - mean) / std, 4)
                ratio      = trip_count / mean
                s_score    = surge_score_from_ratio(ratio)
            else:
                z_score = 0.0
                s_score = 2   # neutral — no baseline

            enriched.append({
                "zone_id":        zone_id,
                "window_start":   win_start,
                "trip_count":     trip_count,
                "avg_fare":       round(float(r.avg_fare or 0), 2),
                "avg_distance":   round(float(r.avg_distance or 0), 4),
                "anomaly_count":  int(r.anomaly_count or 0),
                "weather_label":  wlabel,
                "multiplier":     float(multiplier),
                "predicted_demand": int(trip_count * multiplier),
                "surge_score":    s_score,
                "z_score":        z_score,
            })

            # ── Anomaly detection ──────────────────────────────────────────────
            if abs(z_score) > ANOMALY_Z_THRESHOLD and baseline:
                classification = classify_anomaly(hour, wx_changed)
                zone_name = _centroids.get(zone_id, {}).get("zone_name", f"Zone {zone_id}")
                anomaly_rows.append({
                    "zone_id":        zone_id,
                    "event_time":     now,
                    "z_score":        z_score,
                    "surge_score":    s_score,
                    "classification": classification,
                    "trip_count":     trip_count,
                    "baseline_mean":  round(baseline[0], 2),
                    "weather_label":  wlabel,
                    "zone_name":      zone_name,
                })

        # ── Write trip_stats_by_window ─────────────────────────────────────────
        try:
            from pyspark.sql import Row
            stats_rows = [
                Row(
                    zone_id=e["zone_id"],
                    window_start=e["window_start"],
                    trip_count=e["trip_count"],
                    avg_fare=e["avg_fare"],
                    avg_distance=e["avg_distance"],
                    anomaly_count=e["anomaly_count"],
                    weather_label=e["weather_label"],
                    multiplier=e["multiplier"],
                    predicted_demand=e["predicted_demand"],
                    surge_score=e["surge_score"],
                    z_score=e["z_score"],
                )
                for e in enriched
            ]
            cass_write(spark.createDataFrame(stats_rows), "trip_stats_by_window")
        except Exception as exc:
            print(f"  [trips] trip_stats write error: {exc}")

        # ── Write anomaly_events ───────────────────────────────────────────────
        if anomaly_rows:
            try:
                from pyspark.sql import Row
                anom_spark_rows = [
                    Row(
                        zone_id=a["zone_id"],
                        event_time=a["event_time"],
                        z_score=a["z_score"],
                        surge_score=a["surge_score"],
                        classification=a["classification"],
                        trip_count=a["trip_count"],
                        baseline_mean=a["baseline_mean"],
                        weather_label=a["weather_label"],
                        zone_name=a["zone_name"],
                    )
                    for a in anomaly_rows
                ]
                cass_write(spark.createDataFrame(anom_spark_rows), "anomaly_events")
                for a in anomaly_rows:
                    print(
                        f"  ⚡ ANOMALY zone={a['zone_id']:>3} ({a['zone_name']}) "
                        f"Z={a['z_score']:+.2f} surge={a['surge_score']} "
                        f"[{a['classification']}] "
                        f"count={a['trip_count']} vs baseline={a['baseline_mean']:.1f}"
                    )
            except Exception as exc:
                print(f"  [trips] anomaly_events write error: {exc}")

        # ── Accumulate zone totals + write zone_map_stats ──────────────────────
        surge_by_zone = {e["zone_id"]: e["surge_score"] for e in enriched}

        with _zone_totals_lock:
            if os.path.exists(RESET_FLAG):
                _zone_totals.clear()
                os.remove(RESET_FLAG)
                print(f"  [trips {epoch_id}] RESET — zone totals cleared")
            for e in enriched:
                zid = e["zone_id"]
                _zone_totals[zid] = _zone_totals.get(zid, 0) + e["trip_count"]
            snapshot = dict(_zone_totals)

        if _centroids and snapshot:
            try:
                from pyspark.sql import Row
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
                        surge_score=surge_by_zone.get(zid, 2),
                        updated_at=now,
                    )
                    for zid, total in snapshot.items()
                    if zid in _centroids
                ]
                if map_rows:
                    cass_write(spark.createDataFrame(map_rows), "zone_map_stats")
            except Exception as exc:
                print(f"  [trips] zone_map_stats write error: {exc}")

        # ── Console summary ────────────────────────────────────────────────────
        n_anomalies = len(anomaly_rows)
        top = sorted(enriched, key=lambda e: e["surge_score"], reverse=True)[:3]
        top_str = "  ".join(
            f"z{e['zone_id']}=S{e['surge_score']}" for e in top
        )
        print(
            f"  [trips  {epoch_id}] {len(enriched)} zones | "
            f"weather: {wlabel} ×{multiplier} | "
            f"anomalies: {n_anomalies} | "
            f"top: {top_str}"
        )

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
    print(f"  Window    : 5 min tumbling  |  Trigger: 5 s")
    print(f"  Anomaly Z : >{ANOMALY_Z_THRESHOLD}")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n  Resetting live zone state ...")
    try:
        import urllib.request
        urllib.request.urlopen(
            urllib.request.Request(
                f"http://{os.getenv('ZONE_API_HOST', 'localhost')}:{os.getenv('ZONE_API_PORT', '5001')}/zones/reset",
                method="POST",
            ),
            timeout=5,
        )
        print("  Live zone state reset ✓")
    except Exception as exc:
        print(f"  WARNING: Could not reset live state ({exc}) — dashboard may show stale data")

    print("  Loading zone centroids ...")
    load_centroids()

    print("  Loading baselines from Cassandra ...")
    load_baselines()

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


if __name__ == "__main__":
    main()
