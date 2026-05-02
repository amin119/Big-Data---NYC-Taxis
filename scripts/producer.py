"""
NYC Taxi Pipeline — Kafka Producer (Phase 2)
Reads Yellow Taxi trips from Cloudflare R2 processed zone and streams them
row-by-row into the taxi-trips Kafka topic at ~20 rows/s.
Every WEATHER_EVERY_N rows, fetches live NYC weather from Open-Meteo and
publishes a snapshot to the taxi-weather topic.
Loops through all R2 month files continuously.
"""

import os
import sys
import json
import time
import math
import boto3
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER         = os.getenv("KAFKA_BROKER",      "localhost:9092")
TOPIC_TRIPS          = os.getenv("TOPIC_TRIPS",        "taxi-trips")
TOPIC_WEATHER        = os.getenv("TOPIC_WEATHER",      "taxi-weather")
REPLAY_DELAY         = float(os.getenv("REPLAY_DELAY",    "0.05"))
WEATHER_EVERY_N      = int(os.getenv("WEATHER_EVERY_N",   "500"))

R2_ENDPOINT          = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET            = os.getenv("R2_BUCKET",          "chabbah")
R2_PROCESSED         = os.getenv("R2_PROCESSED",       "processed/trips_clean")

if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
    print("ERROR: R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY must be set in .env")
    sys.exit(1)


# ── R2 helpers ─────────────────────────────────────────────────────────────────

def _r2():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=boto3.session.Config(
            signature_version="s3v4",
            connect_timeout=60,
            read_timeout=300,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def list_trip_files() -> list:
    """Return sorted list of all processed trip Parquet keys in R2."""
    paginator = _r2().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=R2_PROCESSED + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return sorted(keys)


def read_r2_parquet(key: str) -> pd.DataFrame:
    buf = BytesIO()
    _r2().download_fileobj(R2_BUCKET, key, buf)
    buf.seek(0)
    return pd.read_parquet(buf)


# ── Type-safe field helpers ────────────────────────────────────────────────────

def _int(v, default: int = 0) -> int:
    try:
        f = float(v)
        return default if math.isnan(f) or math.isinf(f) else int(f)
    except (TypeError, ValueError):
        return default


def _float(v, default: float = 0.0, decimals: int = 4) -> float:
    try:
        f = float(v)
        return default if math.isnan(f) or math.isinf(f) else round(f, decimals)
    except (TypeError, ValueError):
        return default


def _ts(v) -> str:
    try:
        return pd.Timestamp(v).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


# ── Message builders ───────────────────────────────────────────────────────────

def build_trip_message(row) -> dict:
    pickup  = pd.Timestamp(row.tpep_pickup_datetime)
    dropoff = pd.Timestamp(row.tpep_dropoff_datetime)
    dur     = max((dropoff - pickup).total_seconds() / 60, 0.0)
    fare    = _float(row.fare_amount, decimals=2)
    dist    = _float(row.trip_distance, decimals=4)
    return {
        "VendorID":               _int(row.VendorID),
        "tpep_pickup_datetime":   pickup.isoformat(),
        "tpep_dropoff_datetime":  dropoff.isoformat(),
        "passenger_count":        _int(row.passenger_count),
        "trip_distance":          dist,
        "PULocationID":           _int(row.PULocationID),
        "DOLocationID":           _int(row.DOLocationID),
        "fare_amount":            fare,
        "tip_amount":             _float(row.tip_amount, decimals=2),
        "total_amount":           _float(row.total_amount, decimals=2),
        "payment_type":           _int(row.payment_type),
        "trip_duration_min":      round(dur, 2),
        "hour_of_day":            pickup.hour,
        "day_of_week":            pickup.day_name(),
        "is_fare_anomaly":        fare < 0 or fare > 200,
        "is_distance_anomaly":    dist <= 0 or dist > 100,
        "ingested_at":            datetime.now(timezone.utc).isoformat(),
    }


# ── Weather ────────────────────────────────────────────────────────────────────

_last_weather: dict = {}


def fetch_weather() -> dict:
    """Fetch current NYC weather from Open-Meteo. Returns last known on failure."""
    global _last_weather
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=40.7128&longitude=-74.0060"
        "&current=temperature_2m,precipitation,windspeed_10m,weathercode"
        "&timezone=America%2FNew_York"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        cur  = resp.json()["current"]
        temp = float(cur["temperature_2m"])
        prec = float(cur["precipitation"])
        wind = float(cur["windspeed_10m"])
        code = int(cur["weathercode"])
        rain = prec > 0
        cold = temp < 5.0
        _last_weather = {
            "temperature_2m": temp,
            "precipitation":  prec,
            "windspeed_10m":  wind,
            "weathercode":    code,
            "is_raining":     rain,
            "is_cold":        cold,
            "weather_label":  ("rain" if rain else "clear") + "_" + ("cold" if cold else "mild"),
            "recorded_at":    datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        if not _last_weather:
            # Fallback snapshot so the topic always gets a message
            _last_weather = {
                "temperature_2m": 15.0, "precipitation": 0.0,
                "windspeed_10m": 10.0,  "weathercode": 0,
                "is_raining": False,    "is_cold": False,
                "weather_label": "clear_mild",
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        print(f"  [weather] fetch failed ({exc}) — reusing last snapshot")
    return _last_weather


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  NYC Taxi — Kafka Producer  (Phase 2)")
    print(f"  Broker  : {KAFKA_BROKER}")
    print(f"  Topics  : {TOPIC_TRIPS}  |  {TOPIC_WEATHER}")
    print(f"  Speed   : 1 row / {REPLAY_DELAY}s  (~{1/REPLAY_DELAY:.0f} rows/s)")
    print(f"  Weather : every {WEATHER_EVERY_N} rows")
    print("=" * 60)

    # ── Kafka producer ─────────────────────────────────────────────────────────
    print("\n  Connecting to Kafka ...", end="", flush=True)
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=30_000,
            max_block_ms=10_000,
        )
        print(" OK")
    except NoBrokersAvailable:
        print(" FAILED")
        print("  Kafka broker not reachable. Is Docker running?")
        print("  Run: docker compose up -d")
        sys.exit(1)

    # ── List R2 files ──────────────────────────────────────────────────────────
    print("  Listing trip files on R2 ...", end="", flush=True)
    keys = list_trip_files()
    if not keys:
        print(" NONE FOUND")
        print(f"  Expected: r2://{R2_BUCKET}/{R2_PROCESSED}/year=*/month=*/trips.parquet")
        print("  Run scripts/prepare_data.py first.")
        sys.exit(1)
    print(f" {len(keys)} files\n")
    for k in keys:
        label = k.split("year=")[-1].replace("/month=", "-M").replace("/trips.parquet", "")
        print(f"    {label}")
    print()

    # ── Streaming loop ─────────────────────────────────────────────────────────
    total_sent = 0
    loop       = 0

    while True:
        loop += 1
        print(f"  {'─'*56}")
        print(f"  Loop {loop} — {len(keys)} month files")
        print(f"  {'─'*56}\n")

        for key in keys:
            label = key.split("year=")[-1].replace("/month=", "-").replace("/trips.parquet", "")
            print(f"  [{label}] Downloading from R2 ...", end="", flush=True)
            try:
                df = read_r2_parquet(key)
            except Exception as exc:
                print(f" FAILED ({exc}) — skipping")
                continue

            # Shuffle so the stream doesn't look like ordered history
            df = df.sample(frac=1, random_state=loop).reset_index(drop=True)
            print(f" {len(df):,} rows")

            for row in df.itertuples(index=False):
                try:
                    msg = build_trip_message(row)
                    producer.send(
                        TOPIC_TRIPS,
                        key=str(msg["PULocationID"]),
                        value=msg,
                    )
                    total_sent += 1

                    if total_sent % WEATHER_EVERY_N == 0:
                        weather = fetch_weather()
                        producer.send(TOPIC_WEATHER, value=weather)
                        wlabel = weather.get("weather_label", "?")
                        print(
                            f"  [weather] {wlabel} | "
                            f"temp {weather['temperature_2m']:.1f}°C | "
                            f"precip {weather['precipitation']:.1f}mm"
                        )

                    if total_sent % 1_000 == 0:
                        producer.flush()
                        print(
                            f"  Sent {total_sent:,} total | "
                            f"{label} | zone {msg['PULocationID']:>3} | "
                            f"fare ${msg['fare_amount']:.2f} | "
                            f"{msg['day_of_week'][:3]} {msg['hour_of_day']:02d}h"
                        )

                    time.sleep(REPLAY_DELAY)

                except Exception as exc:
                    print(f"  Row error: {exc} — skipping")
                    continue

        producer.flush()
        print(f"\n  Loop {loop} complete — {total_sent:,} total messages sent\n")


if __name__ == "__main__":
    main()
