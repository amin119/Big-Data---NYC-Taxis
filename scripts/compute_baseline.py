"""
NYC Taxi Pipeline — Zone Baseline Computer  (run once before streaming)

Reads all processed trip Parquet files from Cloudflare R2, aggregates them
into 5-minute window trip counts per pickup zone, then computes the mean and
standard deviation for every (zone, hour, day_of_week, weather_label) group.

These baselines are written to Cassandra's zone_baselines table and loaded
into driver memory by streaming_job.py at startup to power:
  - Surge score  (trip_count / baseline_mean → 0-5 scale)
  - Z-score      ((trip_count - mean) / std  → anomaly threshold 2.5)

Run once (or re-run after adding new months of data):
    python scripts/compute_baseline.py

Prerequisites:
    - docker compose up -d  (Cassandra must be healthy)
    - docker exec -i cassandra cqlsh < cassandra/init.cql
    - R2 must have processed trip files  (run prepare_data.py first)
"""

import gevent.monkey; gevent.monkey.patch_all()  # must be before all other imports for Python 3.12

import os
import sys
import math
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

# ── R2 config ──────────────────────────────────────────────────────────────────
R2_ENDPOINT          = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET            = os.getenv("R2_BUCKET",    "chabbah")
R2_PROCESSED         = os.getenv("R2_PROCESSED", "processed/trips_clean")
R2_RAW_WEATHER       = os.getenv("R2_RAW_WEATHER", "raw/weather")

# ── Cassandra config ───────────────────────────────────────────────────────────
CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")

# ── Baseline params ────────────────────────────────────────────────────────────
WINDOW_MINUTES = 5       # aggregate window size — must match streaming_job.py
MIN_SAMPLES    = 3       # minimum windows needed to store a baseline entry
MIN_STD        = 0.5     # floor for std to avoid division-by-zero in Z-score

# ── Local cache ────────────────────────────────────────────────────────────────
CACHE_DIR      = os.getenv("BASELINE_CACHE_DIR", ".cache/baseline_parquet")
BASELINE_CACHE = ".cache/baseline_computed.parquet"


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


def list_keys(prefix: str) -> list[str]:
    paginator = _r2().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return sorted(keys)


def read_parquet(key: str) -> pd.DataFrame:
    cache_path = os.path.join(CACHE_DIR, key)
    if os.path.exists(cache_path):
        return pd.read_parquet(cache_path)
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    buf = BytesIO()
    _r2().download_fileobj(R2_BUCKET, key, buf)
    buf.seek(0)
    df = pd.read_parquet(buf)
    df.to_parquet(cache_path, index=False, compression="snappy")
    return df


# ── Compute baselines (chunk-by-chunk to avoid OOM) ───────────────────────────
# Instead of loading all 175M rows at once, each file is:
#   1. downloaded  (~3-4M rows)
#   2. aggregated to 5-min window counts  (~tens of thousands of rows)
#   3. freed from memory
# Only the small aggregated chunks are kept and concatenated.

def compute_baselines_chunked(weather: pd.DataFrame) -> pd.DataFrame:
    keys = list_keys(R2_PROCESSED)
    if not keys:
        print(f"ERROR: no trip files found under r2://{R2_BUCKET}/{R2_PROCESSED}/")
        sys.exit(1)

    # Normalise weather date_hour to tz-naive for merging
    if not weather.empty and weather["date_hour"].dt.tz is not None:
        weather = weather.copy()
        weather["date_hour"] = weather["date_hour"].dt.tz_localize(None)

    print(f"  Found {len(keys)} trip file(s) on R2")
    chunks = []
    total_raw = 0

    for i, key in enumerate(keys, 1):
        label = key.split("year=")[-1].replace("/month=", "-M").replace("/trips.parquet", "")
        print(f"  [{i:>3}/{len(keys)}] {label} ...", end="", flush=True)
        try:
            df = read_parquet(key)
            df = df[["tpep_pickup_datetime", "PULocationID"]].copy()
            df["tpep_pickup_datetime"] = pd.to_datetime(
                df["tpep_pickup_datetime"], utc=True, errors="coerce"
            )
            df = df.dropna(subset=["tpep_pickup_datetime"])
            df = df[df["PULocationID"].between(1, 263)]
            total_raw += len(df)

            # Compute temporal features
            df["window"]    = df["tpep_pickup_datetime"].dt.floor(f"{WINDOW_MINUTES}min")
            df["hour"]      = df["tpep_pickup_datetime"].dt.hour.astype("int8")
            df["dow"]       = df["tpep_pickup_datetime"].dt.dayofweek.astype("int8")
            df["date_hour"] = df["tpep_pickup_datetime"].dt.floor("h").dt.tz_localize(None)

            # Aggregate to window level IMMEDIATELY — raw rows are no longer needed
            windowed = (
                df.groupby(["PULocationID", "window", "hour", "dow", "date_hour"])
                .size()
                .reset_index(name="trip_count")
            )
            del df  # free ~3-4M raw rows right away

            # Attach weather label
            if not weather.empty:
                windowed = windowed.merge(
                    weather[["date_hour", "weather_label"]], on="date_hour", how="left"
                )
                windowed["weather_label"] = windowed["weather_label"].fillna("clear_mild")
            else:
                windowed["weather_label"] = "clear_mild"

            # Drop columns we no longer need before accumulating
            windowed = windowed[["PULocationID", "hour", "dow", "weather_label", "trip_count"]]
            chunks.append(windowed)
            print(f" {len(windowed):,} windows")

        except Exception as exc:
            print(f" SKIP ({exc})")

    print(f"\n  Total raw trips processed : {total_raw:,}")
    print(f"  Concatenating {len(chunks)} window chunks ...")
    all_windows = pd.concat(chunks, ignore_index=True)
    del chunks
    print(f"  Total window rows         : {len(all_windows):,}")
    return all_windows


# ── Load weather ───────────────────────────────────────────────────────────────

def load_weather() -> pd.DataFrame:
    keys = list_keys(R2_RAW_WEATHER)
    if not keys:
        print("  WARNING: no weather files found — all baselines will use 'clear_mild'")
        return pd.DataFrame(columns=["date_hour", "weather_label"])

    print(f"  Found {len(keys)} weather file(s) on R2")
    dfs = []
    for key in keys:
        try:
            df = read_parquet(key)
            df = df[["datetime", "weather_label"]].copy()
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
            df = df.dropna(subset=["datetime"])
            dfs.append(df)
        except Exception:
            pass

    weather = pd.concat(dfs, ignore_index=True)
    weather["date_hour"] = weather["datetime"].dt.floor("h")
    weather = weather.drop_duplicates("date_hour")[["date_hour", "weather_label"]]
    print(f"  Weather rows: {len(weather):,}")
    return weather


# ── Final aggregation: mean / std from accumulated window chunks ───────────────

def compute_baselines(all_windows: pd.DataFrame) -> pd.DataFrame:
    print("  Aggregating mean / std per (zone, hour, dow, weather_label) ...")

    baseline = (
        all_windows.groupby(["PULocationID", "hour", "dow", "weather_label"])["trip_count"]
        .agg(["mean", "std", "count"])
        .reset_index()
        .rename(columns={
            "PULocationID": "zone_id",
            "dow":          "day_of_week",
            "mean":         "mean_count",
            "std":          "std_count",
            "count":        "sample_size",
        })
    )

    # Apply floors
    baseline["std_count"] = baseline["std_count"].fillna(MIN_STD).clip(lower=MIN_STD)
    baseline["mean_count"] = baseline["mean_count"].round(4)
    baseline["std_count"]  = baseline["std_count"].round(4)

    # Drop entries with too few samples — they're not reliable
    before = len(baseline)
    baseline = baseline[baseline["sample_size"] >= MIN_SAMPLES]
    print(f"  Baseline rows: {len(baseline):,}  (dropped {before - len(baseline):,} low-sample entries)")

    return baseline


# ── Write to Cassandra ─────────────────────────────────────────────────────────

def write_to_cassandra(baseline: pd.DataFrame) -> None:
    from cassandra.cluster import Cluster
    from cassandra.policies import RoundRobinPolicy

    print(f"\n  Connecting to Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} ...", end="", flush=True)
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=4,
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)
    print(" OK")

    # Truncate so a re-run replaces stale data
    print("  Truncating zone_baselines ...")
    session.execute("TRUNCATE zone_baselines")

    stmt = session.prepare("""
        INSERT INTO zone_baselines
            (zone_id, hour, day_of_week, weather_label, mean_count, std_count, sample_size)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    total = len(baseline)
    print(f"  Writing {total:,} baseline rows ...")
    batch_size = 500
    written = 0

    for i in range(0, total, batch_size):
        chunk = baseline.iloc[i : i + batch_size]
        for row in chunk.itertuples(index=False):
            session.execute(stmt, (
                int(row.zone_id),
                int(row.hour),
                int(row.day_of_week),
                str(row.weather_label),
                float(row.mean_count),
                float(row.std_count),
                int(row.sample_size),
            ))
        written += len(chunk)
        pct = written / total * 100
        print(f"  {written:>7,} / {total:,}  ({pct:.0f}%)", end="\r", flush=True)

    print(f"  {written:,} rows written to zone_baselines           ")
    cluster.shutdown()


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(baseline: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("  Zone Baseline Computation — Summary")
    print("=" * 60)
    print(f"  Baseline entries  : {len(baseline):,}")
    print(f"  Unique zones      : {baseline['zone_id'].nunique()}")
    print(f"  Weather labels    : {sorted(baseline['weather_label'].unique())}")
    print(f"  Avg mean_count    : {baseline['mean_count'].mean():.2f} trips/window")
    print(f"  Avg std_count     : {baseline['std_count'].mean():.2f}")
    print(f"  Avg sample_size   : {baseline['sample_size'].mean():.0f} windows")
    print()
    print("  Top 5 busiest zones (by mean trip count, clear_mild, 8h Fri):")
    top = (
        baseline[
            (baseline["hour"] == 8) &
            (baseline["day_of_week"] == 4) &
            (baseline["weather_label"] == "clear_mild")
        ]
        .nlargest(5, "mean_count")[["zone_id", "mean_count", "std_count", "sample_size"]]
    )
    print(top.to_string(index=False))
    print("=" * 60)
    print("\n  Baselines ready. You can now start the streaming job.")
    print("  Run: python spark/streaming_job.py")
    print("=" * 60)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print("ERROR: R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY must be set in .env")
        sys.exit(1)

    print("=" * 60)
    print("  NYC Taxi — Zone Baseline Computer")
    print(f"  R2 bucket  : {R2_BUCKET}/{R2_PROCESSED}")
    print(f"  Cassandra  : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}")
    print(f"  Window     : {WINDOW_MINUTES} min  |  min samples: {MIN_SAMPLES}")
    print("=" * 60)

    if os.path.exists(BASELINE_CACHE):
        print(f"\n  Found cached baselines at {BASELINE_CACHE} — skipping R2 download + computation.")
        print("  Delete this file to force a full recompute.\n")
        baseline = pd.read_parquet(BASELINE_CACHE)
        print(f"  Loaded {len(baseline):,} baseline rows from cache.")
    else:
        print("\n── Loading weather from R2 ──")
        weather = load_weather()

        print("\n── Processing trips from R2 (chunk by chunk) ──")
        all_windows = compute_baselines_chunked(weather)

        print("\n── Computing final baselines ──")
        baseline = compute_baselines(all_windows)
        del all_windows

        os.makedirs(os.path.dirname(BASELINE_CACHE), exist_ok=True)
        baseline.to_parquet(BASELINE_CACHE, index=False, compression="snappy")
        print(f"  Baseline checkpoint saved → {BASELINE_CACHE}")

    print("\n── Writing to Cassandra ──")
    write_to_cassandra(baseline)

    print_summary(baseline)


if __name__ == "__main__":
    main()
