"""
NYC Taxi Pipeline — Data Lake Preparation (Cloudflare R2 backend)
Downloads NYC Yellow Taxi data (2022 → today, ~4 GB max) from the TLC CDN
and writes it directly to Cloudflare R2 in Hive-partitioned Parquet format.

Data Lake zones on R2 (bucket: chabbah):
  raw/trips/year=YYYY/month=MM/trips.parquet       ← raw ingestion zone
  processed/trips_clean/year=YYYY/month=MM/        ← cleaned zone
  raw/weather/year=YYYY/month=MM/weather.parquet   ← weather zone

Idempotent: months already on R2 are detected and skipped.
TLC publishes with ~2-month lag — 404s are logged, not treated as errors.
"""

import os
import sys
import time
import boto3
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# ── Cloudflare R2 config ───────────────────────────────────────────────────────
R2_ENDPOINT       = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID  = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET         = os.getenv("R2_BUCKET", "chabbah")

if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
    print("ERROR: R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY must be set in .env")
    sys.exit(1)

# ── Data Lake path prefixes ────────────────────────────────────────────────────
R2_RAW_TRIPS   = os.getenv("R2_RAW_TRIPS",   "raw/trips")
R2_RAW_WEATHER = os.getenv("R2_RAW_WEATHER", "raw/weather")
R2_PROCESSED   = os.getenv("R2_PROCESSED",   "processed/trips_clean")

# ── Year range ─────────────────────────────────────────────────────────────────
START_YEAR = int(os.getenv("START_YEAR", 2022))
END_YEAR   = int(os.getenv("END_YEAR",   datetime.now().year))

# ── TLC source — Yellow Taxi ONLY ──────────────────────────────────────────────
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

# ── Unified schema ─────────────────────────────────────────────────────────────
UNIFIED_COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
]


# ── R2 client ──────────────────────────────────────────────────────────────────

def r2_client():
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


def key_exists(key: str) -> bool:
    try:
        r2_client().head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def upload_parquet(df: pd.DataFrame, key: str) -> float:
    """Serialize df to Parquet in memory, upload to R2. Returns MB uploaded."""
    buf = BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    size = buf.tell()
    buf.seek(0)
    # upload_fileobj uses multipart upload automatically for files > 8 MB,
    # which avoids the connection-reset errors that put_object causes on large bodies.
    r2_client().upload_fileobj(buf, R2_BUCKET, key)
    return size / (1024 * 1024)


# ── Data cleaning ──────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only the 11 pipeline columns
    available = [c for c in UNIFIED_COLS if c in df.columns]
    for col in UNIFIED_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[UNIFIED_COLS].copy()

    # Drop physically impossible rows only — this is NOT sampling
    df = df.dropna(subset=["tpep_pickup_datetime", "fare_amount", "trip_distance"])
    df = df[df["trip_distance"] > 0]
    if df["passenger_count"].notna().any():
        df = df[df["passenger_count"].isna() | (df["passenger_count"] > 0)]
    return df


# ── Part A — Trips ─────────────────────────────────────────────────────────────

def process_month(year: int, month: int, stats: list) -> bool:
    raw_key  = f"{R2_RAW_TRIPS}/year={year}/month={month:02d}/trips.parquet"
    proc_key = f"{R2_PROCESSED}/year={year}/month={month:02d}/trips.parquet"

    if key_exists(raw_key) and key_exists(proc_key):
        print(f"  [{year}-{month:02d}] Already on R2 — skipping")
        stats.append({"year": year, "month": month, "status": "skipped", "rows": 0, "mb": 0})
        return True

    url = TLC_URL.format(year=year, month=month)

    # Check 404 before downloading (TLC publication lag ~2 months)
    try:
        if requests.head(url, timeout=15).status_code == 404:
            print(f"  [{year}-{month:02d}] Not published yet — skipping")
            stats.append({"year": year, "month": month, "status": "not_available", "rows": 0, "mb": 0})
            return False
    except Exception:
        pass

    # Download
    for attempt in range(1, 4):
        try:
            print(f"  [{year}-{month:02d}] Downloading (attempt {attempt}) ...", end="", flush=True)
            df = pd.read_parquet(url)
            print(f" {len(df):,} rows", end="", flush=True)
            break
        except Exception as exc:
            print(f" error: {exc}")
            if attempt < 3:
                time.sleep(15)
            else:
                stats.append({"year": year, "month": month, "status": "failed", "rows": 0, "mb": 0})
                return False

    raw_rows = len(df)
    df       = clean(df)
    clean_rows = len(df)

    raw_mb  = upload_parquet(df, raw_key)
    proc_mb = upload_parquet(df, proc_key)

    print(
        f" → {clean_rows:,} rows kept "
        f"(dropped {raw_rows - clean_rows:,})  |  "
        f"{raw_mb:.0f} MB → r2://{R2_BUCKET}/{raw_key}"
    )
    stats.append({"year": year, "month": month, "status": "ok", "rows": clean_rows, "mb": raw_mb})
    return True


def process_all_trips() -> list:
    stats = []
    now   = datetime.now()
    total = sum(
        1
        for y in range(START_YEAR, END_YEAR + 1)
        for m in range(1, 13)
        if not (y == now.year and m >= now.month) and y <= now.year
    )
    done = 0

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n{'─'*60}")
        print(f"  Year {year}")
        print(f"{'─'*60}")
        for month in range(1, 13):
            if year == now.year and month >= now.month:
                break
            if year > now.year:
                break
            done += 1
            print(f"  [{done}/{total}]", end=" ")
            process_month(year, month, stats)

    return stats


# ── Part B — Weather ───────────────────────────────────────────────────────────

def download_weather():
    print(f"\n\n{'='*60}")
    print(f"  Downloading NYC hourly weather {START_YEAR}–{END_YEAR} → R2")
    print(f"{'='*60}")

    now = datetime.now()
    for year in range(START_YEAR, END_YEAR + 1):
        start_date = f"{year}-01-01"
        end_date   = f"{year}-12-31" if year < now.year else now.strftime("%Y-%m-%d")
        print(f"  [{year}] {start_date} → {end_date} ...", end="", flush=True)

        params = {
            "latitude":   40.7128,
            "longitude":  -74.0060,
            "start_date": start_date,
            "end_date":   end_date,
            "hourly":     "temperature_2m,precipitation,windspeed_10m,weathercode",
            "timezone":   "America/New_York",
        }

        data = None
        for attempt in range(1, 4):
            try:
                resp = requests.get(
                    "https://archive-api.open-meteo.com/v1/archive",
                    params=params, timeout=90
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as exc:
                if attempt < 3:
                    print(" retry...", end="", flush=True)
                    time.sleep(20)
                else:
                    print(f" FAILED ({exc})")

        if data is None:
            continue

        hourly = data["hourly"]
        df = pd.DataFrame({
            "datetime":       pd.to_datetime(hourly["time"]),
            "temperature_2m": hourly["temperature_2m"],
            "precipitation":  hourly["precipitation"],
            "windspeed_10m":  hourly["windspeed_10m"],
            "weathercode":    hourly["weathercode"],
        })
        df["is_raining"]    = df["precipitation"] > 0
        df["is_cold"]       = df["temperature_2m"] < 5.0
        df["weather_label"] = df.apply(
            lambda r: ("rain" if r["is_raining"] else "clear")
                      + "_" + ("cold" if r["is_cold"] else "mild"),
            axis=1,
        )
        df["hour"] = df["datetime"].dt.hour
        df["date"] = df["datetime"].dt.date.astype(str)

        for month in range(1, 13):
            mdf = df[df["datetime"].dt.month == month]
            if mdf.empty:
                continue
            key = f"{R2_RAW_WEATHER}/year={year}/month={month:02d}/weather.parquet"
            upload_parquet(mdf, key)

        print(
            f" {len(df):,} hours | "
            f"rain {df['is_raining'].mean()*100:.0f}% | "
            f"cold {df['is_cold'].mean()*100:.0f}% ✓"
        )
        time.sleep(1)


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(stats: list):
    ok        = [s for s in stats if s["status"] == "ok"]
    skipped   = [s for s in stats if s["status"] == "skipped"]
    not_avail = [s for s in stats if s["status"] == "not_available"]
    failed    = [s for s in stats if s["status"] == "failed"]

    total_rows = sum(s["rows"] for s in ok)
    total_mb   = sum(s["mb"]   for s in ok) + sum(s["mb"] for s in skipped)

    print(f"\n{'='*60}")
    print("  NYC Yellow Taxi — Cloudflare R2 Data Lake Summary")
    print(f"{'='*60}")
    print(f"  Bucket           : {R2_BUCKET}")
    print(f"  Period           : {START_YEAR} → {END_YEAR}")
    print(f"  Downloaded       : {len(ok)} months")
    print(f"  Already on R2    : {len(skipped)} months (skipped)")
    print(f"  Not published yet: {len(not_avail)} months")
    print(f"  Failed           : {len(failed)} months")
    print(f"  Total rows       : {total_rows:,}")
    print(f"  Total size (raw) : {total_mb:.0f} MB  (~{total_mb/1024:.1f} GB)")
    print(f"  Raw zone         : r2://{R2_BUCKET}/raw/trips/year=YYYY/month=MM/")
    print(f"  Processed zone   : r2://{R2_BUCKET}/processed/trips_clean/year=YYYY/month=MM/")
    print(f"  Weather zone     : r2://{R2_BUCKET}/raw/weather/year=YYYY/month=MM/")
    if failed:
        print(f"\n  Re-run to retry:")
        for s in failed:
            print(f"    {s['year']}-{s['month']:02d}")
    print(f"{'='*60}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print("=" * 60)
    print("  NYC Yellow Taxi — Cloudflare R2 Data Lake Ingestion")
    print(f"  Period  : {START_YEAR} → today ({today})  (~4 GB max)")
    print(f"  Storage : Cloudflare R2  (bucket: {R2_BUCKET})")
    print(f"  Source  : TLC Yellow Taxi ONLY")
    print(f"  Mode    : FULL data, no sampling, idempotent")
    print("=" * 60)

    print("\n  Verifying R2 connectivity ...", end="", flush=True)
    try:
        r2_client().head_bucket(Bucket=R2_BUCKET)
        print(" OK\n")
    except ClientError as e:
        print(f" FAILED: {e}")
        print("  Check R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY in .env")
        sys.exit(1)

    stats = process_all_trips()
    download_weather()
    print_summary(stats)


if __name__ == "__main__":
    main()
