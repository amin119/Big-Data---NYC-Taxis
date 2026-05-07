"""
NYC Taxi Pipeline — Spark Batch Job + MLlib  (Phase 5)

What this job does (in order):
  1. Reads Yellow Taxi trips + weather from Cloudflare R2, one month at a time.
  2. Joins them on rounded pickup hour (e.g. 14:37 → 14:00).
  3. Computes revenue aggregations per zone/month → writes to Cassandra + R2.
  4. Computes traffic density (zone × hour × weather) → writes to R2.
  5. Trains a trip duration prediction model with Spark MLlib → saved locally.

Architecture choice:
  - Data loading & aggregations : pandas + boto3  (avoids S3A/Hadoop JAR hell)
  - ML training                 : PySpark MLlib   (demonstrates Spark ML)
  - Cassandra writes            : cassandra-driver (simpler than Spark connector here)
  - R2 writes                   : boto3 upload_fileobj

Run:
  python spark/batch_job.py

Configure BATCH_YEAR in .env (default: 2024).
"""

import os
import gevent.monkey; gevent.monkey.patch_all()  # must be before all other imports for Python 3.12

import sys
import math
import shutil
import datetime

import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
R2_ENDPOINT          = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET            = os.getenv("R2_BUCKET",          "chabbah")
R2_PROCESSED         = os.getenv("R2_PROCESSED",       "processed/trips_clean")
R2_RAW_WEATHER       = os.getenv("R2_RAW_WEATHER",     "raw/weather")
R2_BATCH_RESULTS     = os.getenv("R2_BATCH_RESULTS",   "batch_results")

CASSANDRA_HOST       = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT       = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE   = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")

BATCH_YEAR           = int(os.getenv("BATCH_YEAR",     "2024"))
MODEL_DIR            = "models/trip_duration_model"
ML_SAMPLE_PER_MONTH  = 5_000   # rows sampled per month for ML training


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


def _load_r2_month(prefix: str, year: int, month: int) -> pd.DataFrame:
    """Download one month's Parquet file(s) from R2 into a pandas DataFrame."""
    key_prefix = f"{prefix}/year={year}/month={month:02d}/"
    client = _r2()
    paginator = client.get_paginator("list_objects_v2")
    dfs = []
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            buf = BytesIO()
            client.download_fileobj(R2_BUCKET, obj["Key"], buf)
            buf.seek(0)
            dfs.append(pd.read_parquet(buf))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _upload_parquet(df: pd.DataFrame, key: str):
    """Upload a pandas DataFrame as Snappy Parquet to R2."""
    buf = BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)
    _r2().upload_fileobj(buf, R2_BUCKET, key)


# ── Cassandra helpers ──────────────────────────────────────────────────────────

def _cassandra_session():
    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    return cluster, session


def _write_revenue_to_cassandra(df: pd.DataFrame):
    """Insert revenue rows into daily_stats using the cassandra-driver."""
    cluster, session = _cassandra_session()
    stmt = session.prepare("""
        INSERT INTO daily_stats (zone_id, date, trip_count, total_revenue, avg_tip)
        VALUES (?, ?, ?, ?, ?)
    """)
    for _, row in df.iterrows():
        session.execute(stmt, (
            int(row["zone_id"]),
            datetime.date(BATCH_YEAR, int(row["month"]), 1),
            int(row["trip_count"]),
            float(row["total_revenue"]),
            float(row["avg_tip"]),
        ))
    cluster.shutdown()


# ── Step 1 & 2 — Load and join trips + weather for one month ──────────────────

def _process_month(month_num: int):
    """
    Returns (revenue_df, density_df, ml_sample_df) for one month.
    All three are plain pandas DataFrames.
    """
    print(f"\n  [{BATCH_YEAR}-{month_num:02d}]", end="", flush=True)

    trips = _load_r2_month(R2_PROCESSED, BATCH_YEAR, month_num)
    if trips.empty:
        print(" no trips found — skipping")
        return None, None, None

    weather = _load_r2_month(R2_RAW_WEATHER, BATCH_YEAR, month_num)
    print(f" {len(trips):,} trips | {len(weather):,} weather rows", end="", flush=True)

    # ── Parse timestamps & add features ───────────────────────────────────────
    trips["tpep_pickup_datetime"]  = pd.to_datetime(trips["tpep_pickup_datetime"],  errors="coerce")
    trips["tpep_dropoff_datetime"] = pd.to_datetime(trips["tpep_dropoff_datetime"], errors="coerce")
    trips = trips.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    trips["trip_duration_min"] = (
        (trips["tpep_dropoff_datetime"] - trips["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    )
    trips["hour_of_day"]  = trips["tpep_pickup_datetime"].dt.hour
    trips["day_of_week"]  = trips["tpep_pickup_datetime"].dt.dayofweek  # 0=Mon … 6=Sun
    trips["month"]        = month_num
    trips["pickup_hour"]  = trips["tpep_pickup_datetime"].dt.floor("h")

    # ── Join with weather on rounded pickup hour ───────────────────────────────
    if not weather.empty:
        weather["pickup_hour"] = pd.to_datetime(weather["datetime"], errors="coerce").dt.floor("h")
        merged = trips.merge(
            weather[["pickup_hour", "is_raining", "is_cold", "weather_label", "temperature_2m"]],
            on="pickup_hour",
            how="left",
        )
    else:
        merged = trips.copy()
        merged[["is_raining", "is_cold"]] = False
        merged["weather_label"]   = "clear_mild"
        merged["temperature_2m"]  = 15.0

    merged["is_raining"] = merged["is_raining"].fillna(False).infer_objects(copy=False).astype(bool)
    merged["is_cold"]    = merged["is_cold"].fillna(False).infer_objects(copy=False).astype(bool)
    merged["weather_label"] = merged["weather_label"].fillna("clear_mild")

    # ── Step 3 — Revenue per zone/month ───────────────────────────────────────
    revenue = (
        merged.groupby(["PULocationID", "month"])
        .agg(
            trip_count    =("fare_amount",  "count"),
            total_revenue =("total_amount", "sum"),
            avg_fare      =("fare_amount",  "mean"),
            avg_tip       =("tip_amount",   "mean"),
        )
        .reset_index()
        .rename(columns={"PULocationID": "zone_id"})
    )

    # ── Step 4 — Traffic density per zone × hour × weather ────────────────────
    density = (
        merged.groupby(["PULocationID", "hour_of_day", "day_of_week", "is_raining"])
        .agg(trip_count=("fare_amount", "count"))
        .reset_index()
        .rename(columns={"PULocationID": "zone_id"})
    )

    # ── Step 5 — ML sample: keep clean rows only ───────────────────────────────
    ml = merged[
        (merged["trip_duration_min"] > 1) &
        (merged["trip_duration_min"] < 120) &
        (merged["trip_distance"]     > 0) &
        merged[["trip_duration_min", "trip_distance", "PULocationID",
                 "passenger_count"]].notna().all(axis=1)
    ].copy()
    ml_sample = ml.sample(min(ML_SAMPLE_PER_MONTH, len(ml)), random_state=42)

    print(f" → revenue {len(revenue)} zones | density {len(density)} combos | ML {len(ml_sample)} rows")
    return revenue, density, ml_sample


# ── Step 5 — Train trip duration model with PySpark MLlib ─────────────────────

def _train_model(ml_pd: pd.DataFrame):
    print(f"\n  Training MLlib LinearRegression on {len(ml_pd):,} rows ...")

    spark = (
        SparkSession.builder
        .appName("NYCTaxiBatchML")
        .master("local[*]")
        .config("spark.driver.memory",          "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ml_pd = ml_pd.copy()
    ml_pd["is_raining_int"]  = ml_pd["is_raining"].astype(int)
    ml_pd["passenger_count"] = ml_pd["passenger_count"].fillna(1).astype(float)

    spark_df = spark.createDataFrame(
        ml_pd[[
            "trip_distance", "hour_of_day", "day_of_week",
            "PULocationID", "passenger_count", "is_raining_int",
            "trip_duration_min",
        ]]
    )

    features = ["trip_distance", "hour_of_day", "day_of_week",
                "PULocationID",  "passenger_count", "is_raining_int"]

    assembler = VectorAssembler(inputCols=features, outputCol="features")
    assembled = assembler.transform(spark_df).select("features", "trip_duration_min")

    train, test = assembled.randomSplit([0.8, 0.2], seed=42)

    lr    = LinearRegression(featuresCol="features", labelCol="trip_duration_min")
    model = lr.fit(train)

    preds = model.transform(test)
    rmse  = RegressionEvaluator(labelCol="trip_duration_min", metricName="rmse").evaluate(preds)
    r2    = RegressionEvaluator(labelCol="trip_duration_min", metricName="r2").evaluate(preds)

    print(f"  Model — RMSE: {rmse:.2f} min | R²: {r2:.4f}")
    print(f"  Coefficients: distance×{model.coefficients[0]:.3f} | "
          f"hour×{model.coefficients[1]:.3f} | "
          f"rain×{model.coefficients[5]:.3f}")

    if os.path.exists(MODEL_DIR):
        shutil.rmtree(MODEL_DIR)
    os.makedirs(os.path.dirname(MODEL_DIR) or ".", exist_ok=True)
    model.save(MODEL_DIR)
    print(f"  Model saved → {MODEL_DIR}/")

    spark.stop()
    return rmse, r2


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  NYC Taxi — Spark Batch Job  (Phase 5)")
    print(f"  Year      : {BATCH_YEAR}")
    print(f"  Cassandra : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}")
    print(f"  R2 bucket : {R2_BUCKET}")
    print("=" * 60)
    print("\n  Processing months 01–12 ...\n")

    all_revenue = []
    all_density = []
    all_ml      = []

    for m in range(1, 13):
        rev, den, ml = _process_month(m)
        if rev is not None:
            all_revenue.append(rev)
            all_density.append(den)
            all_ml.append(ml)

    if not all_revenue:
        print("\n  No data found for this year. Check BATCH_YEAR in .env.")
        sys.exit(1)

    # ── Write revenue ──────────────────────────────────────────────────────────
    print("\n  Writing revenue → Cassandra + R2 ...", end="", flush=True)
    final_revenue = pd.concat(all_revenue, ignore_index=True)
    _write_revenue_to_cassandra(final_revenue)
    _upload_parquet(
        final_revenue,
        f"{R2_BATCH_RESULTS}/revenue/year={BATCH_YEAR}/revenue.parquet",
    )
    print(f" {len(final_revenue)} rows ✓")

    # ── Write density ──────────────────────────────────────────────────────────
    print("  Writing density → R2 ...", end="", flush=True)
    final_density = pd.concat(all_density, ignore_index=True)
    p33 = final_density["trip_count"].quantile(0.33)
    p66 = final_density["trip_count"].quantile(0.66)
    final_density["density_level"] = final_density["trip_count"].apply(
        lambda x: "Low" if x <= p33 else ("Medium" if x <= p66 else "High")
    )
    _upload_parquet(
        final_density,
        f"{R2_BATCH_RESULTS}/density/year={BATCH_YEAR}/density.parquet",
    )
    print(f" {len(final_density)} rows ✓")

    # ── Train model ────────────────────────────────────────────────────────────
    ml_pd = pd.concat(all_ml, ignore_index=True)
    rmse, r2 = _train_model(ml_pd)

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Batch job complete")
    print("=" * 60)
    total_trips = int(final_revenue["trip_count"].sum())
    total_rev   = final_revenue["total_revenue"].sum()
    print(f"  Year            : {BATCH_YEAR}")
    print(f"  Total trips     : {total_trips:,}")
    print(f"  Total revenue   : ${total_rev:,.0f}")
    print(f"  Zones analysed  : {final_revenue['zone_id'].nunique()}")
    print(f"  Density combos  : {len(final_density):,}")
    print(f"  ML rows trained : {len(ml_pd):,}")
    print(f"  Model RMSE      : {rmse:.2f} min")
    print(f"  Model R²        : {r2:.4f}")
    print(f"  Results in R2   : r2://{R2_BUCKET}/{R2_BATCH_RESULTS}/")
    print(f"  Model saved     : {MODEL_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
