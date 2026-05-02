# NYC Yellow Taxi — End-to-End Big Data Pipeline

A production-grade streaming + batch pipeline built for a GL4 university course.  
Ingests NYC Yellow Taxi trips (2022 → today) and live weather data, processes them with Apache Spark, stores results in Cassandra, and visualises everything in Grafana.

---

## Current Progress

| Phase | Name | Status |
|-------|------|--------|
| 0 | Infrastructure — Docker Compose (6 services) | ✅ Done |
| 1 | Data Lake — TLC trips + weather → Cloudflare R2 | ✅ Done |
| 2 | Streaming Ingestion — Kafka producers | ✅ Done |
| 3 | Stream Processing — Spark Structured Streaming | ✅ Done |
| 4 | Real-time Dashboards — Grafana live | 🔜 Planned |
| 5 | Batch Processing — Airflow + Spark + MLlib | 🔜 Planned |
| 6 | Batch Dashboards — Grafana historical | 🔜 Planned |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│  TLC Yellow Taxi (Parquet, monthly)    Open-Meteo API (hourly)      │
└────────────────┬───────────────────────────────┬────────────────────┘
                 │                               │
                 ▼                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                    CLOUDFLARE R2  (Data Lake)                      │
│  raw/trips/year=YYYY/month=MM/trips.parquet                        │
│  raw/weather/year=YYYY/month=MM/weather.parquet                    │
│  processed/trips_clean/year=YYYY/month=MM/trips.parquet            │
│  batch_results/   models/   checkpoints/                           │
└────────┬───────────────────────────────────────────────────────────┘
         │
         ├─────── STREAMING PATH ──────────────────────────────────────
         │        Kafka producer replays R2 data at 20 trips/s
         │        Topics: taxi-trips  |  taxi-weather
         │                  │
         │                  ▼
         │        Spark Structured Streaming
         │        (5-min windows, weather join, demand forecasting)
         │                  │
         │                  ▼
         │        Cassandra  ←──────── Grafana (live dashboard)
         │
         └─────── BATCH PATH ─────────────────────────────────────────
                  Airflow (daily DAG, 02:00 UTC)
                  Spark batch job (aggregations + ML)
                            │
                            ▼
                  Cassandra  ←──────── Grafana (batch dashboard)
```

---

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Kafka (Confluent) | 7.5.0 | Streaming message bus |
| Apache Spark + PySpark | 3.5.0 | Stream & batch processing, ML |
| Apache Cassandra | 4.1 | Low-latency results store |
| Apache Airflow | 2.8.0 | Daily batch orchestration |
| Grafana | 10.2.0 | Real-time & historical dashboards |
| Cloudflare R2 | — | S3-compatible cloud Data Lake |
| Open-Meteo API | — | Historical + live NYC weather |
| Docker Compose | — | Local service orchestration |

---

## Data

### NYC Yellow Taxi Trips
- **Source**: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) via CloudFront CDN
- **Scope**: Yellow Taxi **only** (no green, no FHV), **2022 → today**
- **Volume**: ~4 GB total (fits within R2's free 10 GB tier)
- **Format**: Hive-partitioned Parquet on Cloudflare R2
- **Schema** (11 columns):

| Column | Type | Description |
|--------|------|-------------|
| `VendorID` | int | Taxi vendor |
| `tpep_pickup_datetime` | timestamp | Pickup time |
| `tpep_dropoff_datetime` | timestamp | Drop-off time |
| `passenger_count` | float | Passenger count |
| `trip_distance` | float | Distance (miles) |
| `PULocationID` | int | Pickup zone |
| `DOLocationID` | int | Drop-off zone |
| `fare_amount` | float | Base fare ($) |
| `tip_amount` | float | Tip ($) |
| `total_amount` | float | Total charged ($) |
| `payment_type` | int | Payment method |

### NYC Weather (Open-Meteo)
- **Historical**: `archive-api.open-meteo.com/v1/archive` — hourly data 2022 → today
- **Live**: `api.open-meteo.com/v1/forecast` — hourly forecast for streaming enrichment
- **No API key required**
- **Columns**: `datetime`, `temperature_2m`, `precipitation`, `windspeed_10m`, `weathercode`, `is_raining`, `is_cold`, `weather_label`, `hour`, `date`
- **Weather labels**: `rain_cold`, `rain_mild`, `clear_cold`, `clear_mild`

### Demand Multiplier Rule (streaming prediction)
| Condition | Multiplier |
|-----------|-----------|
| Rain + Cold (≤ 5 °C) | × 1.4 |
| Rain only | × 1.3 |
| Cold only | × 1.1 |
| Clear + Mild | × 1.0 |

---

## Data Lake Layout (Cloudflare R2)

```
chabbah/                              ← R2 bucket
├── raw/
│   ├── trips/
│   │   └── year=2022/month=01/trips.parquet
│   │   └── year=2022/month=02/trips.parquet
│   │   └── ...  (through today)
│   └── weather/
│       └── year=2022/month=01/weather.parquet
│       └── ...
├── processed/
│   └── trips_clean/
│       └── year=2022/month=01/trips.parquet
│       └── ...
├── batch_results/                    ← Airflow Spark job outputs
├── models/                           ← MLlib saved models
└── checkpoints/                      ← Spark Streaming checkpoints
```

---

## Docker Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 9092 | Message broker |
| `cassandra` | cassandra:4.1 | 9042 | Results database |
| `grafana` | grafana/grafana:10.2.0 | **3001** | Dashboards |
| `postgres` | postgres:15 | internal | Airflow metadata DB |
| `airflow` | apache/airflow:2.8.0 | **8888** | DAG orchestration |

All services share the `taxi-net` bridge network.

> **Note on ports**: Grafana runs on **3001** (not 3000) and Airflow on **8888** (not 8080) because those default ports were already in use on the host machine.

---

## Project Structure

```
Projet Big data/
├── docker-compose.yml              ← 6-service stack
├── .env                            ← R2 credentials + config
├── .env.example                    ← Template — copy to .env and fill in secrets
├── requirements.txt                ← Python dependencies
├── scripts/
│   ├── prepare_data.py             ← ✅ Phase 1: R2 data lake ingestion
│   ├── producer.py                 ← ✅ Phase 2: Kafka trips + weather producers
│   └── consumer_verify.py          ← ✅ Phase 2: Consumer smoke test
├── spark/
│   ├── streaming_job.py            ← ✅ Phase 3: Spark Structured Streaming
│   └── batch_job.py                ← 🔜 Phase 5: Spark batch + MLlib
├── cassandra/
│   └── init.cql                    ← ✅ Phase 3: Keyspace + 3 tables
├── airflow/
│   └── dags/
│       └── taxi_batch_dag.py       ← 🔜 Phase 5: Airflow DAG
└── README.md
```

---

## Setup Guide

### Prerequisites
- Docker Desktop (≥ 4.x) with at least **4 GB RAM** allocated
- Python 3.10+
- Java 11+ (required by PySpark — check with `java -version`)
- A Cloudflare R2 account (free tier covers the full ~4 GB dataset)

### 1 — Configure Environment

Copy the example file and fill in your Cloudflare R2 credentials:

```bash
cp .env.example .env
```

Then open `.env` and replace the `<placeholder>` values with your real R2 credentials. The only values you must change are:

```dotenv
R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=<your-access-key>
R2_SECRET_ACCESS_KEY=<your-secret-key>
R2_BUCKET=<your-bucket-name>
```

All other values in `.env.example` are safe defaults that work as-is. Never commit `.env` — it is git-ignored.

### 2 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3 — Start Docker Services

```bash
docker compose up -d
```

Wait ~60 seconds for Cassandra to become healthy:

```bash
docker compose ps          # all should show "running" or "healthy"
docker logs cassandra      # look for "Starting listening for CQL clients"
```

### 4 — Phase 1: Populate the Data Lake

Downloads Yellow Taxi Parquet files from TLC CDN and uploads them directly to Cloudflare R2.  
Already-uploaded months are detected and skipped — the script is fully idempotent.

```bash
python scripts/prepare_data.py
```

Expected output per month:
```
[2022-01] Downloading (attempt 1) ... 2,463,931 rows → 142,891 rows kept | 8 MB → r2://chabbah/raw/trips/year=2022/month=01/trips.parquet
```

Total time: ~20–40 minutes for 2022 → today on a standard connection.

### 5 — Phase 2: Start Kafka Producers

```bash
# Terminal 1 — keep running; producer loops through all 51 R2 month files continuously
python scripts/producer.py

# Terminal 2 — smoke test: prints 30 messages then exits with a summary
python scripts/consumer_verify.py
```

### 6 — Phase 3: Launch Spark Structured Streaming

Requires the producer (Terminal 1) to already be running. First run downloads ~200 MB of Spark JARs into `~/.ivy2` — subsequent runs are instant.

```powershell
# Terminal 3 — keep running alongside the producer
python spark/streaming_job.py
```

The job auto-creates the Cassandra keyspace and tables on first run. After ~35 seconds you should see:
```
[weather epoch 1] clear_mild | 10.8°C | rain=False cold=False
[trips  epoch 2] 23 zones | weather: clear_mild ×1.0 | window: 2026-05-02 ...
```

**Verify the streaming is working** (Terminal 4, after at least one batch printed):

```powershell
# Check trip window stats were written to Cassandra
docker exec cassandra cqlsh -e "SELECT zone_id, window_start, trip_count, predicted_demand, weather_label FROM taxi_streaming.trip_stats_by_window LIMIT 10;"

# Check live weather snapshots
docker exec cassandra cqlsh -e "SELECT recorded_at, weather_label, temperature_c, is_raining FROM taxi_streaming.weather_snapshots WHERE source='open-meteo' LIMIT 5;"

# Run twice 30 s apart — count must increase each time
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM taxi_streaming.trip_stats_by_window;"
```

### 7 — Phase 4: Open Grafana Dashboards *(planned)*

- URL: `http://localhost:3001`
- Login: `admin` / `admin`
- **Live Dashboard**: rolling 5-minute demand and weather metrics
- **Batch Dashboard**: historical trends, zone heatmaps, model accuracy

### 8 — Phase 5: Trigger Batch Pipeline *(planned)*

> `airflow/dags/taxi_batch_dag.py` and `spark/batch_job.py` are not yet implemented.

```bash
# Airflow UI: http://localhost:8888
# Login: admin / admin
# Enable the "taxi_batch_daily" DAG, then trigger manually
```

---

## Pipeline Phases

| Phase | Name | Description |
|-------|------|-------------|
| 0 | Infrastructure | Docker Compose — 6 services up and healthy |
| 1 | Data Lake | `prepare_data.py` — TLC Yellow Taxi + weather → Cloudflare R2 |
| 2 | Streaming Ingestion | Kafka producers replay R2 trip data + live weather |
| 3 | Stream Processing | Spark Structured Streaming — 5-min windows, weather join |
| 4 | Real-time Dashboards | Grafana live dashboard — demand, fares, weather |
| 5 | Batch Processing | Airflow + Spark — daily aggregations + MLlib demand forecast |
| 6 | Batch Dashboards | Grafana batch dashboard — trends, zones, model KPIs |

---

## Phase 3 — How the Streaming Works

### The Big Picture

Once the Kafka producer is running, `streaming_job.py` opens **two parallel Spark streams** that read from Kafka continuously. Every 30 seconds, Spark wakes up, processes everything that arrived since the last wake-up, and writes results to Cassandra. Grafana reads from Cassandra to update the live dashboard.

```
Kafka: taxi-trips  ──► Stream A (trips)   ──► trip_stats_by_window  ──► Grafana
Kafka: taxi-weather ──► Stream B (weather) ──► weather_snapshots     ──► Grafana
                              │
                        shared weather state
                              │
                    demand prediction applied
                    inside Stream A writes
```

### Stream A — Trips (trigger every 30 s)

1. **Read**: Spark reads every JSON message that arrived on the `taxi-trips` topic in the last 30 seconds. Each message is one taxi trip with fields like `PULocationID`, `fare_amount`, `trip_distance`, `ingested_at`, and pre-computed flags like `is_fare_anomaly`.

2. **Parse + timestamp**: The JSON string is parsed against a fixed schema. `ingested_at` is cast to a proper timestamp — this becomes the **event time** used for windowing.

3. **5-minute tumbling window**: Trips are grouped by `(pickup zone, 5-minute window)`. For each group Spark computes:
   - `trip_count` — how many trips departed this zone in this 5-min slot
   - `avg_fare` — average base fare
   - `avg_distance` — average trip distance
   - `anomaly_count` — trips with a fare outside \$0–200 or distance ≤ 0 or > 100 mi

4. **10-minute watermark**: Spark waits up to 10 minutes for late-arriving messages before finalising a window. This handles network jitter without holding state forever.

5. **Weather multiplier** (applied inside `foreachBatch`): At write time, Spark reads the latest weather from the shared in-memory state and computes:

   | Condition | Multiplier |
   |-----------|-----------|
   | Rain + Cold (≤ 5 °C) | × 1.4 |
   | Rain only | × 1.3 |
   | Cold only | × 1.1 |
   | Clear + Mild | × 1.0 |

   `predicted_demand = trip_count × multiplier` gives an estimate of how many taxis each zone would need in the *next* 5-minute window under current weather.

6. **Write to Cassandra**: Each row written to `trip_stats_by_window` has: `zone_id`, `window_start`, `trip_count`, `avg_fare`, `avg_distance`, `anomaly_count`, `weather_label`, `multiplier`, `predicted_demand`.

### Stream B — Weather (trigger every 10 s)

1. **Read**: Spark reads messages from `taxi-weather`. The producer sends one weather snapshot every 500 trip rows (~25 seconds), so this stream is sparse but timely.

2. **Update shared state**: The latest row from each micro-batch updates a thread-safe Python dict (`_weather`). Stream A reads this dict inside its `foreachBatch`, so the demand multiplier always reflects the most recent weather without any stream-stream join complexity.

3. **Write to Cassandra**: Each weather message is also persisted to `weather_snapshots` (partitioned by `source = 'open-meteo'`, clustered by `recorded_at DESC`). Grafana queries `SELECT * FROM weather_snapshots WHERE source = 'open-meteo' LIMIT 1` to display current conditions.

### Why Not a Stream-Stream Join?

Spark supports joining two streams, but it requires matching events by a key within a time range. Weather messages arrive very infrequently (1 per 500 trips) — a stream-stream join would either miss most weather messages or hold a huge amount of state waiting for a match. The shared in-memory dict is simpler, correct for this use case, and uses zero extra memory beyond one weather object.

### Why Rule-Based Demand Prediction (Not ML)?

Running a Spark MLlib model inside a 30-second micro-batch on a laptop would trigger Out-Of-Memory errors because the model must be loaded and applied on the driver within the batch window. The rule-based multiplier is backed by real NYC TLC studies (rain increases taxi demand 20–40%) and runs in microseconds. The ML model is trained in the batch path (Phase 5) where memory is not a constraint.

---

## Cassandra Schema

Full schema in [cassandra/init.cql](cassandra/init.cql). Applied automatically by `streaming_job.py` on first run, or manually:

```bash
docker exec -i cassandra cqlsh < cassandra/init.cql
```

| Table | Written by | Purpose |
|-------|-----------|---------|
| `trip_stats_by_window` | Spark Streaming (30 s) | 5-min window counts, fares, predicted demand |
| `weather_snapshots` | Spark Streaming (10 s) | Current NYC conditions for Grafana |
| `daily_stats` | Spark Batch (Phase 5) | Daily aggregates per zone |

```cql
-- Streaming: 5-min window per zone, newest first
CREATE TABLE trip_stats_by_window (
    zone_id int, window_start timestamp,
    trip_count bigint, avg_fare double, avg_distance double,
    anomaly_count bigint, weather_label text,
    multiplier double, predicted_demand int,
    PRIMARY KEY ((zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Weather: latest snapshot queryable with LIMIT 1
CREATE TABLE weather_snapshots (
    source text, recorded_at timestamp,
    temperature_c double, precipitation double, windspeed double,
    weathercode int, is_raining boolean, is_cold boolean, weather_label text,
    PRIMARY KEY (source, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

-- Batch: daily aggregates per zone (Phase 5)
CREATE TABLE daily_stats (
    zone_id int, date date,
    trip_count bigint, total_revenue double, avg_tip double,
    PRIMARY KEY ((zone_id), date)
) WITH CLUSTERING ORDER BY (date DESC);
```

---

## Troubleshooting

### Cassandra not healthy after `docker compose up`
Cassandra takes 60–90 s to initialise. Wait and re-check:
```bash
docker compose ps cassandra
docker logs cassandra --tail 50
```

### Grafana unreachable
The project uses port **3001** (not 3000):
```
http://localhost:3001
```

### Airflow unreachable
The project uses port **8888** (not 8080):
```
http://localhost:8888
```

### R2 upload fails — `InvalidAccessKeyId`
Verify your `.env` credentials match the R2 API token exactly. Quick connectivity test:

```bash
python - <<'EOF'
import boto3, os
from dotenv import load_dotenv
load_dotenv()
s3 = boto3.client("s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    region_name="auto")
print(s3.head_bucket(Bucket=os.getenv("R2_BUCKET")))
EOF
```

### TLC month returns 404
TLC publishes data with a ~2-month lag. `prepare_data.py` handles this automatically — a 404 is logged as `not_available` and the script continues without error.

### Kafka `NoBrokersAvailable`
```bash
docker compose ps kafka
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Spark OOM during streaming or batch
Add these configs to the SparkSession builder:
```python
.config("spark.sql.shuffle.partitions", "8")
.config("spark.memory.fraction", "0.6")
```
Then delete the checkpoint directory and restart:
```bash
# Windows PowerShell
Remove-Item -Recurse -Force checkpoints/
```

---

## What Success Looks Like

| Milestone | Verification | Status |
|-----------|-------------|--------|
| Phase 0 | `docker compose ps` — all 6 services running/healthy | ✅ |
| Phase 1 | R2 bucket has `raw/trips/year=2022/` through current month | ✅ |
| Phase 2 | `consumer_verify.py` prints trip records continuously | ✅ |
| Phase 3 | Cassandra `trip_stats_by_window` accumulates rows every 30 s | ✅ |
| Phase 4 | Grafana live dashboard at `localhost:3001` shows rolling demand chart | 🔜 |
| Phase 5 | Airflow DAG `taxi_batch_daily` completes all tasks green | 🔜 |
| Phase 6 | Grafana batch dashboard shows historical zone heatmap | 🔜 |

---

## Key Design Decisions

**Why Cloudflare R2?**  
No local disk space for ~4 GB of raw data. R2 is S3-compatible (boto3 works unchanged), has a 10 GB free tier, and zero egress fees — ideal for a student project.

**Why Yellow Taxi only, 2022 → today?**  
Green taxi and FHV have different schemas. Limiting to Yellow Taxi keeps the schema uniform. Starting from 2022 gives 3+ years of rich data while staying under 4 GB (~R2 free tier).

**Why Open-Meteo for weather?**  
Free, no API key, high quality historical archive back to 1940, and a forecast API for live streaming enrichment — no budget required.

**Why rule-based weather multiplier in streaming?**  
Running MLlib inference inside a Spark micro-batch on a laptop causes OOM errors. The rule-based multiplier (rain/cold percentages) is backed by TLC studies and lightweight enough to run in every 30-second micro-batch. The ML model lives in the batch path where memory is not a constraint.

**Why a single Grafana instance for both dashboards?**  
Keeps the stack at 6 containers. Both dashboards read from Cassandra — Grafana is the only visualisation tool needed.

---

## Notes

- Copy `.env.example` → `.env` and fill in your R2 credentials before running anything
- `.env` is git-ignored — never commit it; `.env.example` (no real values) is committed instead
- All R2 paths use Hive-style partitioning (`year=YYYY/month=MM/`) for native Spark compatibility
- `prepare_data.py` is fully idempotent: re-running it safely skips already-uploaded months
- Kafka heap is capped at 256 MB, Cassandra at 512 MB to stay within typical laptop RAM limits
- Open-Meteo requires no API key and has a generous rate limit (10 000 calls/day) — well within project needs
- TLC data is published with a ~2-month lag; `prepare_data.py` handles 404s gracefully without failing
