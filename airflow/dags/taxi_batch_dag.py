"""
NYC Taxi Pipeline — Airflow DAG  (Phase 5)

Runs the Spark batch job daily at 02:00 UTC.
Four tasks in sequence:
  1. verify_r2       — confirm R2 bucket is reachable
  2. verify_cassandra — confirm Cassandra keyspace exists
  3. run_batch_job   — run spark/batch_job.py
  4. log_done        — print a completion summary

The batch job script lives at /opt/airflow/spark/batch_job.py
(mounted from ./spark via docker-compose volumes).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner":          "airflow",
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="taxi_batch_daily",
    description="NYC Taxi daily batch: revenue + density + MLlib model",
    schedule_interval="0 2 * * *",   # every day at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi", "batch", "spark", "mllib"],
) as dag:

    # ── Task 1 — Verify R2 connectivity ───────────────────────────────────────
    def _verify_r2(**ctx):
        import boto3, os
        endpoint  = os.getenv("R2_ENDPOINT")
        key_id    = os.getenv("R2_ACCESS_KEY_ID")
        secret    = os.getenv("R2_SECRET_ACCESS_KEY")
        bucket    = os.getenv("R2_BUCKET", "chabbah")
        batch_year = os.getenv("BATCH_YEAR", "2024")

        if not all([endpoint, key_id, secret]):
            raise ValueError("R2 credentials missing — check .env / Airflow env vars")

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            region_name="auto",
        )
        s3.head_bucket(Bucket=bucket)

        # Count how many processed trip files exist for this year
        paginator = s3.get_paginator("list_objects_v2")
        count = sum(
            1
            for page in paginator.paginate(
                Bucket=bucket,
                Prefix=f"processed/trips_clean/year={batch_year}/"
            )
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        )
        if count == 0:
            raise FileNotFoundError(
                f"No processed trip files found for year {batch_year}. "
                "Run scripts/prepare_data.py first."
            )
        print(f"R2 OK — {count} trip files found for {batch_year}")

    verify_r2 = PythonOperator(
        task_id="verify_r2",
        python_callable=_verify_r2,
    )

    # ── Task 2 — Verify Cassandra ─────────────────────────────────────────────
    def _verify_cassandra(**ctx):
        from cassandra.cluster import Cluster
        import os
        host    = os.getenv("CASSANDRA_HOST", "cassandra")   # Docker service name
        port    = int(os.getenv("CASSANDRA_PORT", "9042"))
        keyspace = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")

        cluster = Cluster([host], port=port)
        session = cluster.connect()
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspaces = [r.keyspace_name for r in rows]
        cluster.shutdown()

        if keyspace not in keyspaces:
            raise RuntimeError(
                f"Cassandra keyspace '{keyspace}' not found. "
                "Run: docker exec -i cassandra cqlsh < cassandra/init.cql"
            )
        print(f"Cassandra OK — keyspace '{keyspace}' exists")

    verify_cassandra = PythonOperator(
        task_id="verify_cassandra",
        python_callable=_verify_cassandra,
    )

    # ── Task 3 — Run the batch job ────────────────────────────────────────────
    run_batch_job = BashOperator(
        task_id="run_batch_job",
        bash_command="cd /opt/airflow && python spark/batch_job.py",
        env={
            # Pass all needed env vars explicitly so the script sees them
            "R2_ENDPOINT":          "{{ var.value.get('R2_ENDPOINT',          env_var('R2_ENDPOINT', '')) }}",
            "R2_ACCESS_KEY_ID":     "{{ var.value.get('R2_ACCESS_KEY_ID',     env_var('R2_ACCESS_KEY_ID', '')) }}",
            "R2_SECRET_ACCESS_KEY": "{{ var.value.get('R2_SECRET_ACCESS_KEY', env_var('R2_SECRET_ACCESS_KEY', '')) }}",
            "R2_BUCKET":            "{{ var.value.get('R2_BUCKET',            env_var('R2_BUCKET', 'chabbah')) }}",
            "CASSANDRA_HOST":       "cassandra",   # Docker service name (not localhost)
            "CASSANDRA_PORT":       "9042",
            "CASSANDRA_KEYSPACE":   "taxi_streaming",
            "BATCH_YEAR":           "{{ var.value.get('BATCH_YEAR', env_var('BATCH_YEAR', '2024')) }}",
        },
    )

    # ── Task 4 — Log completion ───────────────────────────────────────────────
    def _log_done(**ctx):
        run_id    = ctx["run_id"]
        exec_date = ctx["execution_date"]
        batch_year = os.getenv("BATCH_YEAR", "2024")
        print("=" * 50)
        print(f"  Batch job completed successfully")
        print(f"  Run ID     : {run_id}")
        print(f"  Exec date  : {exec_date}")
        print(f"  Year       : {batch_year}")
        print(f"  Results    : r2://chabbah/batch_results/")
        print(f"  Cassandra  : daily_stats table updated")
        print(f"  Grafana    : http://localhost:3001 — batch dashboard ready")
        print("=" * 50)

    log_done = PythonOperator(
        task_id="log_done",
        python_callable=_log_done,
    )

    # ── DAG flow ──────────────────────────────────────────────────────────────
    verify_r2 >> verify_cassandra >> run_batch_job >> log_done
