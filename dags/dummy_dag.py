from __future__ import annotations

import random
import time
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="dummy_process_demo_1",
    description="A simple Airflow 3 dummy process DAG",
    start_date=datetime(2026, 2, 1),
    schedule=None,   # trigger manually
    catchup=False,
    tags=["demo", "dummy", "airflow3"],
) as dag:

    @task
    def start():
        print("Starting dummy workflow...")
        return {
            "job_name": "dummy_etl_job",
            "run_ts": datetime.utcnow().isoformat(),
        }

    @task
    def extract(meta: dict):
        print(f"Extract step started for {meta['job_name']}")
        time.sleep(2)  # simulate work
        rows = random.randint(50, 150)
        print(f"Extracted {rows} rows")
        return {"rows": rows, "meta": meta}

    @task
    def process(data: dict):
        print("Processing data...")
        time.sleep(3)  # simulate processing
        processed_rows = data["rows"] - random.randint(0, 10)
        print(f"Processed rows: {processed_rows}")
        return {"processed_rows": processed_rows, "meta": data["meta"]}

    @task
    def validate(result: dict):
        print("Validating output...")
        time.sleep(1)
        if result["processed_rows"] <= 0:
            raise ValueError("No rows left after processing")
        print("Validation successful")
        return result

    @task
    def finish(result: dict):
        print("Dummy process completed successfully")
        print(f"Final output: {result}")

    s = start()
    e = extract(s)
    p = process(e)
    v = validate(p)
    finish(v)
