"""
DAG: dag_load_test_serial
Description: Airflow 3.0.0 Load Test — Serial Task Execution
Tests spinning up 10, 100, and 1000 tasks one after another.
Captures task scheduling latency, execution time, and total DAG duration.

Airflow 3.0.0 compatible.
"""

from __future__ import annotations

import time
import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
SERIAL_SIZES = [10, 100, 1000]  # number of serial tasks per batch

default_args = {
    "owner": "load-test",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}


def _make_serial_dag(task_count: int) -> DAG:
    """Factory: one DAG per serial task-count."""
    dag_id = f"load_test_serial_{task_count}_tasks"

    with DAG(
        dag_id=dag_id,
        description=f"Serial load test with {task_count} tasks",
        schedule=None,                        # Trigger manually / via API
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["load-test", "serial", f"n={task_count}"],
        params={
            "sleep_seconds": Param(0.0, type="number", description="Sleep per task (seconds)"),
        },
        doc_md=f"""
## Serial Load Test — {task_count} tasks

Spawns **{task_count}** tasks chained serially (task_0 → task_1 → … → task_n).

### Metrics collected (in XCom)
| Key | Description |
|-----|-------------|
| `dag_start_ts` | Unix timestamp when DAG logic started |
| `dag_end_ts`   | Unix timestamp when final task finished |
| `total_elapsed_sec` | Wall-clock seconds for all tasks |
| `task_durations` | List of per-task durations |
| `avg_task_sec` | Average time per task |
| `task_count` | Number of tasks executed |

### How to trigger
```bash
airflow dags trigger {dag_id}
```
        """,
    ) as dag:

        # ── Benchmark start ──────────────────────────────────────────────
        @task(task_id="benchmark_start")
        def benchmark_start(**context):
            ts = time.time()
            log.info("=== SERIAL LOAD TEST START | tasks=%d | ts=%.4f ===", task_count, ts)
            return {"dag_start_ts": ts, "task_count": task_count, "durations": []}

        # ── Individual serial worker ─────────────────────────────────────
        @task(task_id="serial_task")          # task_id overridden via .override() below
        def serial_task(prev_state: dict, task_index: int, **context) -> dict:
            sleep_sec: float = context["params"].get("sleep_seconds", 0.0)
            t0 = time.time()
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            elapsed = time.time() - t0

            log.info("[serial] task_index=%d elapsed=%.4fs", task_index, elapsed)

            durations = prev_state.get("durations", [])
            durations.append(round(elapsed, 6))
            return {**prev_state, "durations": durations, "last_task_index": task_index}

        # ── Benchmark end ────────────────────────────────────────────────
        @task(task_id="benchmark_end")
        def benchmark_end(final_state: dict, **context):
            end_ts = time.time()
            start_ts = final_state["dag_start_ts"]
            durations = final_state.get("durations", [])
            total_elapsed = end_ts - start_ts
            avg = (sum(durations) / len(durations)) if durations else 0.0

            summary = {
                "dag_start_ts": start_ts,
                "dag_end_ts": end_ts,
                "total_elapsed_sec": round(total_elapsed, 4),
                "task_count": task_count,
                "avg_task_sec": round(avg, 6),
                "min_task_sec": round(min(durations), 6) if durations else 0,
                "max_task_sec": round(max(durations), 6) if durations else 0,
                "task_durations": durations,
            }

            log.info("=" * 60)
            log.info("SERIAL LOAD TEST SUMMARY")
            log.info("  Task count       : %d", task_count)
            log.info("  Total elapsed    : %.4f s", total_elapsed)
            log.info("  Avg per task     : %.6f s", avg)
            log.info("  Min task time    : %.6f s", summary["min_task_sec"])
            log.info("  Max task time    : %.6f s", summary["max_task_sec"])
            log.info("=" * 60)
            return summary

        # ── Wire tasks serially ──────────────────────────────────────────
        state = benchmark_start()
        for i in range(task_count):
            state = serial_task.override(task_id=f"serial_task_{i}")(
                prev_state=state, task_index=i
            )
        benchmark_end(final_state=state)

    return dag


# Register all DAGs into the global namespace so Airflow discovers them
for _n in SERIAL_SIZES:
    globals()[f"serial_load_test_{_n}"] = _make_serial_dag(_n)
