"""
DAG: dag_load_test_parallel
Description: Airflow 3.0.0 Load Test — Parallel Task Execution
Tests spinning up 10, 100, and 1000 tasks all at once (fan-out pattern).
Captures scheduling latency, parallel throughput, and total wall-clock time.

Airflow 3.0.0 compatible.
"""

from __future__ import annotations

import time
import logging
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
PARALLEL_SIZES = [10, 100, 1000]

default_args = {
    "owner": "load-test",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}


def _make_parallel_dag(task_count: int) -> DAG:
    """Factory: one DAG per parallel task-count."""
    dag_id = f"load_test_parallel_{task_count}_tasks"

    with DAG(
        dag_id=dag_id,
        description=f"Parallel load test with {task_count} tasks",
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args=default_args,
        # max_active_tasks controls how many tasks run concurrently in this DAG
        max_active_tasks=task_count if task_count <= 100 else 256,
        tags=["load-test", "parallel", f"n={task_count}"],
        params={
            "sleep_seconds": Param(0.0, type="number", description="Sleep per task (seconds)"),
        },
        doc_md=f"""
## Parallel Load Test — {task_count} tasks

Spawns **{task_count}** tasks in a pure fan-out pattern.

```
benchmark_start
    ├── parallel_task_0
    ├── parallel_task_1
    ├── …
    └── parallel_task_{task_count - 1}
            └── benchmark_end (aggregate)
```

### Metrics pushed to XCom by benchmark_end
| Key | Description |
|-----|-------------|
| `dag_start_ts` | When DAG started |
| `dag_end_ts` | When aggregation finished |
| `total_elapsed_sec` | Wall-clock from start → aggregate |
| `task_count` | Total parallel tasks |
| `avg_task_sec` | Average individual task duration |
| `min_task_sec` / `max_task_sec` | Fastest / slowest task |
| `scheduling_overhead_sec` | `total_elapsed - max_task_time` approx. scheduler overhead |

### Trigger
```bash
airflow dags trigger {dag_id}
```
        """,
    ) as dag:

        # ── Benchmark start ──────────────────────────────────────────────
        @task(task_id="benchmark_start")
        def benchmark_start(**context) -> dict:
            ts = time.time()
            log.info("=== PARALLEL LOAD TEST START | tasks=%d | ts=%.4f ===", task_count, ts)
            return {"dag_start_ts": ts, "task_count": task_count}

        # ── Individual parallel worker ───────────────────────────────────
        @task(task_id="parallel_task")
        def parallel_task(start_info: dict, task_index: int, **context) -> dict:
            sleep_sec: float = context["params"].get("sleep_seconds", 0.0)
            scheduled_at = time.time()
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            elapsed = time.time() - scheduled_at

            # Measure how long after DAG start this task actually began running
            lag = scheduled_at - start_info["dag_start_ts"]

            log.info(
                "[parallel] task_index=%d | elapsed=%.4fs | scheduling_lag=%.4fs",
                task_index, elapsed, lag,
            )
            return {
                "task_index": task_index,
                "elapsed_sec": round(elapsed, 6),
                "scheduling_lag_sec": round(lag, 4),
                "dag_start_ts": start_info["dag_start_ts"],
            }

        # ── Aggregate all results ────────────────────────────────────────
        @task(task_id="benchmark_end")
        def benchmark_end(results: list[dict], **context) -> dict:
            end_ts = time.time()
            start_ts = results[0]["dag_start_ts"]

            durations   = [r["elapsed_sec"] for r in results]
            lags        = [r["scheduling_lag_sec"] for r in results]
            total_elapsed = end_ts - start_ts

            summary = {
                "dag_start_ts": start_ts,
                "dag_end_ts": end_ts,
                "total_elapsed_sec": round(total_elapsed, 4),
                "task_count": task_count,
                "avg_task_sec": round(sum(durations) / len(durations), 6),
                "min_task_sec": round(min(durations), 6),
                "max_task_sec": round(max(durations), 6),
                "avg_scheduling_lag_sec": round(sum(lags) / len(lags), 4),
                "max_scheduling_lag_sec": round(max(lags), 4),
                "scheduling_overhead_sec": round(total_elapsed - max(durations), 4),
            }

            log.info("=" * 60)
            log.info("PARALLEL LOAD TEST SUMMARY")
            log.info("  Task count              : %d", task_count)
            log.info("  Total wall-clock        : %.4f s", total_elapsed)
            log.info("  Avg task duration       : %.6f s", summary["avg_task_sec"])
            log.info("  Min / Max task duration : %.6f s / %.6f s",
                     summary["min_task_sec"], summary["max_task_sec"])
            log.info("  Avg scheduling lag      : %.4f s", summary["avg_scheduling_lag_sec"])
            log.info("  Max scheduling lag      : %.4f s", summary["max_scheduling_lag_sec"])
            log.info("  Scheduling overhead ≈   : %.4f s", summary["scheduling_overhead_sec"])
            log.info("=" * 60)
            return summary

        # ── Wire: fan-out then aggregate ─────────────────────────────────
        start_info = benchmark_start()
        worker_results = [
            parallel_task.override(task_id=f"parallel_task_{i}")(
                start_info=start_info, task_index=i
            )
            for i in range(task_count)
        ]
        benchmark_end(results=worker_results)

    return dag


# Register all DAGs
for _n in PARALLEL_SIZES:
    globals()[f"parallel_load_test_{_n}"] = _make_parallel_dag(_n)
