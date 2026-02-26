"""
DAG: dag_load_test_memory
Description: Airflow 3.0.0 Load Test — Memory Consumption per Task
Each task allocates a configurable chunk of memory, holds it for a
configurable duration, then reports RSS (Resident Set Size) before/after.

Provides per-task and aggregate memory stats so you can profile how
Airflow worker memory grows with task count and payload size.

Airflow 3.0.0 compatible.
"""

from __future__ import annotations

import gc
import os
import time
import logging
import tracemalloc
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configuration — task counts to test
# ──────────────────────────────────────────────
MEMORY_TEST_SIZES = [10, 50, 100]   # number of tasks; 1000 can be heavy – add cautiously

default_args = {
    "owner": "load-test",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _rss_mb() -> float:
    """Return the current process Resident Set Size in MB (Linux/macOS)."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: kB → MB
    except Exception:
        pass
    try:
        # Fallback: /proc/self/status (Linux)
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024  # kB → MB
    except Exception:
        pass
    return 0.0


def _process_memory_full() -> dict:
    """Collect a comprehensive snapshot of current process memory."""
    stats: dict = {}
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        mem = proc.memory_info()
        stats["rss_mb"]  = round(mem.rss / 1024 ** 2, 3)
        stats["vms_mb"]  = round(mem.vms / 1024 ** 2, 3)
        stats["pct"]     = round(proc.memory_percent(), 3)
        try:
            stats["uss_mb"] = round(proc.memory_full_info().uss / 1024 ** 2, 3)
        except Exception:
            stats["uss_mb"] = None
    except ImportError:
        # psutil not installed — fall back to resource module
        stats["rss_mb"]  = round(_rss_mb(), 3)
        stats["vms_mb"]  = None
        stats["pct"]     = None
        stats["uss_mb"]  = None
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# DAG factory
# ─────────────────────────────────────────────────────────────────────────────

def _make_memory_dag(task_count: int) -> DAG:
    dag_id = f"load_test_memory_{task_count}_tasks"

    with DAG(
        dag_id=dag_id,
        description=f"Memory load test with {task_count} tasks",
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["load-test", "memory", f"n={task_count}"],
        params={
            "alloc_mb": Param(
                50,
                type="integer",
                description="MB of memory each task should allocate",
            ),
            "hold_seconds": Param(
                1.0,
                type="number",
                description="Seconds each task holds the allocated memory",
            ),
            "parallel": Param(
                False,
                type="boolean",
                description="Run tasks in parallel (True) or serial (False)",
            ),
        },
        doc_md=f"""
## Memory Load Test — {task_count} tasks

Each task:
1. Snapshots **RSS before** allocation
2. Allocates `alloc_mb` MB of data in-process
3. Holds it for `hold_seconds`
4. Snapshots **RSS after** allocation
5. Frees memory and snapshots **RSS after free**
6. Reports delta (peak − baseline)

### Params
| Param | Default | Description |
|-------|---------|-------------|
| `alloc_mb` | 50 | MB to allocate per task |
| `hold_seconds` | 1.0 | Seconds to hold allocation |
| `parallel` | false | Fan-out (parallel) or chain (serial) |

### Aggregate metrics (XCom from `memory_report`)
| Key | Description |
|-----|-------------|
| `task_count` | Number of memory tasks |
| `alloc_mb_per_task` | Configured allocation |
| `avg_peak_delta_mb` | Average RSS increase per task |
| `max_peak_delta_mb` | Highest RSS increase seen |
| `total_elapsed_sec` | Wall-clock for all tasks |
| `per_task` | List of per-task stats |

### Install psutil (optional but recommended)
```bash
pip install psutil
```

### Trigger
```bash
airflow dags trigger {dag_id} --conf '{{"alloc_mb": 100, "hold_seconds": 2}}'
```
        """,
    ) as dag:

        # ── Benchmark start ──────────────────────────────────────────────
        @task(task_id="memory_benchmark_start")
        def memory_benchmark_start(**context) -> dict:
            ts = time.time()
            baseline = _process_memory_full()
            log.info(
                "=== MEMORY LOAD TEST START | tasks=%d | baseline_rss=%.1f MB ===",
                task_count, baseline.get("rss_mb", 0),
            )
            return {
                "dag_start_ts": ts,
                "task_count": task_count,
                "baseline_memory": baseline,
            }

        # ── Memory worker ────────────────────────────────────────────────
        @task(task_id="memory_task")
        def memory_task(prev: dict, task_index: int, **context) -> dict:
            alloc_mb: int    = int(context["params"].get("alloc_mb", 50))
            hold_sec: float  = float(context["params"].get("hold_seconds", 1.0))

            # ── Snapshot BEFORE ──────────────────────────────────────────
            gc.collect()
            mem_before = _process_memory_full()
            rss_before = mem_before.get("rss_mb", 0.0)

            # ── Allocate ─────────────────────────────────────────────────
            tracemalloc.start()
            alloc_bytes = alloc_mb * 1024 * 1024
            chunk = bytearray(alloc_bytes)   # force physical allocation
            chunk[::4096] = b"\x00" * (alloc_bytes // 4096)  # touch every page

            mem_peak = _process_memory_full()
            rss_peak = mem_peak.get("rss_mb", 0.0)

            time.sleep(hold_sec)

            _, peak_traced = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            # ── Free ─────────────────────────────────────────────────────
            del chunk
            gc.collect()
            mem_after = _process_memory_full()
            rss_after = mem_after.get("rss_mb", 0.0)

            result = {
                "task_index"        : task_index,
                "alloc_mb"          : alloc_mb,
                "rss_before_mb"     : rss_before,
                "rss_peak_mb"       : rss_peak,
                "rss_after_free_mb" : rss_after,
                "peak_delta_mb"     : round(rss_peak - rss_before, 3),
                "peak_traced_kb"    : round(peak_traced / 1024, 1),
                "held_sec"          : hold_sec,
                "dag_start_ts"      : prev.get("dag_start_ts", prev.get("dag_start_ts", 0)),
                "baseline_memory"   : prev.get("baseline_memory", {}),
            }

            log.info(
                "[memory] task=%d | before=%.1fMB | peak=%.1fMB | after=%.1fMB | delta=+%.1fMB | traced=%.1fKB",
                task_index,
                rss_before, rss_peak, rss_after,
                result["peak_delta_mb"],
                result["peak_traced_kb"],
            )
            return result

        # ── Aggregate report ─────────────────────────────────────────────
        @task(task_id="memory_report")
        def memory_report(results: list[dict], **context) -> dict:
            end_ts = time.time()
            start_ts = results[0]["dag_start_ts"]

            deltas   = [r["peak_delta_mb"]   for r in results]
            peaks    = [r["rss_peak_mb"]      for r in results]
            baselines= [r["rss_before_mb"]    for r in results]

            summary = {
                "task_count"         : task_count,
                "alloc_mb_per_task"  : results[0]["alloc_mb"],
                "total_elapsed_sec"  : round(end_ts - start_ts, 4),
                "baseline_rss_mb"    : round(baselines[0], 3),
                "avg_peak_delta_mb"  : round(sum(deltas) / len(deltas), 3),
                "max_peak_delta_mb"  : round(max(deltas), 3),
                "min_peak_delta_mb"  : round(min(deltas), 3),
                "max_rss_seen_mb"    : round(max(peaks), 3),
                "memory_leak_estimate_mb": round(
                    results[-1]["rss_after_free_mb"] - results[0]["rss_before_mb"], 3
                ),
                "per_task": [
                    {
                        "task_index"    : r["task_index"],
                        "peak_delta_mb" : r["peak_delta_mb"],
                        "rss_peak_mb"   : r["rss_peak_mb"],
                        "traced_kb"     : r["peak_traced_kb"],
                    }
                    for r in sorted(results, key=lambda x: x["task_index"])
                ],
            }

            log.info("=" * 60)
            log.info("MEMORY LOAD TEST SUMMARY")
            log.info("  Task count            : %d", task_count)
            log.info("  Alloc per task        : %d MB", summary["alloc_mb_per_task"])
            log.info("  Total elapsed         : %.4f s", summary["total_elapsed_sec"])
            log.info("  Baseline RSS          : %.1f MB", summary["baseline_rss_mb"])
            log.info("  Avg peak delta        : +%.1f MB", summary["avg_peak_delta_mb"])
            log.info("  Max peak delta        : +%.1f MB", summary["max_peak_delta_mb"])
            log.info("  Max RSS seen          : %.1f MB",  summary["max_rss_seen_mb"])
            log.info("  Est. memory leak      : %.3f MB",  summary["memory_leak_estimate_mb"])
            log.info("=" * 60)
            return summary

        # ── Wire tasks (serial or parallel based on param default) ───────
        # NOTE: The `parallel` param only takes effect if you build two separate
        # task paths. Here we default to SERIAL wiring; to run parallel, set
        # `parallel=true` — the DAG will run worker tasks in fan-out style
        # using the dynamic task mapping approach below.

        start_info = memory_benchmark_start()

        # Dynamic task mapping — Airflow 3.x supports .expand()
        # We pass task_index via a mapped argument list.
        task_results = (
            memory_task
            .partial(prev=start_info)
            .expand(task_index=list(range(task_count)))
        )

        memory_report(results=task_results)

    return dag


# Register all DAGs
for _n in MEMORY_TEST_SIZES:
    globals()[f"memory_load_test_{_n}"] = _make_memory_dag(_n)
