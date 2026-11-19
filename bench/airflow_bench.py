#!/usr/bin/env python3
"""End-to-end Airflow benchmark. Runs on the host, drives the cluster via kubectl.

For every bench_<variant>_<workload> DAG: one warm-up run (discarded), then --runs
measured runs, triggered sequentially. Timings come from Airflow's metadata DB:

  queue_to_start  ti.start_date - ti.queued_dttm     executor launch (k8s exec: worker pod)
  task_runtime    ti.end_date   - ti.start_date      operator incl. KPO pod / runner call
  task_total      ti.end_date   - dag_run.start_date scheduled -> done, per task
  makespan        dag_run.end_date - dag_run.start_date   all FAN_OUT tasks done

Usage: bench/airflow_bench.py [--runs 3] [--variants ...] [--workloads run noop] [--out results/]
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

NS = "airjet"
VARIANTS = ["kpo_k8sexec", "kpo_local", "wasm_k8sexec", "wasm_local", "wasm_wasmexec", "uv_bash_local",
            "uv_op_local", "native_bash_local"]


def kubectl(*args: str, check: bool = True) -> str:
    return subprocess.run(["kubectl", "-n", NS, *args], check=check, capture_output=True, text=True).stdout


def psql(sql: str) -> list[list[str]]:
    out = kubectl("exec", "deploy/postgres", "--", "psql", "-U", "airflow", "-d", "airflow", "-tA", "-F", "\t",
                  "-c", sql)
    return [line.split("\t") for line in out.strip().splitlines() if line]


def trigger(dag_id: str, run_id: str) -> None:
    kubectl("exec", "deploy/airflow-scheduler", "--", "airflow", "dags", "trigger", dag_id, "--run-id", run_id)


def wait(dag_id: str, run_id: str, timeout: float = 900) -> str:
    t = time.time()
    while time.time() - t < timeout:
        rows = psql(f"select state from dag_run where dag_id='{dag_id}' and run_id='{run_id}'")
        if rows and rows[0][0] in ("success", "failed"):
            return rows[0][0]
        time.sleep(1)
    raise TimeoutError(f"{dag_id}/{run_id}")


def ms(a: str, b: str) -> float:
    pa, pb = datetime.fromisoformat(a), datetime.fromisoformat(b)
    return (pb - pa).total_seconds() * 1e3


def collect(dag_id: str, run_id: str) -> dict:
    (dr,) = psql(f"select start_date, end_date, state from dag_run where dag_id='{dag_id}' and run_id='{run_id}'")
    tis = psql(f"select task_id, queued_dttm, start_date, end_date, state from task_instance "
               f"where dag_id='{dag_id}' and run_id='{run_id}' order by task_id")
    tasks = [{
        "task_id": t[0], "state": t[4],
        "queue_to_start": ms(t[1], t[2]), "task_runtime": ms(t[2], t[3]), "task_total": ms(dr[0], t[3]),
    } for t in tis if t[1] and t[2] and t[3]]
    return {"dag_id": dag_id, "run_id": run_id, "state": dr[2], "makespan": ms(dr[0], dr[1]), "tasks": tasks}


def summarize(runs: list[dict]) -> dict:
    def med(xs):
        return round(statistics.median(xs), 0) if xs else None

    tasks = [t for r in runs for t in r["tasks"]]
    return {
        "runs": len(runs),
        "failed_runs": sum(r["state"] != "success" for r in runs),
        "makespan_ms_p50": med([r["makespan"] for r in runs]),
        "queue_to_start_ms_p50": med([t["queue_to_start"] for t in tasks]),
        "task_runtime_ms_p50": med([t["task_runtime"] for t in tasks]),
        "task_total_ms_p50": med([t["task_total"] for t in tasks]),
        "task_total_ms_max": round(max((t["task_total"] for t in tasks), default=0), 0),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--variants", nargs="*", default=VARIANTS)
    ap.add_argument("--workloads", nargs="*", default=["run", "noop"])
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    summary = {}
    for workload in a.workloads:
        for variant in a.variants:
            dag_id = f"bench_{variant}_{workload}"
            runs = []
            for i in range(a.runs + 1):
                run_id = f"bench_{stamp}_{i}"
                trigger(dag_id, run_id)
                state = wait(dag_id, run_id)
                if i == 0:
                    print(f"{dag_id}: warm-up {state}", file=sys.stderr)
                    continue
                r = collect(dag_id, run_id)
                runs.append(r)
                print(f"{dag_id} run {i}: {state} makespan={r['makespan']:.0f}ms", file=sys.stderr)
            summary[dag_id] = summarize(runs)
            print(json.dumps({"dag_id": dag_id, **summary[dag_id]}), flush=True)
            (out / f"{stamp}_{dag_id}.json").write_text(json.dumps(runs, indent=1))
    (out / f"{stamp}_summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
