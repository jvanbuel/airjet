"""Benchmark DAGs: the same Polars aggregation run through each execution strategy.

One DAG per (variant, workload). Each DAG run fans out FAN_OUT identical tasks so we
measure both per-task latency and how each strategy behaves under concurrency.

workload:
  run   aggregate 200k orders with Polars -> parquet
  noop  start the task program and exit immediately (pure launch overhead)

variant                  executor            pods/task  task program
kpo_k8sexec   (baseline) KubernetesExecutor  2          native Rust/Polars in a container
kpo_local                LocalExecutor       1          native Rust/Polars in a container
wasm_k8sexec             KubernetesExecutor  1          Rust/Polars wasm on airjet-runner
wasm_local    (airjet)   LocalExecutor       0          Rust/Polars wasm on airjet-runner
uv_bash_local            LocalExecutor       0          BashOperator: uv run --script (Python Polars)
uv_op_local              LocalExecutor       0          UvOperator: cached uv venv (Python Polars)
native_bash_local (ref)  LocalExecutor       0          BashOperator: native binary, no isolation
"""

from __future__ import annotations

import os

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from airjet_airflow import UvOperator, WasmOperator

FAN_OUT = int(os.environ.get("AIRJET_BENCH_FAN_OUT", "8"))
NAMESPACE = os.environ.get("AIRJET_NAMESPACE", "airjet")
PIPELINE_IMAGE = os.environ.get("AIRJET_PIPELINE_IMAGE", "airjet/pipeline-native:dev")
AIRJET_HOME = "/opt/airjet"
DATA = f"{AIRJET_HOME}/data/orders.csv"
OUT = "/tmp/airjet-out/{{ run_id | replace(':', '_') | replace('+', '_') }}/{{ ti.task_id }}.parquet"


def args_for(workload: str, data: str, out: str) -> list[str]:
    return ["run", data, out] if workload == "run" else ["noop"]


def make_task(variant: str, workload: str, i: int):
    tid = f"t{i:02d}"
    if variant.startswith("kpo_"):
        return KubernetesPodOperator(
            task_id=tid,
            executor="KubernetesExecutor" if variant == "kpo_k8sexec" else "LocalExecutor",
            namespace=NAMESPACE,
            name=f"pipeline-{tid}",
            image=PIPELINE_IMAGE,
            image_pull_policy="IfNotPresent",
            cmds=["airjet-pipeline"],
            arguments=args_for(workload, "/data/orders.csv", "/tmp/out.parquet"),
            container_resources=k8s.V1ResourceRequirements(requests={"cpu": "100m", "memory": "128Mi"}),
            get_logs=True,
            on_finish_action="delete_pod",
            startup_check_interval_seconds=1,
            poll_interval=1,
        )
    if variant.startswith("wasm_"):
        return WasmOperator(
            task_id=tid,
            executor="KubernetesExecutor" if variant == "wasm_k8sexec" else "LocalExecutor",
            module="airjet-pipeline",
            # inside the sandbox: /data is the runner's data dir, /out is per-run
            args=args_for(workload, "/data/orders.csv", "/out/agg.parquet"),
            timeout_secs=600,
            memory_limit_mb=1024,
        )
    if variant == "uv_bash_local":
        a = " ".join(args_for(workload, DATA, OUT))
        return BashOperator(
            task_id=tid,
            executor="LocalExecutor",
            bash_command=f"mkdir -p $(dirname {OUT}) && uv run --quiet --script {AIRJET_HOME}/pipeline.py {a}",
        )
    if variant == "uv_op_local":
        return UvOperator(
            task_id=tid,
            executor="LocalExecutor",
            script=f"{AIRJET_HOME}/pipeline.py",
            args=args_for(workload, DATA, f"/tmp/airjet-uvop-{tid}.parquet"),
            memory_limit_mb=2048,
        )
    if variant == "native_bash_local":
        a = " ".join(args_for(workload, DATA, OUT))
        return BashOperator(
            task_id=tid,
            executor="LocalExecutor",
            bash_command=f"mkdir -p $(dirname {OUT}) && {AIRJET_HOME}/bin/airjet-pipeline {a}",
        )
    raise ValueError(variant)


VARIANTS = ["kpo_k8sexec", "kpo_local", "wasm_k8sexec", "wasm_local", "uv_bash_local", "uv_op_local",
            "native_bash_local"]

for variant in VARIANTS:
    for workload in ("run", "noop"):
        with DAG(
            dag_id=f"bench_{variant}_{workload}",
            schedule=None,
            start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
            catchup=False,
            max_active_runs=1,
            tags=["airjet-bench", variant, workload],
        ) as dag:
            for i in range(FAN_OUT):
                make_task(variant, workload, i)
        globals()[dag.dag_id] = dag
