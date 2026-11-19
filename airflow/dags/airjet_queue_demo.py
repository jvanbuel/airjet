"""WasmQueueExecutor demos: routing to runner pools, retries, and cancellation."""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG

from airjet_airflow import WasmOperator

common = dict(schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False,
              tags=["airjet"], default_args={"executor": "WasmQueueExecutor"})

with DAG(dag_id="airjet_queue_demo", **common):
    # queue="default": served by deploy/airjet-runner
    WasmOperator(task_id="aggregate", module="airjet-pipeline",
                 args=["run", "/data/orders.csv", "/out/orders-{{ ds }}.parquet"])
    # queue="highmem": only deploy/airjet-runner-highmem claims it
    WasmOperator(task_id="aggregate_highmem", queue="highmem", module="airjet-pipeline",
                 args=["run", "/data/orders.csv", "/out/orders-{{ ds }}.parquet"], memory_limit_mb=4096)
    # fails, retried once by Airflow (new try -> new job), then failed
    WasmOperator(task_id="missing_input", module="airjet-pipeline",
                 args=["run", "/data/does-not-exist.csv", "/out/x.parquet"],
                 retries=1, retry_delay=timedelta(seconds=2))

with DAG(dag_id="airjet_queue_cancel_demo", **common):
    # Loops forever; mark it failed in the UI (or via the API) and the runner stops it
    # at its next heartbeat. Otherwise the 10-minute timeout kills it.
    WasmOperator(task_id="spin", module="spin", timeout_secs=600)

with DAG(dag_id="airjet_queue_restart_demo", **common):
    # Runs ~90 s (then hits its timeout): restart the scheduler meanwhile; the job keeps
    # running on the runner and the new scheduler adopts the task and collects its result.
    WasmOperator(task_id="long", module="spin", timeout_secs=90)
