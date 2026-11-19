"""WasmExecutor demo: templating, retry on failure, and fallback for non-wasm tasks."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airjet_airflow import WasmOperator

with DAG(
    dag_id="airjet_wasmexec_demo",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airjet"],
    default_args={"executor": "WasmExecutor"},
):
    # Templated args are rendered by the executor from the serialized DAG.
    aggregate = WasmOperator(
        task_id="aggregate",
        module="airjet-pipeline",
        args=["run", "/data/orders.csv", "/out/orders-{{ ds }}-{{ task_id }}.parquet"],
    )
    # Fails (input doesn't exist); retried once, then marked failed.
    missing_input = WasmOperator(
        task_id="missing_input",
        module="airjet-pipeline",
        args=["run", "/data/does-not-exist.csv", "/out/x.parquet"],
        retries=1,
        retry_delay=timedelta(seconds=2),
    )
    # Not a WasmOperator: the WasmExecutor hands it to a regular task process.
    report = BashOperator(task_id="report", bash_command="echo 'fallback task ran in a normal process'")
    aggregate >> report
