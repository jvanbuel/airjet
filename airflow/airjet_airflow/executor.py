"""WasmExecutor: run WasmOperator tasks without any per-task process or pod.

With the LocalExecutor, every task, including a WasmOperator that only makes one
HTTP call, gets a Python task process: fork, load the DAG file, render templates,
talk to the Execution API, then call the runner. That costs ~0.4 s per task, while
the wasm instantiation it ends up doing takes ~0.2 ms.

This executor short-circuits that for WasmOperator tasks:

  1. read the task's definition from the *serialized* DAG in the metadata DB
     (no user DAG code runs in the scheduler),
  2. tell the Execution API the task is running (same call the supervisor makes),
  3. render the templated fields and POST them to airjet-runner,
  4. stream the result into the task log and report the terminal state
     (success / up_for_retry / failed) through the Execution API.

All of that happens on a thread pool inside the scheduler process. Any other task
routed to this executor falls back to the normal LocalExecutor worker processes, so
it is safe to make it the default executor.

Enable it as an Airflow 3 multi-executor entry, e.g.::

    AIRFLOW__CORE__EXECUTOR="LocalExecutor,WasmExecutor:airjet_airflow.executor.WasmExecutor"

and route tasks to it with ``executor="WasmExecutor"``.

Prototype limits: no XCom push, no ``on_*_callback``s, no outlets/assets, and only
a small template context (see ``_template_context``). Tasks needing those should keep
using the WasmOperator under the LocalExecutor.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from airflow.executors.base_executor import get_execution_api_server_url
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import ExecuteTask
from airflow.utils.state import TaskInstanceState

WASM_TASK_TYPES = {"WasmOperator"}
HEARTBEAT_EVERY = 30.0


class _TaskLog:
    """Writes JSON-lines task logs where Airflow's log reader expects them."""

    def __init__(self, base: str, rel: str | None):
        self.path = Path(base) / rel if rel else None
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self.path, "a", buffering=1) if self.path else None

    def __call__(self, event: str, level: str = "info", **kw: Any) -> None:
        if self._f:
            rec = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": level,
                   "logger": "airjet.WasmExecutor", "event": event, **kw}
            self._f.write(json.dumps(rec, default=str) + "\n")

    def close(self) -> None:
        if self._f:
            self._f.close()


class WasmExecutor(LocalExecutor):
    """LocalExecutor that runs WasmOperator tasks in-process against airjet-runner."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._results: queue.SimpleQueue = queue.SimpleQueue()
        self._pool: ThreadPoolExecutor | None = None
        self._tasks: OrderedDict = OrderedDict()  # (dag_version_id, task_id) -> task spec | None
        self._http = requests.Session()
        self._runner_url = os.environ.get("AIRJET_RUNNER_URL", "http://airjet-runner:8080").rstrip("/")

    # -- lifecycle ----------------------------------------------------------------
    def start(self) -> None:
        # Like LocalExecutor.start, but don't pre-fork `parallelism` workers: they're
        # only needed for fallback tasks and _check_workers() spawns them on demand.
        from multiprocessing import SimpleQueue, Value
        import ctypes

        self.activity_queue = SimpleQueue()
        self.result_queue = SimpleQueue()
        self.workers = {}
        self._unread_messages = Value(ctypes.c_uint)
        self._pool = ThreadPoolExecutor(max_workers=self.parallelism, thread_name_prefix="wasm-exec")
        self.log.info("WasmExecutor started (threads=%d, runner=%s)", self.parallelism, self._runner_url)

    def sync(self) -> None:
        while not self._results.empty():
            key, state = self._results.get()
            self.change_state(key, state)
        super().sync()

    def end(self) -> None:
        if self._pool:
            self._pool.shutdown(wait=True)
        self.sync()
        super().end()

    # -- dispatch -----------------------------------------------------------------
    def _process_workloads(self, workload_list):
        fallback = []
        for w in workload_list:
            spec = self._wasm_spec(w) if isinstance(w, ExecuteTask) else None
            if spec is None:
                fallback.append(w)
                continue
            self.queued_tasks.pop(w.key, None)
            self._results.put((w.key, TaskInstanceState.RUNNING))
            self._pool.submit(self._run_safely, w, spec)
        if fallback:
            super()._process_workloads(fallback)

    def _wasm_spec(self, w: ExecuteTask) -> dict | None:
        return wasm_spec(w, self._tasks)

    # -- execution (worker threads) ----------------------------------------------
    def _run_safely(self, w: ExecuteTask, spec: dict) -> None:
        try:
            self._run(w, spec)
            self._results.put((w.key, w.success_state))
        except Exception:
            self.log.exception("WasmExecutor failed to run %s", w.key)
            self._results.put((w.key, w.failure_state))

    def _run(self, w: ExecuteTask, spec: dict) -> None:
        from airflow.configuration import conf
        from airflow.sdk.api.client import Client

        ti = w.ti
        log = _TaskLog(conf.get("logging", "base_log_folder"), str(w.log_path) if w.log_path else None)
        client = Client(base_url=get_execution_api_server_url(), token=w.token)
        try:
            run_ctx = client.task_instances.start(ti.id, os.getpid(), datetime.now(timezone.utc))
            log("Task started by WasmExecutor (no task process)", ti_id=str(ti.id))

            stop = threading.Event()
            hb = threading.Thread(target=self._heartbeat, args=(client, ti.id, stop), daemon=True)
            hb.start()
            try:
                ok, detail = self._call_runner(w, spec, run_ctx, log)
            finally:
                stop.set()

            now = datetime.now(timezone.utc)
            if ok:
                client.task_instances.succeed(ti.id, when=now, task_outlets=[], outlet_events=[],
                                              rendered_map_index=None)
                log("Task succeeded")
            elif run_ctx.should_retry:
                client.task_instances.retry(ti.id, end_date=now, rendered_map_index=None,
                                            retry_reason=detail)
                log(f"Task failed, will retry: {detail}", level="error")
            else:
                client.task_instances.finish(ti.id, state=TaskInstanceState.FAILED, when=now,
                                             rendered_map_index=None)
                log(f"Task failed: {detail}", level="error")
        finally:
            log.close()
            client.close()

    def _call_runner(self, w: ExecuteTask, spec: dict, run_ctx, log) -> tuple[bool, str]:
        payload = render_run_request(w, spec, run_ctx.dag_run)
        timeout = payload["timeout_secs"]
        log("Calling airjet-runner", module=payload["module"], args=payload["args"])
        t0 = time.perf_counter()
        try:
            resp = self._http.post(f"{self._runner_url}/v1/run", json=payload, timeout=timeout + 30)
        except requests.RequestException as e:
            return False, f"airjet-runner unreachable: {e}"
        if resp.status_code != 200:
            return False, f"airjet-runner rejected task ({resp.status_code}): {resp.text}"
        r = resp.json()
        for stream in ("stdout", "stderr"):
            for line in r[stream].splitlines():
                log(f"[{stream}] {line}")
        log("wasm task finished", exit_code=r["exit_code"], instantiate_ms=round(r["instantiate_ms"], 3),
            exec_ms=round(r["exec_ms"], 1), round_trip_ms=round((time.perf_counter() - t0) * 1e3, 1),
            out_dir=r["out_dir"])
        if r.get("error") or r["exit_code"] != 0:
            return False, f"exit={r['exit_code']} {r.get('error') or ''}".strip()
        return True, ""

    @staticmethod
    def _heartbeat(client, ti_id, stop: threading.Event) -> None:
        while not stop.wait(HEARTBEAT_EVERY):
            try:
                client.task_instances.heartbeat(ti_id, pid=os.getpid())
            except Exception:
                pass


def wasm_spec(w: ExecuteTask, cache: OrderedDict) -> dict | None:
    """Look a task up in the serialized DAG version it was scheduled with.

    Returns its templated fields if it is a WasmOperator, else None. Cached per
    (dag_version_id, task_id): a DAG version is immutable.
    """
    cache_key = (str(w.ti.dag_version_id), w.ti.task_id)
    if cache_key in cache:
        cache.move_to_end(cache_key)
        return cache[cache_key]
    from airflow.models.dag_version import DagVersion
    from airflow.utils.session import create_session

    spec = None
    with create_session() as session:
        dv = session.get(DagVersion, w.ti.dag_version_id)
        if dv is not None and dv.serialized_dag is not None:
            task = dv.serialized_dag.dag.get_task(w.ti.task_id)
            if task.task_type in WASM_TASK_TYPES:
                spec = {f: getattr(task, f, None) for f in task.template_fields}
                spec["retries"] = task.retries
    cache[cache_key] = spec
    if len(cache) > 4096:
        cache.popitem(last=False)
    return spec


def render_run_request(w: ExecuteTask, spec: dict, dag_run) -> dict[str, Any]:
    """Render a WasmOperator spec into an airjet-runner /v1/run request body."""
    render = _renderer(_template_context(w, dag_run))
    return {
        "module": render(spec["module"]),
        "args": [str(render(a)) for a in (spec.get("args") or [])],
        "env": {k: str(render(v)) for k, v in (spec.get("env") or {}).items()},
        "run_id": f"{w.ti.dag_id}.{w.ti.task_id}.{w.ti.map_index}.{w.ti.try_number}",
        "timeout_secs": float(render(spec.get("timeout_secs") or 3600)),
        "memory_limit_mb": int(render(spec.get("memory_limit_mb") or 2048)),
    }


def _template_context(w: ExecuteTask, dr) -> dict[str, Any]:
    """Template context from a dag run (Execution API DagRun or ORM DagRun)."""
    logical = dr.logical_date
    return {
        "dag_id": w.ti.dag_id,
        "task_id": w.ti.task_id,
        "run_id": w.ti.run_id,
        "try_number": w.ti.try_number,
        "map_index": w.ti.map_index,
        "logical_date": logical,
        "ds": logical.strftime("%Y-%m-%d") if logical else None,
        "ts": logical.isoformat() if logical else None,
        "data_interval_start": dr.data_interval_start,
        "data_interval_end": dr.data_interval_end,
        "params": dict(getattr(dr, "conf", None) or {}),
        "ti": w.ti,
    }


def _renderer(ctx: dict[str, Any]):
    from jinja2.sandbox import SandboxedEnvironment

    env = SandboxedEnvironment()

    def render(v):
        if isinstance(v, str) and "{{" in v:
            return env.from_string(v).render(**ctx)
        return v

    return render
