"""WasmQueueExecutor: Celery-style, pull-based execution of WasmOperator tasks.

The WasmExecutor pushes: a scheduler thread holds an HTTP request open per running
task, so in-flight work is tied to one scheduler process, load balancing is whatever
the Service does, and there's no routing. This executor uses a durable queue instead
(the ``airjet_job`` table in Postgres, see ``runner/src/queue.rs``):

    scheduler ──INSERT + NOTIFY──► airjet_job ◄──claim (SKIP LOCKED)── airjet-runner xN
        ▲                                                     │
        └──── sync(): finished rows → task log + state ◄──────┘  (Execution API calls
                                                                    go runner → Airflow)

* Producing is cheap: one INSERT per task; no thread is held while it runs.
* Workers pull only when they have a free slot, so jobs queue in the table and spread
  over however many runner replicas there are (scale them on queue depth).
* ``queue=`` on the operator routes to the runner pools subscribed to that queue.
* Runners drive the task lifecycle themselves through the Execution API (enter running,
  heartbeat, terminal state). Clearing or failing a task in the UI makes the next
  heartbeat fail, and the runner interrupts the guest.
* In-flight jobs survive a scheduler restart: each task's ``external_executor_id``
  points at its job, and the new scheduler adopts those tasks.

Any non-wasm task routed here falls back to LocalExecutor worker processes, as with
the WasmExecutor. The queue lives in the Airflow metadata DB for this POC; any Postgres
reachable by scheduler and runners would do.

Enable with::

    AIRFLOW__CORE__EXECUTOR="LocalExecutor,WasmQueueExecutor:airjet_airflow.queue_executor.WasmQueueExecutor"
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from airflow.executors.base_executor import get_execution_api_server_url
from airflow.executors.workloads import ExecuteTask
from airflow.utils.state import TaskInstanceState

from airjet_airflow.executor import WasmExecutor, render_run_request, wasm_spec

JOB_PREFIX = "airjet-job:"
TERMINAL = ("success", "failed", "up_for_retry", "cancelled", "error")
SCHEMA = (Path(__file__).with_name("queue_schema.sql")).read_text()


class WasmQueueExecutor(WasmExecutor):
    """Enqueue WasmOperator tasks for pull-based airjet-runner workers."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_purge = 0.0

    def start(self) -> None:
        super().start()
        self._pool.shutdown(wait=False)  # no in-process execution in this executor
        self._pool = None
        with self._engine().begin() as conn:
            conn.exec_driver_sql(SCHEMA)
        self.log.info("WasmQueueExecutor started")

    @staticmethod
    def _engine():
        from airflow import settings

        return settings.engine

    # -- producing ------------------------------------------------------------------
    def _process_workloads(self, workload_list):
        fallback, jobs = [], []
        for w in workload_list:
            spec = wasm_spec(w, self._tasks) if isinstance(w, ExecuteTask) else None
            if spec is None:
                fallback.append(w)
            else:
                jobs.append((w, spec))
        if jobs:
            self._enqueue(jobs)
        if fallback:
            # skip WasmExecutor._process_workloads: go straight to LocalExecutor's
            super(WasmExecutor, self)._process_workloads(fallback)

    def _enqueue(self, jobs) -> None:
        from airflow.models.dagrun import DagRun
        from airflow.utils.session import create_session
        from sqlalchemy import select, text

        api_url = get_execution_api_server_url()
        rows = []
        with create_session() as session:
            runs = {}
            for w, spec in jobs:
                k = (w.ti.dag_id, w.ti.run_id)
                if k not in runs:
                    runs[k] = session.scalar(select(DagRun).where(DagRun.dag_id == k[0], DagRun.run_id == k[1]))
                request = render_run_request(w, spec, runs[k])
                rows.append({
                    "queue": w.ti.queue or "default",
                    "ti_id": str(w.ti.id),
                    "ti_key": json.dumps(w.ti.key._asdict()),
                    "token": w.token,
                    "api_url": api_url,
                    "spec": json.dumps(request),
                    "log_path": str(w.log_path) if w.log_path else None,
                })
        with self._engine().begin() as conn:
            ids = [
                conn.execute(
                    text(
                        "insert into airjet_job (queue, ti_id, ti_key, token, api_url, spec, log_path) "
                        "values (:queue, cast(:ti_id as uuid), cast(:ti_key as jsonb), :token, :api_url, "
                        "cast(:spec as jsonb), :log_path) "
                        "on conflict (ti_id) do update set state='queued', token=excluded.token, "
                        "spec=excluded.spec, reported=false "
                        "returning id"
                    ),
                    r,
                ).scalar_one()
                for r in rows
            ]
            for q in {r["queue"] for r in rows}:
                conn.execute(text("select pg_notify('airjet_jobs', :q)"), {"q": q})
        for (w, _), job_id in zip(jobs, ids):
            self.queued_tasks.pop(w.key, None)
            self.queued(w.key, f"{JOB_PREFIX}{job_id}")  # becomes ti.external_executor_id

    # -- consuming results ------------------------------------------------------------
    def sync(self) -> None:
        from sqlalchemy import text

        with self._engine().begin() as conn:
            done = conn.execute(
                text(
                    "update airjet_job set reported=true where id in ("
                    "  select id from airjet_job where not reported and state = any(:terminal)"
                    "  for update skip locked) "
                    "returning id, ti_key, state, worker, exit_code, stdout, stderr, error, timings, log_path, "
                    "created_at, claimed_at, finished_at"
                ),
                {"terminal": list(TERMINAL)},
            ).mappings().all()
            if time.monotonic() - self._last_purge > 300:
                conn.execute(text("delete from airjet_job where reported and finished_at < now() - interval '1 hour'"))
                self._last_purge = time.monotonic()
        for row in done:
            key = _ti_key(row["ti_key"])
            self._write_log(row)
            # Like LocalExecutor: report the workload as completed; the terminal TI state
            # was already set by the runner through the Execution API.
            state = TaskInstanceState.FAILED if row["state"] == "error" else TaskInstanceState.SUCCESS
            self.change_state(key, state)
        super(WasmExecutor, self).sync()  # LocalExecutor fallback workers

    def _write_log(self, row) -> None:
        from airflow.configuration import conf

        if not row["log_path"]:
            return
        path = Path(conf.get("logging", "base_log_folder")) / row["log_path"]
        path.parent.mkdir(parents=True, exist_ok=True)

        def line(ts, event, level="info", **kw):
            rec = {"timestamp": (ts or datetime.now(timezone.utc)).isoformat(), "level": level,
                   "logger": "airjet.WasmQueueExecutor", "event": event, **kw}
            return json.dumps(rec, default=str) + "\n"

        with open(path, "a") as f:
            f.write(line(row["created_at"], f"Queued as airjet job {row['id']}"))
            f.write(line(row["claimed_at"], f"Claimed by worker {row['worker']}"))
            for stream in ("stdout", "stderr"):
                for text_line in (row[stream] or "").splitlines():
                    f.write(line(row["finished_at"], f"[{stream}] {text_line}"))
            level = "info" if row["state"] == "success" else "error"
            f.write(line(row["finished_at"], f"Job finished: {row['state']}", level, exit_code=row["exit_code"],
                         error=row["error"], timings=row["timings"]))

    # -- scheduler restarts -----------------------------------------------------------
    def try_adopt_task_instances(self, tis):
        """Adopt tasks whose airjet job is still queued/running; results arrive via sync()."""
        not_adopted = []
        for ti in tis:
            if ti.external_executor_id and ti.external_executor_id.startswith(JOB_PREFIX):
                self.running.add(ti.key)
                self.log.info("Adopted %s (%s)", ti.key, ti.external_executor_id)
            else:
                not_adopted.append(ti)
        return not_adopted


def _ti_key(d: dict | str):
    from airflow.models.taskinstancekey import TaskInstanceKey

    if isinstance(d, str):
        d = json.loads(d)
    return TaskInstanceKey(**d)
