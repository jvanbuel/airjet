"""WasmOperator: run a WASI module on an airjet-runner instead of in a new pod."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator


class WasmOperator(BaseOperator):
    """Execute ``module`` (a WASI preview1 program) on the airjet-runner service.

    The task runs inside a fresh wasmtime store on a long-lived runner pod: no pod
    scheduling, no image pull, no container start. The guest sees only ``/data``
    (read-only) and a private ``/out`` directory, and is bounded by
    ``timeout_secs`` and ``memory_limit_mb``.

    :param module: module name, i.e. ``<module>.wasm`` in the runner's module dir.
    :param args: argv[1:] passed to the module (templated).
    :param env: environment variables visible to the guest (templated).
    :param runner_url: base URL of the runner; defaults to ``$AIRJET_RUNNER_URL``.
    """

    # timeout/memory are templated mostly so they're kept in the serialized DAG,
    # which is where the WasmExecutor reads task definitions from.
    template_fields: Sequence[str] = ("module", "args", "env", "timeout_secs", "memory_limit_mb")
    ui_color = "#654ff0"

    def __init__(
        self,
        *,
        module: str,
        args: Sequence[str] = (),
        env: dict[str, str] | None = None,
        runner_url: str | None = None,
        timeout_secs: float = 3600,
        memory_limit_mb: int = 2048,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.module = module
        self.args = list(args)
        self.env = env or {}
        self.runner_url = runner_url
        self.timeout_secs = timeout_secs
        self.memory_limit_mb = memory_limit_mb

    def execute(self, context: Any) -> dict[str, Any]:
        url = (self.runner_url or os.environ.get("AIRJET_RUNNER_URL", "http://airjet-runner:8080")).rstrip("/")
        ti = context["ti"]
        payload = {
            "module": self.module,
            "args": [str(a) for a in self.args],
            "env": {k: str(v) for k, v in self.env.items()},
            "run_id": f"{ti.dag_id}.{ti.task_id}.{ti.map_index}.{ti.try_number}",
            "timeout_secs": float(self.timeout_secs),
            "memory_limit_mb": int(self.memory_limit_mb),
        }
        try:
            resp = requests.post(f"{url}/v1/run", json=payload, timeout=float(self.timeout_secs) + 30)
        except requests.RequestException as e:
            raise AirflowException(f"airjet-runner unreachable at {url}: {e}") from e
        if resp.status_code != 200:
            raise AirflowException(f"airjet-runner rejected task ({resp.status_code}): {resp.text}")
        r = resp.json()
        for stream in ("stdout", "stderr"):
            for line in r[stream].splitlines():
                self.log.info("[%s] %s", stream, line)
        self.log.info(
            "wasm task: exit=%s queue=%.2fms instantiate=%.2fms exec=%.1fms out=%s",
            r["exit_code"], r["queue_ms"], r["instantiate_ms"], r["exec_ms"], r["out_dir"],
        )
        if r.get("error") or r["exit_code"] != 0:
            raise AirflowException(f"wasm module {self.module} failed: exit={r['exit_code']} {r.get('error') or ''}")
        return {k: r[k] for k in ("exit_code", "queue_ms", "instantiate_ms", "exec_ms", "out_dir", "stdout")}
