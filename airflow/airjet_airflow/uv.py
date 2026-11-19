"""UvOperator: run a Python script in a cached, uv-managed virtualenv."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import resource
import shutil
import subprocess
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

_PEP723 = re.compile(r"(?m)^# /// script\s*$\n(?P<body>(?:^#(?:| .*)$\n)+?)^# ///\s*$")


def read_script_metadata(script: str) -> dict[str, Any]:
    """Parse PEP 723 inline metadata (``# /// script``) from a script."""
    import tomllib

    m = _PEP723.search(Path(script).read_text())
    if not m:
        return {}
    body = "".join(line[2:] if line.startswith("# ") else line[1:] for line in m.group("body").splitlines(True))
    return tomllib.loads(body)


class UvOperator(BaseOperator):
    """Run ``script`` with its dependencies in an isolated virtualenv built by uv.

    Unlike ``BashOperator("uv run --script ...")`` the environment is resolved and
    installed once per unique (python, dependencies) set and then reused directly,
    so a warm task start is a bare ``python`` exec: no uv resolution, no shell.

    Isolation here is *dependency* isolation plus light process hygiene (scrubbed
    environment, private temp cwd, ``python -I``, optional rlimits). It is not a
    security sandbox: the script runs as the worker user with the worker's
    filesystem and network. Use the WasmOperator when you need a real sandbox.

    :param script: path to the Python script (PEP 723 metadata is honoured).
    :param args: arguments for the script (templated).
    :param dependencies: overrides the script's inline dependencies.
    :param python: python version request for ``uv venv`` (default: script's
        ``requires-python`` or the interpreter uv finds).
    :param env: extra environment for the process (templated).
    :param inherit_env: pass the worker's environment through (default False).
    :param memory_limit_mb: RLIMIT_AS for the child process.
    :param cpu_time_secs: RLIMIT_CPU for the child process.
    :param env_cache_dir: where environments are kept (``$AIRJET_UV_ENV_DIR``).
    """

    template_fields: Sequence[str] = ("script", "args", "env")
    ui_color = "#de5fe9"

    def __init__(
        self,
        *,
        script: str,
        args: Sequence[str] = (),
        dependencies: Sequence[str] | None = None,
        python: str | None = None,
        env: dict[str, str] | None = None,
        inherit_env: bool = False,
        memory_limit_mb: int | None = None,
        cpu_time_secs: int | None = None,
        env_cache_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.script = script
        self.args = list(args)
        self.dependencies = list(dependencies) if dependencies is not None else None
        self.python = python
        self.env = env or {}
        self.inherit_env = inherit_env
        self.memory_limit_mb = memory_limit_mb
        self.cpu_time_secs = cpu_time_secs
        self.env_cache_dir = env_cache_dir

    # -- environment management -------------------------------------------------
    def _uv(self) -> str:
        uv = shutil.which("uv")
        if not uv:
            raise AirflowException("uv executable not found on PATH")
        return uv

    def ensure_env(self) -> Path:
        meta = read_script_metadata(self.script)
        deps = sorted(self.dependencies if self.dependencies is not None else meta.get("dependencies", []))
        python = self.python or meta.get("requires-python")
        key = hashlib.sha256(json.dumps({"deps": deps, "python": python}).encode()).hexdigest()[:16]
        root = Path(self.env_cache_dir or os.environ.get("AIRJET_UV_ENV_DIR", "/tmp/airjet-uv-envs"))
        env_dir = root / key
        ready = env_dir / ".airjet-ready"
        if ready.exists():
            return env_dir

        root.mkdir(parents=True, exist_ok=True)
        with open(root / f"{key}.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)  # serialize concurrent builders of the same env
            if ready.exists():
                return env_dir
            t0 = time.perf_counter()
            uv = self._uv()
            shutil.rmtree(env_dir, ignore_errors=True)
            cmd = [uv, "venv", "--quiet", str(env_dir)]
            if python:
                cmd += ["--python", python]
            subprocess.run(cmd, check=True)
            if deps:
                subprocess.run(
                    [uv, "pip", "install", "--quiet", "--python", str(env_dir / "bin" / "python"), *deps],
                    check=True,
                )
            ready.write_text(json.dumps({"deps": deps, "python": python}))
            self.log.info("built uv env %s (%s) in %.2fs", key, deps, time.perf_counter() - t0)
        return env_dir

    # -- execution ----------------------------------------------------------------
    def _limits(self):
        mem, cpu = self.memory_limit_mb, self.cpu_time_secs
        if not mem and not cpu:
            return None

        def apply() -> None:
            if mem:
                resource.setrlimit(resource.RLIMIT_AS, (mem << 20, mem << 20))
            if cpu:
                resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))

        return apply

    def execute(self, context: Any) -> dict[str, Any]:
        env_dir = self.ensure_env()
        python = env_dir / "bin" / "python"
        with tempfile.TemporaryDirectory(prefix="airjet-uv-") as tmp:
            if self.inherit_env:
                child_env = dict(os.environ)
            else:
                child_env = {"PATH": f"{env_dir / 'bin'}:/usr/local/bin:/usr/bin:/bin", "HOME": tmp,
                             "TMPDIR": tmp, "LANG": "C.UTF-8"}
            child_env.update({"VIRTUAL_ENV": str(env_dir), **{k: str(v) for k, v in self.env.items()}})
            t0 = time.perf_counter()
            proc = subprocess.run(
                [str(python), "-I", self.script, *map(str, self.args)],
                cwd=tmp, env=child_env, capture_output=True, text=True, preexec_fn=self._limits(),
            )
            exec_ms = (time.perf_counter() - t0) * 1e3
        for stream, text in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            for line in text.splitlines():
                self.log.info("[%s] %s", stream, line)
        self.log.info("uv task: exit=%s exec=%.1fms env=%s", proc.returncode, exec_ms, env_dir.name)
        if proc.returncode != 0:
            raise AirflowException(f"{self.script} exited with {proc.returncode}")
        return {"exit_code": proc.returncode, "exec_ms": exec_ms, "stdout": proc.stdout}
