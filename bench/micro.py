"""Micro-benchmark: cost of launching one task through each mechanism, no Airflow.

Runs inside the airflow-scheduler pod (it has uv, the native binary, the pipeline
script, in-cluster k8s credentials and network access to airjet-runner):

    kubectl -n airjet cp bench/micro.py <scheduler-pod>:/tmp/micro.py
    kubectl -n airjet exec <scheduler-pod> -- python /tmp/micro.py --n 10

Prints one JSON line per (mechanism, workload) with latency stats in ms.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import tempfile
import time
import uuid

import requests

AIRJET = "/opt/airjet"
DATA = f"{AIRJET}/data/orders.csv"
RUNNER = os.environ.get("AIRJET_RUNNER_URL", "http://airjet-runner:8080")
NS = os.environ.get("AIRJET_NAMESPACE", "airjet")
IMAGE = os.environ.get("AIRJET_PIPELINE_IMAGE", "airjet/pipeline-native:dev")


def args(workload: str, data: str, out: str) -> list[str]:
    return ["run", data, out] if workload == "run" else ["noop"]


def native(workload: str) -> None:
    subprocess.run([f"{AIRJET}/bin/airjet-pipeline", *args(workload, DATA, "/tmp/m-native.parquet")],
                   check=True, capture_output=True)


def uv_run(workload: str) -> None:
    subprocess.run(["uv", "run", "--quiet", "--script", f"{AIRJET}/pipeline.py",
                    *args(workload, DATA, "/tmp/m-uv.parquet")], check=True, capture_output=True)


_uv_env = None


def uv_operator(workload: str) -> None:
    """What UvOperator does per task once its env is cached."""
    global _uv_env
    if _uv_env is None:
        from airjet_airflow import UvOperator

        _uv_env = UvOperator(task_id="micro", script=f"{AIRJET}/pipeline.py").ensure_env()
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run([f"{_uv_env}/bin/python", "-I", f"{AIRJET}/pipeline.py",
                        *args(workload, DATA, "/tmp/m-uvop.parquet")],
                       check=True, capture_output=True, cwd=tmp,
                       env={"PATH": f"{_uv_env}/bin:/usr/bin:/bin", "HOME": tmp})


_session = requests.Session()


def wasm_runner(workload: str) -> None:
    r = _session.post(f"{RUNNER}/v1/run", json={
        "module": "airjet-pipeline", "args": args(workload, "/data/orders.csv", "/out/agg.parquet")})
    r.raise_for_status()
    assert r.json()["exit_code"] == 0, r.text


_k8s = None


def k8s_pod(workload: str) -> None:
    """Create a pod and wait until it has Succeeded, like KubernetesPodOperator does."""
    global _k8s
    from kubernetes import client, config, watch

    if _k8s is None:
        config.load_incluster_config()
        _k8s = client.CoreV1Api()
    name = f"micro-{uuid.uuid4().hex[:8]}"
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, labels={"airjet": "micro"}),
        spec=client.V1PodSpec(restart_policy="Never", containers=[client.V1Container(
            name="base", image=IMAGE, image_pull_policy="IfNotPresent",
            args=args(workload, "/data/orders.csv", "/tmp/out.parquet"),
            resources=client.V1ResourceRequirements(requests={"cpu": "100m", "memory": "128Mi"}))]))
    _k8s.create_namespaced_pod(NS, pod)
    try:
        w = watch.Watch()
        for ev in w.stream(_k8s.list_namespaced_pod, NS, field_selector=f"metadata.name={name}",
                           timeout_seconds=300):
            phase = ev["object"].status.phase
            if phase in ("Succeeded", "Failed"):
                w.stop()
                if phase == "Failed":
                    raise RuntimeError(f"pod {name} failed")
                break
    finally:
        _k8s.delete_namespaced_pod(name, NS, grace_period_seconds=0)


MECHANISMS = {
    "native_exec": native,
    "uv_run_script": uv_run,
    "uv_operator_env": uv_operator,
    "wasm_runner_http": wasm_runner,
    "k8s_pod": k8s_pod,
}


def stats(xs: list[float]) -> dict[str, float]:
    xs = sorted(xs)
    return {"n": len(xs), "min": round(xs[0], 1), "p50": round(statistics.median(xs), 1),
            "p90": round(xs[min(len(xs) - 1, int(0.9 * len(xs)))], 1), "max": round(xs[-1], 1),
            "mean": round(statistics.fmean(xs), 1)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--only", nargs="*", default=list(MECHANISMS))
    a = ap.parse_args()
    for name in a.only:
        fn = MECHANISMS[name]
        for workload in ("noop", "run"):
            fn(workload)  # warm-up (env build, connection, page cache)
            xs = []
            for _ in range(a.n):
                t = time.perf_counter()
                fn(workload)
                xs.append((time.perf_counter() - t) * 1e3)
            print(json.dumps({"mechanism": name, "workload": workload, **stats(xs)}), flush=True)


if __name__ == "__main__":
    main()
