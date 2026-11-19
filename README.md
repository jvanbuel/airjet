# airjet

Proof of concept: run Airflow data-pipeline tasks as **WebAssembly (WASI) modules on a
long-lived runner in Kubernetes**, instead of one or two pods per task.

With the KubernetesExecutor + KubernetesPodOperator (KPO), every task costs **two pods**:
the executor's worker pod, which then launches the KPO pod that does the work. Each pod
means API-server writes, scheduling, sandbox/network setup, an image pull (if not cached),
container start and teardown. airjet keeps the isolation (a sandbox per task) but drops
the pods: tasks run in a fresh wasmtime store inside an already-running `airjet-runner`.

We benchmark this against the KPO baseline and against **uv** as a lighter isolation layer
(`BashOperator` + `uv run --script`, and a custom `UvOperator`).

```
                        ┌──────────────── Kubernetes ────────────────────────────┐
 baseline (2 pods/task) │ scheduler ──► worker pod ──► KPO pod (native Polars)   │
 airjet   (0 pods/task) │ scheduler/LocalExecutor ──HTTP──► airjet-runner        │
                        │                              (wasmtime, store per task)│
 uv       (0 pods/task) │ scheduler/LocalExecutor ──► python in cached uv venv   │
                        └────────────────────────────────────────────────────────┘
```

## What's in here

| path | what |
|---|---|
| `pipeline/` | The task: a Rust **Polars** ETL step (read 200k orders CSV → filter → derive revenue/month → group-by aggregate → Parquet). One source, built natively (KPO image) and for `wasm32-wasip1` (runner). |
| `pipeline_py/pipeline.py` | The same transformation in Python Polars with PEP 723 inline deps, for the uv variants. All variants produce the identical result (480 rows, same revenue total). |
| `runner/` | `airjet-runner`: Rust + wasmtime + axum. Loads modules once, AOT-compiles them (`.cwasm` baked into the image), keeps an `InstancePre`, and runs each `POST /v1/run` in a fresh `Store`. |
| `airflow/airjet_airflow/` | `WasmOperator` (calls the runner) and `UvOperator` (cached uv venv per dependency set). Shipped via the Airflow `plugins/` folder. |
| `airflow/dags/airjet_bench.py` | One DAG per variant × workload, each fanning out 8 identical tasks. |
| `docker/`, `k8s/`, `scripts/` | Images, an Airflow 3 deployment running **LocalExecutor and KubernetesExecutor side by side** (Airflow 3 multi-executor, per-task `executor=`), build and deploy scripts. |
| `bench/` | `micro.py`: launch cost per mechanism without Airflow. `airflow_bench.py`: end-to-end timings from the Airflow metadata DB. |

### The runner's sandbox

Each task gets its own wasmtime `Store` (own linear memory, no shared state), sees only
`/data` (read-only) and a private per-run `/out`, has no network or other host access
(WASI preview1 has no sockets; nothing else is preopened), and is bounded by a memory
limit (`StoreLimits`) and a wall-clock timeout (epoch interruption). The runner pod itself
runs non-root with a read-only root filesystem and all capabilities dropped. Task start is
module instantiation: **~0.2–0.4 ms**.

### Getting Polars onto WASI

Polars does not officially support WASI (its wasm support targets emscripten/pyodide).
What it took, all documented in `pipeline/vendor/README.md`:

- a one-function patch to `polars-core` (`RAYON::block_on` references a thread pool that
  only exists on emscripten/native);
- `--cfg tokio_unstable` for the wasm target, because polars-core pulls tokio with `net`;
- using the **eager** API from `polars-core` + `polars-io` rather than the `polars` facade:
  `lazy`/`csv` pull in `polars-stream`, which force-enables cloud I/O (object_store,
  reqwest, mmap, file locks) that doesn't build for WASI;
- reading input into memory (no mmap on WASI);
- wasi-sdk's clang to cross-compile zstd/lz4 (C) for Parquet.

The wasm build is single-threaded (no threads in WASI preview1), so the job itself runs
~2–4x slower than native multi-threaded Polars. `wasm32-wasip1-threads` would be the
next step.

## Running it

Requirements: Docker, Rust (rustup), kubectl, and either kind or any cluster that can see
locally built images.

```bash
./scripts/build.sh                 # cargo builds (+ fetches wasi-sdk) and 3 images
./scripts/deploy.sh                # kind cluster "airjet" + everything in namespace airjet
CLUSTER=none ./scripts/deploy.sh   # ...or use the current kubectl context as-is

python3 bench/airflow_bench.py --runs 5 --out results/airflow
POD=$(kubectl -n airjet get pod -l app=airflow-scheduler -o name | head -1)
kubectl -n airjet cp bench/micro.py ${POD#pod/}:/tmp/micro.py
kubectl -n airjet exec $POD -- python /tmp/micro.py --n 10
```

RESULTS_PLACEHOLDER
