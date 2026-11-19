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
| `airflow/airjet_airflow/` | `WasmOperator` (calls the runner), `UvOperator` (cached uv venv per dependency set) and `WasmExecutor` (runs WasmOperator tasks with no per-task process at all). Shipped via the Airflow `plugins/` folder. |
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

## Results

Single-node k3s on a 4-vCPU / 15 GB VM; images are local (no pulls). Every task runs the
same Polars job (200k rows → 480-row Parquet) or, for `noop`, starts and exits at once.
Raw data: `results/airflow/*.json`, `results/micro/micro.jsonl`.

### End-to-end in Airflow 3.3.2 (8 parallel tasks per DAG run, median of 5 runs)

| variant | pods / task | isolation | makespan `run` | makespan `noop` | per-task scheduled→done (`run`) | queued→started |
|---|---:|---|---:|---:|---:|---:|
| KPO + KubernetesExecutor (**baseline**) | 2 | container | 27.7 s | 27.6 s | 25.6 s | 7.4 s |
| WasmOperator + KubernetesExecutor | 1 | wasm sandbox | 23.3 s | 21.0 s | 20.5 s | 7.3 s |
| KPO + LocalExecutor | 1 | container | 6.9 s | 6.4 s | 6.0 s | 0.07 s |
| BashOperator + `uv run --script` | 0 | deps only | 2.40 s | 1.19 s | 1.45 s | 0.08 s |
| UvOperator (cached venv) | 0 | deps only (+rlimits) | 2.29 s | 1.26 s | 1.28 s | 0.05 s |
| **WasmOperator + LocalExecutor (airjet)** | **0** | **wasm sandbox** | **1.19 s** | **1.08 s** | **0.87 s** | 0.06 s |
| BashOperator + native binary (reference) | 0 | none | 1.22 s | 1.15 s | 0.79 s | 0.05 s |

### Launch cost per mechanism, no Airflow (sequential, p50 of 10, from inside the cluster)

| mechanism | `noop` | `run` |
|---|---:|---:|
| native exec | 2.8 ms | 62 ms |
| wasm on airjet-runner (HTTP round trip incl.) | **1.5 ms** | 169 ms |
| UvOperator-style: python from cached venv | 23 ms | 266 ms |
| `uv run --script` (warm cache) | 35 ms | 276 ms |
| create pod → Succeeded → delete | 2,185 ms | 2,253 ms |

### What this says

1. **Pods are the cost, not the work.** The baseline takes the same 27.6 s whether
   tasks do the Polars job or exit immediately. Per task it pays ~7 s before the task
   even starts (worker pod) and then another pod for KPO. On a real cluster (image pulls,
   busier API server, node autoscaling) this only gets worse; these numbers are a
   best case for the baseline.
2. **The KubernetesExecutor worker pod is the bigger half.** Dropping it (KPO on
   LocalExecutor, 6.9 s) helps far more than dropping the KPO pod (Wasm on
   KubernetesExecutor, 23.3 s): the worker pod boots a full Airflow Python process and
   parses the DAG before running anything.
3. **airjet removes both pods: 27.7 s → 1.19 s (~23x) with a real sandbox per task.**
   It matches the unisolated native binary, even though the WASI build of Polars is
   single-threaded and ~2.7x slower per job (169 vs 62 ms). Instantiation is ~0.2 ms;
   what remains per task (~0.45 s on `noop`) is Airflow's own task-process overhead.
4. **uv is nearly as fast to launch, but it is not a sandbox.** Warm-cache `uv run`
   costs ~35 ms, and the UvOperator saves ~12 ms of that by skipping resolution and the
   shell. In Airflow both are within ~10% of airjet on `noop`; on `run` they're ~2x
   slower here mainly because eight multi-threaded Python Polars processes contend for
   4 cores (the wasm runner caps concurrency at 4). uv gives dependency isolation only:
   the task runs as the worker user with the worker's filesystem, network and secrets.
   A cold uv cache also means downloading wheels (Polars is ~50 MB) on first use per
   worker.

So: if you trust your task code and just need per-task dependencies, a `UvOperator` on
LocalExecutor/Celery is the cheapest change. If you need tasks isolated from the worker
and each other (multi-tenant, untrusted or third-party code, least-privilege data access),
wasm on a shared runner gives you that for roughly the same cost as a plain subprocess.

## WasmExecutor: no task process either

With airjet on the LocalExecutor there are no pods per task, but every task still gets
a Python task process: fork, load the DAG file, render templates, talk to the Execution
API, then make one HTTP call to the runner. That is ~0.4 s per task, for a wasm
instantiation that takes ~0.2 ms.

`airjet_airflow/executor.py` is a `LocalExecutor` subclass that short-circuits this for
`WasmOperator` tasks. On a thread pool inside the scheduler it:

1. reads the task definition from the **serialized DAG version** in the metadata DB
   (no user DAG code is imported into the scheduler);
2. marks the task running through the Execution API, the same call the task
   supervisor makes;
3. renders the templated fields (sandboxed Jinja, small context: `ds`, `ts`,
   `logical_date`, `run_id`, `task_id`, `params`, ...) and POSTs them to airjet-runner,
   sending heartbeats while it waits;
4. writes the runner's stdout/stderr into the normal task log file and reports success,
   `up_for_retry` or failed through the Execution API.

Any other task routed to it falls back to regular LocalExecutor worker processes, so it
can be the default executor. Enable it as an Airflow 3 multi-executor entry and route
tasks with `executor="WasmExecutor"`:

```
AIRFLOW__CORE__EXECUTOR="LocalExecutor,KubernetesExecutor,WasmExecutor:airjet_airflow.executor.WasmExecutor"
```

`airflow/dags/airjet_wasmexec_demo.py` exercises the edge cases, all verified: a
templated output path renders (`orders-2026-11-19-aggregate.parquet`), a task with a
missing input fails, is retried once and ends `failed` with the guest's stderr in both
attempt logs, and a `BashOperator` on the same executor runs in a normal task process.

### Results (same benchmark, re-run in one session for a fair comparison)

| variant | per-task scheduled→done `run` | per-task `noop` | task runtime `noop` | makespan `run` | makespan `noop` |
|---|---:|---:|---:|---:|---:|
| WasmOperator + LocalExecutor | 1.02 s | 0.50 s | 420 ms | 1.96 s | 1.15 s |
| **WasmOperator + WasmExecutor** | **0.51 s** | **0.15 s** | **96 ms** | **1.26 s** | 1.19 s |
| native binary + LocalExecutor (reference) | 0.77 s | 0.55 s | 454 ms | 1.39 s | 1.17 s |

Raw data: `results/airflow-wasmexec/`.

- **Per-task overhead drops ~3x** (0.50 → 0.15 s on `noop`). What remains is two
  Execution API calls plus the runner round trip; the task process is gone. Sandboxed
  wasm tasks now finish faster than an unsandboxed native binary under LocalExecutor.
- **The makespan floor is now the scheduler itself.** For every variant, the DAG run is
  marked done 0.6-1.0 s after its last task finishes, because that waits for the
  scheduler's next loop. Once tasks are this cheap, scheduler tuning
  (`scheduler_idle_sleep_time` and friends, not tried here) is the next lever, not the
  executor.
- **Concurrency gets cheap.** A running task is a thread blocked on HTTP, not a ~100 MB
  Python process, so one scheduler can drive far more concurrent wasm tasks; the limit
  becomes runner capacity, which scales by adding runner replicas.

Prototype limits: no XCom push, no `on_*_callback`s, no outlets/assets, a reduced
template context, and logs are written locally by the scheduler (fine for
LocalExecutor-style log serving, but remote logging isn't wired in). It also relies on
Airflow internals (`LocalExecutor` internals, the Execution API client, `DagVersion`),
which change between Airflow versions.

## Caveats

- One small node, local images, one runner replica with `AIRJET_MAX_CONCURRENCY=4`.
  `WORKER_PODS_CREATION_BATCH_SIZE` was raised to 32 so the baseline isn't throttled
  (the default of 1 would make it look worse).
- The cluster in these runs is k3s using the host Docker. Nested containers (kind) don't
  work in the VM the benchmark ran in, and the VM lacks `CAP_SYS_RESOURCE`, so a runc
  wrapper zeroed negative `oom_score_adj` values. Neither affects launch latency. The
  repo's `scripts/deploy.sh` targets kind (or any context with `CLUSTER=none`).
- Airflow's `start_date` is set before the KubernetesExecutor worker has finished
  booting, so for the `*_k8sexec` variants part of the worker start-up shows up as task
  runtime. Compare makespans and per-task totals, not the split.
- WASI-specific limits of the POC: Polars runs single-threaded and eager-only (no
  lazy/streaming engine, see `pipeline/vendor/README.md`), inputs are read into memory,
  and modules are baked into the runner image. A production version would pull modules
  from an OCI registry/object store, mount data via object storage (WASI preview2 / HTTP)
  instead of preopened dirs, use a deferrable operator for long tasks, and autoscale
  runners on queue depth.
- Runner limits verified: an infinite loop is killed at the timeout (epoch
  interruption); a module growing memory past `memory_limit_mb` traps; files outside
  `/data` and `/out` are invisible; the runner keeps serving after each.
