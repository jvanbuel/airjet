//! airjet-runner: a long-lived service that runs WASI (preview1) task modules in-process.
//!
//! Instead of one (or two) pods per Airflow task, a small Deployment of runners
//! receives `POST /v1/run` requests from the `WasmOperator` and executes each task in
//! a fresh wasmtime `Store`: its own linear memory, no ambient filesystem/network
//! access, only the directories we preopen, plus CPU-time (epoch) and memory limits.
//!
//! Modules are loaded once at startup from `AIRJET_MODULE_DIR` (`<name>.wasm`),
//! AOT-compiled (cached as `<name>.cwasm`), and kept as `InstancePre`, so a task
//! start is instantiation only, typically well under a millisecond.
//!
//! Env:
//!   AIRJET_LISTEN        (default 0.0.0.0:8080)
//!   AIRJET_MODULE_DIR    (default /modules)
//!   AIRJET_DATA_DIR      host dir preopened read-only as /data (default /data)
//!   AIRJET_OUT_DIR       host dir; each run gets <out>/<run_id> preopened rw as /out
//!   AIRJET_MAX_CONCURRENCY  concurrent tasks (default: available CPUs)
//!
//! CLI: `airjet-runner precompile [module_dir]` writes .cwasm files (used at image build).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use wasmtime::{Config, Engine, InstancePre, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{
    FsPerms, I32Exit, WasiCtxBuilder,
    p1::{self, WasiP1Ctx},
    p2::pipe::MemoryOutputPipe,
};

/// Epoch tick: timeouts are enforced with this granularity.
const EPOCH_TICK: Duration = Duration::from_millis(50);
const MAX_CAPTURE: usize = 16 << 20;
static RUN_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct TaskState {
    wasi: WasiP1Ctx,
    limits: StoreLimits,
}

struct App {
    engine: Engine,
    modules: HashMap<String, InstancePre<TaskState>>,
    data_dir: PathBuf,
    out_dir: PathBuf,
    permits: Arc<Semaphore>,
    max_concurrency: usize,
}

#[derive(Deserialize)]
struct RunRequest {
    module: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    /// Used to name the per-run output directory; sanitized.
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default = "default_timeout")]
    timeout_secs: f64,
    #[serde(default = "default_mem")]
    memory_limit_mb: usize,
}
fn default_timeout() -> f64 {
    3600.0
}
fn default_mem() -> usize {
    2048
}

#[derive(Serialize)]
struct RunResponse {
    exit_code: i32,
    stdout: String,
    stderr: String,
    /// Time spent waiting for a concurrency slot.
    queue_ms: f64,
    /// Store + WASI context creation + instantiation.
    instantiate_ms: f64,
    /// Guest `_start` execution.
    exec_ms: f64,
    out_dir: String,
    error: Option<String>,
}

fn engine() -> Result<Engine> {
    let mut cfg = Config::new();
    cfg.epoch_interruption(true);
    Engine::new(&cfg).map_err(|e| anyhow!("{e:?}"))
}

/// Load `<name>.wasm`, preferring a compatible cached `<name>.cwasm`.
fn load_module(engine: &Engine, wasm: &Path) -> Result<Module> {
    let cwasm = wasm.with_extension("cwasm");
    if cwasm.exists() {
        // SAFETY: the .cwasm files are produced by this binary (`precompile`) at image
        // build time; wasmtime additionally checks engine/version compatibility.
        match unsafe { Module::deserialize_file(engine, &cwasm) } {
            Ok(m) => return Ok(m),
            Err(e) => tracing::warn!("ignoring stale {}: {e:?}", cwasm.display()),
        }
    }
    let t = Instant::now();
    let bytes = std::fs::read(wasm).with_context(|| format!("reading {}", wasm.display()))?;
    let module = Module::new(engine, &bytes).map_err(|e| anyhow!("{e:?}"))?;
    tracing::info!("compiled {} in {:?}", wasm.display(), t.elapsed());
    if let Ok(ser) = module.serialize() {
        let _ = std::fs::write(&cwasm, ser);
    }
    Ok(module)
}

fn wasm_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading module dir {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wasm"))
        .collect();
    v.sort();
    Ok(v)
}

fn sanitize(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' })
        .collect::<String>()
        .trim_start_matches('.')
        .to_string()
}

/// Run one task to completion on the current (blocking) thread.
fn execute(app: &App, req: RunRequest, queue_ms: f64) -> Result<RunResponse> {
    let t0 = Instant::now();
    let pre = app
        .modules
        .get(&req.module)
        .ok_or_else(|| anyhow!("unknown module {:?}", req.module))?;

    let run_id = sanitize(req.run_id.as_deref().unwrap_or("run"));
    let seq = RUN_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let run_id = format!("{run_id}-{}-{seq}", std::process::id());
    let out_dir = app.out_dir.join(&run_id);
    std::fs::create_dir_all(&out_dir)?;

    let stdout = MemoryOutputPipe::new(MAX_CAPTURE);
    let stderr = MemoryOutputPipe::new(MAX_CAPTURE);
    let mut argv = vec![req.module.clone()];
    argv.extend(req.args);
    let env: Vec<(String, String)> = req.env.into_iter().collect();

    let mut b = WasiCtxBuilder::new();
    b.args(&argv)
        .envs(&env)
        .stdout(stdout.clone())
        .stderr(stderr.clone())
        .preopened_dir(&out_dir, "/out", FsPerms::ReadWrite)
        .map_err(|e| anyhow!("{e:?}"))?;
    if app.data_dir.is_dir() {
        b.preopened_dir(&app.data_dir, "/data", FsPerms::ReadOnly)
            .map_err(|e| anyhow!("{e:?}"))?;
    }
    let state = TaskState {
        wasi: b.build_p1(),
        limits: StoreLimitsBuilder::new()
            .memory_size(req.memory_limit_mb << 20)
            .instances(1)
            .build(),
    };
    let mut store = Store::new(&app.engine, state);
    store.limiter(|s| &mut s.limits);
    let ticks = (req.timeout_secs / EPOCH_TICK.as_secs_f64()).ceil().max(1.0) as u64;
    store.set_epoch_deadline(ticks);

    let instance = pre.instantiate(&mut store).map_err(|e| anyhow!("{e:?}"))?;
    let start = instance
        .get_typed_func::<(), ()>(&mut store, "_start")
        .map_err(|e| anyhow!("{e:?}"))?;
    let instantiate_ms = t0.elapsed().as_secs_f64() * 1e3;

    let t1 = Instant::now();
    let result = start.call(&mut store, ());
    let exec_ms = t1.elapsed().as_secs_f64() * 1e3;

    let (exit_code, error) = match result {
        Ok(()) => (0, None),
        Err(e) => match e.downcast_ref::<I32Exit>() {
            Some(I32Exit(code)) => (*code, None),
            None => {
                let msg = match e.downcast_ref::<wasmtime::Trap>() {
                    Some(wasmtime::Trap::Interrupt) => {
                        format!("timeout after {}s", req.timeout_secs)
                    }
                    _ => format!("{e:?}"),
                };
                (-1, Some(msg))
            }
        },
    };
    drop(store);
    let s = |p: &MemoryOutputPipe| String::from_utf8_lossy(&p.contents()).into_owned();
    Ok(RunResponse {
        exit_code,
        stdout: s(&stdout),
        stderr: s(&stderr),
        queue_ms,
        instantiate_ms,
        exec_ms,
        out_dir: out_dir.display().to_string(),
        error,
    })
}

async fn run(
    State(app): State<Arc<App>>,
    Json(req): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, String)> {
    let t = Instant::now();
    let permit = app
        .permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    let queue_ms = t.elapsed().as_secs_f64() * 1e3;
    let module = req.module.clone();
    let app2 = app.clone();
    let res = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        execute(&app2, req, queue_ms)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    match res {
        Ok(r) => {
            tracing::info!(
                module,
                exit = r.exit_code,
                inst_ms = r.instantiate_ms,
                exec_ms = r.exec_ms,
                "task done"
            );
            Ok(Json(r))
        }
        Err(e) => Err((StatusCode::BAD_REQUEST, format!("{e:#}"))),
    }
}

#[derive(Serialize)]
struct Info {
    modules: Vec<String>,
    max_concurrency: usize,
    available: usize,
}

async fn info(State(app): State<Arc<App>>) -> Json<Info> {
    let mut modules: Vec<_> = app.modules.keys().cloned().collect();
    modules.sort();
    Json(Info {
        modules,
        max_concurrency: app.max_concurrency,
        available: app.permits.available_permits(),
    })
}

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let args: Vec<String> = std::env::args().collect();
    let module_dir = PathBuf::from(env_or("AIRJET_MODULE_DIR", "/modules"));
    let engine = engine()?;

    if args.get(1).map(String::as_str) == Some("precompile") {
        let dir = args.get(2).map(PathBuf::from).unwrap_or(module_dir);
        for w in wasm_files(&dir)? {
            load_module(&engine, &w)?;
            println!("precompiled {}", w.display());
        }
        return Ok(());
    }

    let mut linker: Linker<TaskState> = Linker::new(&engine);
    p1::add_to_linker_sync(&mut linker, |s| &mut s.wasi).map_err(|e| anyhow!("{e:?}"))?;
    let mut modules = HashMap::new();
    for w in wasm_files(&module_dir)? {
        let name = w.file_stem().unwrap().to_string_lossy().into_owned();
        let m = load_module(&engine, &w)?;
        let pre = linker.instantiate_pre(&m).map_err(|e| anyhow!("{e:?}"))?;
        tracing::info!("loaded module {name}");
        modules.insert(name, pre);
    }

    // Epoch ticker drives per-task timeouts.
    let e2 = engine.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(EPOCH_TICK);
            e2.increment_epoch();
        }
    });

    let max_concurrency: usize = env_or("AIRJET_MAX_CONCURRENCY", "0").parse().unwrap_or(0);
    let max_concurrency = if max_concurrency == 0 {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    } else {
        max_concurrency
    };
    let app = Arc::new(App {
        engine,
        modules,
        data_dir: env_or("AIRJET_DATA_DIR", "/data").into(),
        out_dir: env_or("AIRJET_OUT_DIR", "/tmp/airjet-out").into(),
        permits: Arc::new(Semaphore::new(max_concurrency)),
        max_concurrency,
    });

    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/modules", get(info))
        .route("/v1/run", post(run))
        .with_state(app);
    let addr = env_or("AIRJET_LISTEN", "0.0.0.0:8080");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("airjet-runner listening on {addr} (max_concurrency={max_concurrency})");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
