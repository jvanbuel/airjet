//! Pull-based worker mode: "Celery for wasm jobs", with Postgres as the broker.
//!
//! The `WasmQueueExecutor` (Airflow side) inserts one row per task into `airjet_job` and
//! `NOTIFY`s the queue's channel. Each runner replica:
//!
//! * `LISTEN`s for new work (no polling delay) and claims jobs from the queues it serves
//!   with `FOR UPDATE SKIP LOCKED`, at most as many as it has free slots (backpressure:
//!   jobs wait in the table, not in a runner);
//! * drives the task lifecycle itself through Airflow's Execution API, exactly like the
//!   Python task supervisor does: `run` (enter running), periodic heartbeats, terminal
//!   state (success / up_for_retry / failed);
//! * stops the guest if a heartbeat says the task should no longer run (cleared or
//!   marked failed in the UI);
//! * writes stdout/stderr/timings back to the job row, where the executor picks them up
//!   for the task log.
//!
//! Env: AIRJET_QUEUE_DSN (Postgres DSN), AIRJET_QUEUES (comma-separated, default
//! "default"), AIRJET_WORKER_NAME (default $HOSTNAME), AIRJET_HEARTBEAT_SECS (default 30),
//! AIRJET_AIRFLOW_API_VERSION (Execution API version header).

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};
use tokio_postgres::{AsyncMessage, Client, NoTls};

use crate::{App, RunRequest, execute};

/// Shared with the Python WasmQueueExecutor, which creates the same table.
pub const SCHEMA: &str = include_str!("../../airflow/airjet_airflow/queue_schema.sql");
pub const CHANNEL: &str = "airjet_jobs";

pub struct QueueConfig {
    dsn: String,
    queues: Vec<String>,
    worker: String,
    heartbeat: Duration,
    api_version: String,
}

impl QueueConfig {
    pub fn from_env(dsn: String) -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let host = env("HOSTNAME", "airjet-runner");
        QueueConfig {
            dsn,
            queues: env("AIRJET_QUEUES", "default")
                .split(',')
                .map(|q| q.trim().to_string())
                .filter(|q| !q.is_empty())
                .collect(),
            worker: env("AIRJET_WORKER_NAME", &host),
            heartbeat: Duration::from_secs_f64(env("AIRJET_HEARTBEAT_SECS", "30").parse().unwrap_or(30.0)),
            api_version: env("AIRJET_AIRFLOW_API_VERSION", "2026-06-30"),
        }
    }
}

struct Job {
    id: i64,
    ti_id: String,
    token: String,
    api_url: String,
    spec: RunRequest,
}

struct Ctx {
    app: Arc<App>,
    db: Client,
    http: reqwest::Client,
    cfg: QueueConfig,
    /// Signalled on NOTIFY and whenever a job finishes (a slot frees up).
    wake: Notify,
}

pub async fn run_worker(app: Arc<App>, cfg: QueueConfig) -> Result<()> {
    // Connection 1: queries. Connection 2: LISTEN (notifications are delivered through
    // the connection future, so it needs its own driver loop).
    let (db, conn) = tokio_postgres::connect(&cfg.dsn, NoTls).await.context("connecting to queue DB")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("queue DB connection: {e}");
            std::process::exit(1);
        }
    });
    db.batch_execute(SCHEMA).await.context("creating queue schema")?;

    let ctx = Arc::new(Ctx {
        app,
        db,
        http: reqwest::Client::builder().no_proxy().build()?,
        cfg,
        wake: Notify::new(),
    });

    let (listener, mut lconn) = tokio_postgres::connect(&ctx.cfg.dsn, NoTls).await?;
    let ctx2 = ctx.clone();
    tokio::spawn(async move {
        let mut messages = stream::poll_fn(move |cx| lconn.poll_message(cx));
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(AsyncMessage::Notification(_)) => ctx2.wake.notify_one(),
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("queue LISTEN connection: {e}");
                    std::process::exit(1);
                }
            }
        }
    });
    listener.batch_execute(&format!("LISTEN {CHANNEL}")).await?;
    tracing::info!(queues = ?ctx.cfg.queues, worker = %ctx.cfg.worker, "airjet queue worker started");

    loop {
        // Take every free slot, claim at most that many jobs, give back the rest.
        let mut permits = Vec::new();
        while let Ok(p) = ctx.app.permits.clone().try_acquire_owned() {
            permits.push(p);
        }
        let claimed = if permits.is_empty() { Vec::new() } else { claim(&ctx, permits.len()).await? };
        let idle = claimed.len() < permits.len();
        for job in claimed {
            let permit = permits.pop().unwrap();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let id = job.id;
                if let Err(e) = process(&ctx, job).await {
                    tracing::error!(job = id, "job failed in worker: {e:#}");
                    let _ = ctx
                        .db
                        .execute(
                            "update airjet_job set state='error', error=$2, finished_at=now() where id=$1",
                            &[&id, &format!("{e:#}")],
                        )
                        .await;
                }
                drop(permit);
                ctx.wake.notify_one();
            });
        }
        drop(permits);
        if idle || ctx.app.permits.available_permits() == 0 {
            // Fallback poll in case a NOTIFY was missed (e.g. reconnect).
            let _ = tokio::time::timeout(Duration::from_secs(5), ctx.wake.notified()).await;
        }
    }
}

async fn claim(ctx: &Ctx, n: usize) -> Result<Vec<Job>> {
    let rows = ctx
        .db
        .query(
            "update airjet_job set state='running', worker=$1, claimed_at=now(), heartbeat_at=now()
             where id in (select id from airjet_job where state='queued' and queue = any($2)
                          order by id for update skip locked limit $3)
             returning id, ti_id::text, token, api_url, spec",
            &[&ctx.cfg.worker, &ctx.cfg.queues, &(n as i64)],
        )
        .await?;
    rows.into_iter()
        .map(|r| {
            Ok(Job {
                id: r.get(0),
                ti_id: r.get(1),
                token: r.get(2),
                api_url: r.get(3),
                spec: serde_json::from_value(r.get::<_, Value>(4))?,
            })
        })
        .collect()
}

/// Minimal client for the parts of Airflow's Execution API a task runner needs.
struct ExecApi {
    ctx: Arc<Ctx>,
    base: String,
    ti: String,
    token: Mutex<String>,
}

impl ExecApi {
    async fn call(&self, method: reqwest::Method, path: &str, body: Value) -> Result<reqwest::Response> {
        let token = self.token.lock().await.clone();
        let resp = self
            .ctx
            .http
            .request(method, format!("{}task-instances/{}/{path}", self.base, self.ti))
            .bearer_auth(token)
            .header("airflow-api-version", &self.ctx.cfg.api_version)
            .json(&body)
            .send()
            .await?;
        if let Some(t) = resp.headers().get("Refreshed-API-Token").and_then(|v| v.to_str().ok()) {
            *self.token.lock().await = t.to_string();
        }
        Ok(resp)
    }

    async fn ok(&self, method: reqwest::Method, path: &str, body: Value) -> Result<reqwest::Response> {
        let resp = self.call(method, path, body).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(anyhow!("execution API {path}: {status} {}", resp.text().await.unwrap_or_default()));
        }
        Ok(resp)
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

async fn process(ctx: &Arc<Ctx>, job: Job) -> Result<()> {
    let t0 = Instant::now();
    // Shared with the heartbeat task: the Execution API swaps the workload token for an
    // execution token on the first call (Refreshed-API-Token), and both must use it.
    let api = Arc::new(ExecApi {
        ctx: ctx.clone(),
        base: if job.api_url.ends_with('/') { job.api_url.clone() } else { format!("{}/", job.api_url) },
        ti: job.ti_id.clone(),
        token: Mutex::new(job.token.clone()),
    });
    let pid = std::process::id();
    let host = ctx.cfg.worker.clone();

    // 1. Enter running. The API can refuse (e.g. the task was cleared meanwhile).
    let run_ctx: Value = api
        .ok(
            reqwest::Method::PATCH,
            "run",
            json!({"state": "running", "hostname": host, "unixname": "airjet", "pid": pid, "start_date": now()}),
        )
        .await?
        .json()
        .await?;
    let should_retry = run_ctx.get("should_retry").and_then(Value::as_bool).unwrap_or(false);
    let start_ms = t0.elapsed().as_secs_f64() * 1e3;

    // 2. Run the module, heartbeating meanwhile. A 404/409/410 heartbeat means
    //    "stop": the guest is interrupted at the next epoch tick.
    let cancel = Arc::new(AtomicBool::new(false));
    let done = Arc::new(Notify::new());
    let hb = {
        let (cancel, done, api) = (cancel.clone(), done.clone(), api.clone());
        let ctx = ctx.clone();
        let job_id = job.id;
        let host = host.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = done.notified() => return,
                    _ = tokio::time::sleep(ctx.cfg.heartbeat) => {}
                }
                match api.call(reqwest::Method::PUT, "heartbeat", json!({"hostname": host, "pid": pid})).await {
                    Ok(r) if matches!(r.status().as_u16(), 404 | 409 | 410) => {
                        tracing::warn!(job = job_id, status = %r.status(), "task no longer running; cancelling");
                        cancel.store(true, Ordering::Relaxed);
                        return;
                    }
                    Ok(_) => {
                        let _ = ctx.db.execute("update airjet_job set heartbeat_at=now() where id=$1", &[&job_id]).await;
                    }
                    Err(e) => tracing::warn!(job = job_id, "heartbeat failed: {e}"),
                }
            }
        })
    };
    let app = ctx.app.clone();
    let spec = job.spec.clone();
    let cancel2 = cancel.clone();
    let res = tokio::task::spawn_blocking(move || execute(&app, spec, 0.0, Some(cancel2))).await?;
    done.notify_one();
    let _ = hb.await;

    // 3. Report the terminal state, unless the server already decided (cancelled).
    let (state, exit_code, stdout, stderr, error, timings) = match res {
        Ok(r) => {
            let cancelled = cancel.load(Ordering::Relaxed);
            let ok = r.exit_code == 0 && r.error.is_none();
            let state = if cancelled {
                "cancelled"
            } else if ok {
                "success"
            } else if should_retry {
                "up_for_retry"
            } else {
                "failed"
            };
            let timings = json!({"start_api_ms": start_ms, "instantiate_ms": r.instantiate_ms, "exec_ms": r.exec_ms,
                                 "out_dir": r.out_dir});
            let err = r.error.clone().or_else(|| (!ok).then(|| format!("exit={}", r.exit_code)));
            (state, Some(r.exit_code), r.stdout, r.stderr, err, timings)
        }
        Err(e) => {
            let state = if should_retry { "up_for_retry" } else { "failed" };
            (state, None, String::new(), String::new(), Some(format!("{e:#}")), json!({"start_api_ms": start_ms}))
        }
    };
    let body = match state {
        "success" => Some(json!({"state": "success", "end_date": now(), "task_outlets": [], "outlet_events": []})),
        "up_for_retry" => Some(json!({"state": "up_for_retry", "end_date": now(), "retry_reason": error})),
        "failed" => Some(json!({"state": "failed", "end_date": now()})),
        _ => None,
    };
    if let Some(body) = body {
        api.ok(reqwest::Method::PATCH, "state", body).await?;
    }
    let mut timings = timings;
    timings["total_ms"] = json!(t0.elapsed().as_secs_f64() * 1e3);
    ctx.db
        .execute(
            "update airjet_job set state=$2, exit_code=$3, stdout=$4, stderr=$5, error=$6, timings=$7,
             finished_at=now() where id=$1",
            &[&job.id, &state, &exit_code, &stdout, &stderr, &error, &timings],
        )
        .await?;
    tracing::info!(job = job.id, state, ms = t0.elapsed().as_secs_f64() * 1e3, "job done");
    Ok(())
}
