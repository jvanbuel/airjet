-- airjet job queue, shared by the WasmQueueExecutor and airjet-runner workers.
create table if not exists airjet_job (
    id            bigserial primary key,
    queue         text not null default 'default',
    ti_id         uuid not null unique,
    ti_key        jsonb not null,
    token         text not null,
    api_url       text not null,
    spec          jsonb not null,
    log_path      text,
    state         text not null default 'queued',
    worker        text,
    created_at    timestamptz not null default now(),
    claimed_at    timestamptz,
    heartbeat_at  timestamptz,
    finished_at   timestamptz,
    exit_code     integer,
    stdout        text,
    stderr        text,
    error         text,
    timings       jsonb,
    reported      boolean not null default false
);
create index if not exists airjet_job_queued on airjet_job (queue, id) where state = 'queued';
create index if not exists airjet_job_unreported on airjet_job (id) where not reported and state <> 'queued' and state <> 'running';
