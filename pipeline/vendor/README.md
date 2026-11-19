# vendored polars-core 0.55.2

Unmodified crates.io `polars-core 0.55.2` except for `src/runtime.rs`,
`RAYON::block_on`: upstream only supports wasm via emscripten (where a 1-thread
rayon pool exists). On `wasm32-wasip1` there is no `THREAD_POOL` and no worker
threads, so the patch makes `block_on` run its closure inline.

Other WASI notes for this pipeline (no patches needed):
- tokio is pulled with `net`/`fs`/`rt-multi-thread`; `.cargo/config.toml` sets
  `--cfg tokio_unstable` for the wasm target, which tokio requires for those on WASI.
- the lazy/streaming engine (`polars` facade with `lazy`/`csv`) drags in
  `polars-stream`, which force-enables cloud I/O (`object_store`, `reqwest`, mmap,
  file locks). We use the eager API from `polars-core` + `polars-io` instead.
- WASI has no mmap: read CSV into memory and parse from a cursor.
- zstd/lz4 (C) need wasi-sdk to cross-compile; see `scripts/build.sh`.
