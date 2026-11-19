#!/usr/bin/env bash
# Build all artifacts into dist/ and the three container images.
#   dist/airjet-pipeline        native Rust/Polars binary (KPO image + reference variant)
#   dist/airjet-pipeline.wasm   same program for wasm32-wasip1 (airjet-runner)
#   dist/airjet-runner          wasmtime-based task runner service
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WASI_SDK_VERSION=${WASI_SDK_VERSION:-27}
WASI_SDK=${WASI_SDK:-$ROOT/.cache/wasi-sdk-$WASI_SDK_VERSION.0-x86_64-linux}

if [ ! -d "$WASI_SDK" ]; then
  # polars-arrow links zstd/lz4 (C); cross-compiling them to WASI needs wasi-sdk's clang + sysroot.
  mkdir -p .cache
  curl -fsSL "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-$WASI_SDK_VERSION/wasi-sdk-$WASI_SDK_VERSION.0-x86_64-linux.tar.gz" | tar xz -C .cache
fi
rustup target add wasm32-wasip1 >/dev/null

export CC_wasm32_wasip1=$WASI_SDK/bin/clang
export AR_wasm32_wasip1=$WASI_SDK/bin/llvm-ar
export CFLAGS_wasm32_wasip1="--sysroot=$WASI_SDK/share/wasi-sysroot"

(cd pipeline && cargo build --release && cargo build --release --target wasm32-wasip1)
(cd runner && cargo build --release)

mkdir -p dist
cp pipeline/target/release/airjet-pipeline dist/
cp pipeline/target/wasm32-wasip1/release/airjet-pipeline.wasm dist/
cp runner/target/release/airjet-runner dist/
cp pipeline_py/pipeline.py dist/
[ -f dist/orders.csv ] || dist/airjet-pipeline gen dist/orders.csv "${AIRJET_ROWS:-200000}"
ls -la dist

TAG=${TAG:-dev}
docker build -f docker/pipeline-native.Dockerfile -t airjet/pipeline-native:$TAG .
docker build -f docker/runner.Dockerfile -t airjet/runner:$TAG .
# EXTRA_CA_FILE: CA bundle for TLS-intercepting proxies (only used to pre-warm uv).
docker build ${EXTRA_CA_FILE:+--secret id=extra_ca,src=$EXTRA_CA_FILE} -f docker/airflow.Dockerfile -t airjet/airflow:$TAG .
