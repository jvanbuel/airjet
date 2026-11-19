FROM debian:bookworm-slim
COPY dist/airjet-runner /usr/local/bin/airjet-runner
COPY dist/airjet-pipeline.wasm /modules/airjet-pipeline.wasm
# WAT text is accepted too: an infinite loop for timeout/cancellation demos.
COPY runner/modules/spin.wat /modules/spin.wasm
COPY dist/orders.csv /data/orders.csv
# AOT-compile modules at build time so pods start serving immediately.
RUN airjet-runner precompile /modules && mkdir -p /tmp/airjet-out && chown 65534 /tmp/airjet-out
USER 65534
ENV AIRJET_MODULE_DIR=/modules AIRJET_DATA_DIR=/data AIRJET_OUT_DIR=/tmp/airjet-out
EXPOSE 8080
ENTRYPOINT ["airjet-runner"]
