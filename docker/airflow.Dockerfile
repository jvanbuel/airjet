# syntax=docker/dockerfile:1
FROM apache/airflow:3.3.2-python3.12

# EdgeExecutor (long-lived HTTP-polling workers) for the *_edge benchmark variants.
# Optional build secret `extra_ca`: a CA bundle for TLS-intercepting proxies.
RUN --mount=type=secret,id=extra_ca,required=false,uid=50000 \
    if [ -s /run/secrets/extra_ca ]; then export PIP_CERT=/run/secrets/extra_ca; fi \
 && pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-edge3==5.1.1"

# Operators ship via the plugins folder (on sys.path for every Airflow component
# and worker pod), so no package install is needed.
COPY --chown=airflow:root airflow/airjet_airflow /opt/airflow/plugins/airjet_airflow
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root dist/pipeline.py /opt/airjet/pipeline.py
COPY --chown=airflow:root dist/orders.csv /opt/airjet/data/orders.csv
COPY --chown=airflow:root dist/airjet-pipeline /opt/airjet/bin/airjet-pipeline

# Pre-warm both uv paths so the benchmark measures warm starts (see README for cold).
ENV UV_CACHE_DIR=/opt/airjet/uv-cache AIRJET_UV_ENV_DIR=/opt/airjet/uv-envs \
    PYTHONPATH=/opt/airflow/plugins
RUN --mount=type=secret,id=extra_ca,required=false,uid=50000 \
    if [ -s /run/secrets/extra_ca ]; then export SSL_CERT_FILE=/run/secrets/extra_ca UV_NATIVE_TLS=1; fi \
 && uv run --quiet --script /opt/airjet/pipeline.py noop \
 && python -c "from airjet_airflow import UvOperator; UvOperator(task_id='warm', script='/opt/airjet/pipeline.py').ensure_env()"
