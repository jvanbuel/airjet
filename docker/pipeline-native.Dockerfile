# Baseline task image for the KubernetesPodOperator variants.
FROM debian:bookworm-slim
COPY dist/airjet-pipeline /usr/local/bin/airjet-pipeline
COPY dist/orders.csv /data/orders.csv
USER 65534
ENTRYPOINT ["airjet-pipeline"]
