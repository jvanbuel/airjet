#!/usr/bin/env bash
# Create a kind cluster (if needed), load images and deploy airjet + Airflow.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-airjet}
TAG=${TAG:-dev}
# CLUSTER=none: use the current kubectl context as-is (e.g. k3s --docker, which
# sees locally built images directly). Otherwise create/use a kind cluster.
if [ "$CLUSTER" != none ]; then
  kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
  for img in airjet/pipeline-native airjet/runner airjet/airflow postgres:16-alpine; do
    [[ $img == *:* ]] || img=$img:$TAG
    kind load docker-image --name "$CLUSTER" "$img"
  done
fi
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/
kubectl -n airjet rollout status deploy/postgres --timeout=180s
kubectl -n airjet wait --for=condition=complete job/airflow-db-migrate --timeout=600s
kubectl -n airjet rollout restart deploy/airflow-scheduler deploy/airflow-dag-processor deploy/airflow-api-server deploy/airflow-edge-worker deploy/airjet-runner
for d in airjet-runner airflow-api-server airflow-scheduler airflow-dag-processor airflow-edge-worker; do
  kubectl -n airjet rollout status deploy/$d --timeout=300s
done
kubectl -n airjet get pods
