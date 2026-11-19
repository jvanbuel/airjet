"""Benchmark DAGs for every variant except the EdgeExecutor ones (see airjet_airflow.bench)."""

from airjet_airflow.bench import VARIANTS, build_dags

build_dags([v for v in VARIANTS if not v.endswith("_edge")], globals())
