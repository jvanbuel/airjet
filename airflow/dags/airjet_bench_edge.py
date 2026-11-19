"""EdgeExecutor benchmark DAGs.

Kept in their own file so the task processes on the Edge workers, which parse this file,
don't import the kubernetes client that the KPO variants in airjet_bench.py need.
"""

from airjet_airflow.bench import VARIANTS, build_dags

build_dags([v for v in VARIANTS if v.endswith("_edge")], globals())
