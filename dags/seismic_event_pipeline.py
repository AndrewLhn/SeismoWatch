"""DAG shell: snapshot, versioned load, dbt models, and the freshness gate.

All ingestion logic lives in seismic_pipeline_run.py, which imports no Airflow
modules and is covered by unit tests. The tasks here are deliberately thin.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

RUN_INGESTION = (
    'env -u PYTHONPATH -u LD_PRELOAD PYTHONNOUSERSITE=1 '
    '/home/airflow/pipeline_env/bin/python '
    '/opt/airflow/dags/seismic_pipeline_run.py --ds "{{ ds }}"'
)
# dbt artifacts go to /tmp: the project directory is a bind mount owned by the
# host user, and the container's airflow user must not depend on its write bits.
# In dbt-core 1.11 the path flags are subcommand-level only (the global
# --target-path was removed), so they must follow `build` / `source freshness`.
DBT = "cd /opt/airflow/dbt && /home/airflow/dbt_env/bin/dbt"
DBT_PATHS = "--target-path /tmp/dbt/target --log-path /tmp/dbt/logs"

with DAG(
    dag_id="seismic_event_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=20),
    },
    max_active_runs=1,
    tags=["seismic", "usgs", "geospatial"],
    description="Snapshots, versions, and models USGS seismic-event observations.",
) as dag:
    ingest = BashOperator(task_id="ingest_usgs_observations", bash_command=RUN_INGESTION)
    transform = BashOperator(
        task_id="build_analytics_models",
        bash_command=f"{DBT} build --profiles-dir . {DBT_PATHS}",
    )
    # Freshness is a gate, not a report: if the landing zone stops receiving
    # versions, the run fails and the runbook applies (docs/runbooks/).
    freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=f"{DBT} source freshness --profiles-dir . {DBT_PATHS}",
    )

    ingest >> transform >> freshness
