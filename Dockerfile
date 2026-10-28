FROM apache/airflow:3.3.2-python3.12

# Task-runtime dependencies go into Airflow's own environment (root install);
# dbt gets an isolated venv owned by the airflow user, the same separation
# Ledgerline uses for its Iceberg tooling.
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

USER airflow
COPY --chown=airflow:airflow dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /home/airflow/dbt_env \
    && /home/airflow/dbt_env/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt \
    && rm /tmp/dbt-requirements.txt
