FROM apache/airflow:3.3.2-python3.12

# The base image runs as uid 50000 (airflow) and already treats
# /home/airflow/.local as its own VIRTUAL_ENV, so nothing here is installed as
# root or into Airflow's environment: a root-owned site-packages in the airflow
# user's home breaks later writes, and pinning packages next to Airflow's own
# can make the resolver fail at build time.
#
# Instead the pipeline dependencies and dbt each get their own venv, the same
# isolation Ledgerline uses for its Iceberg tooling. A bump on either side
# cannot disturb the scheduler.
USER airflow

COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt
RUN python -m venv /home/airflow/pipeline_env \
    && /home/airflow/pipeline_env/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

COPY --chown=airflow:airflow dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /home/airflow/dbt_env \
    && /home/airflow/dbt_env/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt \
    && rm /tmp/dbt-requirements.txt
