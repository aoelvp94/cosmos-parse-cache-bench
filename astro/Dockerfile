FROM astrocrpublic.azurecr.io/runtime:3.3-12

# dbt lives in its own virtualenv so it never conflicts with Airflow's dependencies (Cosmos docs pattern).
# The runtime ships Python 3.14; dbt-core 1.12 is pinned to a 3.12 interpreter fetched by uv.
RUN pip install --no-cache-dir uv && \
    uv venv /usr/local/airflow/dbt_venv --python 3.12 && \
    uv pip install --no-cache --python /usr/local/airflow/dbt_venv/bin/python "dbt-core==1.12.6" "dbt-duckdb==1.11.1"
ENV COSMOS_BENCH_DBT=/usr/local/airflow/dbt_venv/bin/dbt
