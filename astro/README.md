# Astro CLI project for the Cosmos parse/cache bench

Local Astronomer Runtime 3.3 project used to show the same synthetic dbt projects
under the Astro CLI (`astro dev parse` / `astro dev start`). It is not a
deployment template and is not affiliated with any employer.

See the repository root `README.md` for how to generate the dbt projects, what
DAGs are defined, and how this relates to the plain Airflow DagBag harness.

- `dags/cosmos_bench_astro.py` — one `DbtDag` per generated project under
  `dags/dbt/<name>/`, in both `LoadMode.DBT_LS` and `LoadMode.DBT_MANIFEST`.
- `dags/dbt/profiles.yml` — dbt-duckdb, no credentials.
- Generated projects (`dags/dbt/n<N>/`) are gitignored; create them with
  `just astro-projects` or `just astro-parse` from the repo root.
- The Dockerfile installs dbt-core 1.12 + dbt-duckdb into a separate 3.12
  virtualenv and points `COSMOS_BENCH_DBT` at it (Cosmos docs pattern).

Docker on this machine is reached at `DOCKER_HOST=tcp://127.0.0.1:2375`.
