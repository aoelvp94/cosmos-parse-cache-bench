# Cosmos parse/cache benchmark. Everything runs locally; no cloud credentials needed.
set shell := ["bash", "-euo", "pipefail", "-c"]

airflow_version := "3.3.2"
cosmos_version := "1.16.0"
dbt_core_version := "1.12.6"
dbt_duckdb_version := "1.11.1"
py := "3.12"
constraints := "https://raw.githubusercontent.com/apache/airflow/constraints-" + airflow_version + "/constraints-" + py + ".txt"
export AIRFLOW_HOME := justfile_directory() / ".airflow_home"
export AIRFLOW__CORE__LOAD_EXAMPLES := "False"

default:
    @just --list

# Airflow + Cosmos venv (.venv) and a separate dbt venv (.venv-dbt), plus a SQLite metadata DB
setup:
    uv venv -q .venv --python {{py}}
    uv pip install -q --python .venv/bin/python "apache-airflow=={{airflow_version}}" "astronomer-cosmos=={{cosmos_version}}" -c {{constraints}}
    uv venv -q .venv-dbt --python {{py}}
    uv pip install -q --python .venv-dbt/bin/python "dbt-core=={{dbt_core_version}}" "dbt-duckdb=={{dbt_duckdb_version}}"
    .venv/bin/airflow db migrate > /dev/null
    @just versions

# Extra venv with Cosmos from a git ref, e.g. `just setup-ref pr3179 sohailshk/astronomer-cosmos 5056f94...`
setup-ref name repo sha:
    uv venv -q .venv-{{name}} --python {{py}}
    uv pip install -q --python .venv-{{name}}/bin/python "apache-airflow=={{airflow_version}}" -c {{constraints}}
    uv pip install -q --python .venv-{{name}}/bin/python "astronomer-cosmos @ git+https://github.com/{{repo}}@{{sha}}"

# The three Cosmos builds used in the repros: main at the PR base, PR #3179 (#2960), PR #3136 (#2739)
setup-prs:
    just setup-ref main astronomer/astronomer-cosmos 131cf3a72192987376622587538fde6e12368ba4
    just setup-ref pr3179 sohailshk/astronomer-cosmos 5056f941a9a4bd796f60fad817caffcbc2e07fef
    just setup-ref pr3136 Hrushikesh-ramilla/astronomer-cosmos 7d697d8243b7ca07d3c0c7667f79624468d8def5

# PR #3179 plus the local follow-up patch (profile/target in the dbt ls cache key)
setup-pr3179-target:
    just setup-ref pr3179-target sohailshk/astronomer-cosmos 5056f941a9a4bd796f60fad817caffcbc2e07fef
    patch -p1 -d .venv-pr3179-target/lib/python3.12/site-packages < patches/2960-profile-target-in-dbt-ls-cache-key.patch

versions:
    @.venv/bin/python -c "import sys, airflow, cosmos; print('python', sys.version.split()[0]); print('airflow', airflow.__version__); print('cosmos', cosmos.__version__)"
    @.venv-dbt/bin/dbt --version 2>/dev/null | sed -n '1,6p'

# Generate one synthetic dbt project, e.g. `just gen 1000 /tmp/p1000`
gen n out:
    python3 scripts/gen_project.py -n {{n}} --out {{out}}

# Full matrix: sizes x variants x cold/warm, medians over `rounds`.
# For n>=5000, pass e.g. `--import-timeout 600` (Airflow's default dagbag_import_timeout is 30s).
bench sizes="100,500,1000,2500,5000" rounds="3" tag="local":
    .venv/bin/python scripts/bench.py --sizes {{sizes}} --rounds {{rounds}} --tag {{tag}}

# Quick sanity run (~1 min)
bench-smoke:
    .venv/bin/python scripts/bench.py --sizes 100 --rounds 1 --tag smoke

# #2960 dbt ls cache key repro against an installed Cosmos venv (default: .venv = 1.16.0)
repro-2960 venv=".venv" label="cosmos-1.16.0":
    REPRO_PY={{justfile_directory()}}/{{venv}}/bin/python .venv/bin/python repro/repro_2960.py --label {{label}}

# #2739 profile-cache logging repro
repro-2739 venv=".venv" label="cosmos-1.16.0":
    REPRO_PY={{justfile_directory()}}/{{venv}}/bin/python .venv/bin/python repro/repro_2739.py --label {{label}}

# All repros on 1.16.0, main and the two open PRs (needs `just setup-prs`)
repro-all:
    just repro-2960 .venv cosmos-1.16.0
    just repro-2960 .venv-main cosmos-main
    just repro-2960 .venv-pr3179 cosmos-pr3179
    just repro-2960 .venv-pr3179-target cosmos-pr3179-plus-target
    just repro-2739 .venv cosmos-1.16.0
    just repro-2739 .venv-main cosmos-main
    just repro-2739 .venv-pr3136 cosmos-pr3136

# Optional: build a 100-model project and `astro dev parse` it (needs Docker).
# This box talks to Docker at tcp://127.0.0.1:2375; override DOCKER_HOST if needed.
astro-parse sizes="100":
    just astro-projects {{sizes}}
    cd astro && DOCKER_HOST="${DOCKER_HOST:-tcp://127.0.0.1:2375}" astro dev parse

# Synthetic projects for the Astro CLI project (astro/dags/dbt/n<N>, with a manifest)
astro-projects sizes="1000":
    for n in $(echo {{sizes}} | tr ',' ' '); do \
      python3 scripts/gen_project.py -n $n --out astro/dags/dbt/n$n > /dev/null; \
      .venv-dbt/bin/dbt parse -q --project-dir astro/dags/dbt/n$n --profiles-dir astro/dags/dbt; \
      rm -rf astro/dags/dbt/n$n/target/partial_parse.msgpack astro/dags/dbt/n$n/logs; \
    done

clean:
    rm -rf work .airflow_home_repro
