# Cosmos parse-time and cache bench

Local, dependency-free benchmark and reproduction repo for
[Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos)
DAG parse time and cache behaviour. It measures how long a fresh Airflow
process takes to import a `DbtDag` as the DAG processor would, how many
times `dbt ls` runs, and whether the Cosmos `dbt ls` cache (an Airflow
Variable) hits or misses. It also reproduces two open upstream issues:

- [#2960](https://github.com/astronomer/astronomer-cosmos/issues/2960) — the
  `dbt ls` cache key ignores the requested `--output-keys` (and, as a
  follow-up found here, also ignores the dbt profile/target).
- [#2739](https://github.com/astronomer/astronomer-cosmos/issues/2739) —
  rendered `profiles.yml` contents are not logged when profile caching is on.

Everything runs against synthetic dbt-duckdb projects. There are no cloud
credentials, no employer or product branding, and nothing in this tree is
pushed anywhere by default.

## Layout

| Path | Purpose |
|---|---|
| `scripts/gen_project.py` | Deterministic synthetic dbt project (staging / intermediate / marts, sources, unique + not_null tests). |
| `scripts/bench.py`, `scripts/parse_once.py`, `scripts/dbt_shim.sh` | Fresh-process `DagBag` harness: parse time, task count, `dbt ls` call counting, cache hit/miss, RSS. |
| `dags/cosmos_bench_dag.py` | The single `DbtDag` under test (load mode and project come from env vars). |
| `dags/target_switch_dag.py` | Minimal `DbtDag` whose `ProfileConfig.target_name` comes from the Airflow Variable `target_switch_dbt_target`. |
| `projects/target_switch/` | Two-model dbt project (`always_on`, plus `prod_only` enabled only when `target.name == 'prod'`). |
| `repro/repro_2960.py`, `repro/repro_2739.py` | Issue reproductions, compared across Cosmos builds. |
| `repro/repro_target_switch.py` | End-to-end DagBag repro of a `dev` → `prod` target switch on Cosmos main vs the local fix. |
| `patches/2960-profile-target-in-dbt-ls-cache-key.patch` | Local follow-up on top of PR #3179: put `--profile` / `--target` in the cache key. |
| `astro/` | Astro CLI (Runtime 3.3-12) project for the same DAGs. Generated dbt projects are gitignored. |
| `results/` | Raw JSONL, median tables, repro transcripts, and command logs from the runs below. |
| `justfile` | Setup, bench, repro, and Astro entry points. |
| `work/` | Generated projects, manifests, and caches (gitignored). |

## Setup

Python 3.12, [uv](https://github.com/astral-sh/uv), and [just](https://github.com/casey/just)
are required. Docker is optional and only needed for the Astro CLI path.

```bash
just setup          # Airflow 3.3.2 + Cosmos 1.16.0 in .venv, dbt-core 1.12.6 + dbt-duckdb 1.11.1 in .venv-dbt
just setup-prs      # optional: Cosmos main, PR #3179, PR #3136, for the repros
just setup-pr3179-target   # optional: PR #3179 plus the local profile/target patch
just versions
```

## Reproduce the benchmarks (plain Airflow DagBag)

This is the path the numbers below were measured with. Each sample is a
fresh Python process (`scripts/parse_once.py`) importing `dags/cosmos_bench_dag.py`
through Airflow 3's `DagBag`, the way a DAG processor re-parses a file.

```bash
just bench-smoke                                          # n=100, 1 round, ~1 min
just bench                                                # default: n=100,500,1000,2500,5000; 3 rounds
.venv/bin/python scripts/bench.py --sizes 5000 --rounds 3 \
    --tag n5000-timeout600 --import-timeout 600           # n=5000 needs more than the 30s default
.venv/bin/python scripts/bench.py --sizes 100,500,1000,2500,5000 \
    --variants dbt_ls_file --rounds 3 --tag ls-file-100-5000 --import-timeout 600
```

Variants (see the header of `scripts/bench.py` for the exact env):

| variant | `LoadMode` | cache flags |
|---|---|---|
| `dbt_ls_nocache` | `DBT_LS` | `AIRFLOW__COSMOS__ENABLE_CACHE=False` (no `dbt ls` cache, no partial-parse cache) |
| `dbt_ls_pp_only` | `DBT_LS` | `AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=False` (partial-parse cache only) |
| `dbt_ls_cache` | `DBT_LS` | Cosmos defaults (`dbt ls` cache in an Airflow Variable + partial-parse cache) |
| `dbt_manifest` | `DBT_MANIFEST` | pre-built `target/manifest.json` (`dbt parse` cost is reported separately) |
| `dbt_ls_file` | `DBT_LS_FILE` | pre-built `dbt ls --output json` file (same `--output-keys` Cosmos 1.16 uses) |

`cold` wipes the Cosmos cache dir and the `cosmos_cache__*` Variables before
the parse. `warm` is the immediately following parse, caches kept, project
untouched. Every variant/size combination is repeated `--rounds` times;
tables report the median.

## Reproduce the issue repros

```bash
just setup-prs
just setup-pr3179-target          # only needed for the profile/target follow-up
just repro-all                    # writes results/repro-2960-*.md and results/repro-2739-*.md
# or one at a time:
just repro-2960 .venv cosmos-1.16.0
just repro-2960 .venv-pr3179 cosmos-pr3179
just repro-2960 .venv-pr3179-target cosmos-pr3179-plus-target
just repro-2739 .venv cosmos-1.16.0
just repro-2739 .venv-pr3136 cosmos-pr3136
just setup-fix                    # .venv-fix with the profile/target fix (fork branch, 41ad52b3)
just repro-target-switch          # DagBag e2e: dev→prod on main vs the fix; writes results/repro-target-switch.md
```

## Astro CLI path (optional)

`astro/` is an Astro CLI 1.47 project on Astronomer Runtime 3.3-12. It defines,
for each generated project under `dags/dbt/<name>/`, a `LoadMode.DBT_LS` DAG and
a `LoadMode.DBT_MANIFEST` DAG. The Dockerfile installs dbt into a separate
Python 3.12 virtualenv and points `COSMOS_BENCH_DBT` at it.

```bash
export DOCKER_HOST=tcp://127.0.0.1:2375   # how Docker is reached on this machine; override if needed
just astro-parse                          # generates n=100 and runs `astro dev parse`
# or:
just astro-projects 100
cd astro && astro dev parse
# full local Airflow (five containers) is also available, not required for the numbers:
# cd astro && astro dev start
```

`astro dev parse` needs a Docker client with the **buildx** plugin. Without it
the Runtime image build fails (`the --mount option requires BuildKit`). On this
machine, after installing buildx v0.30.1, `just astro-parse` completed
successfully: 2 DAG integrity tests passed, "No errors detected in your Dags"
(see `results/log-astro-parse.log`). The parse-time numbers themselves still
come from the plain `DagBag` harness above, not from the Astro CLI.

## Results

Measured on 2026-11-10 (Asia/Yerevan) with Python 3.12, Airflow 3.3.2, Cosmos
1.16.0, dbt-core 1.12.6, and dbt-duckdb 1.11.1, on an 8-vCPU Xeon box with 16
GiB RAM. Other workloads were running on the same machine, so **absolute times
are noisy**; the useful signal is the comparison across variants and the
cold/warm ratio. Each model has unique + not_null tests, so a project of N
models renders **2N Airflow tasks**.

Raw data: `results/raw-concurrent-100-2500.jsonl`,
`results/raw-n5000-timeout600.jsonl`, `results/raw-ls-file-100-5000.jsonl`.
Per-run median tables: `results/summary-*.md`. Combined table:
`results/summary-all.md`.

### Median DAG parse time (seconds)

| models | dbt_ls_nocache cold | dbt_ls_nocache warm | dbt_ls_pp_only cold | dbt_ls_pp_only warm | dbt_ls_cache cold | dbt_ls_cache warm | dbt_manifest cold | dbt_manifest warm | dbt_ls_file cold | dbt_ls_file warm |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 4.64 | 4.70 | 4.75 | 3.25 | 5.13 | 0.32 | 0.29 | 0.29 | 0.18 | 0.19 |
| 500 | 8.31 | 8.40 | 8.70 | 4.79 | 8.58 | 0.84 | 0.87 | 0.86 | 0.76 | 0.78 |
| 1000 | 13.84 | 13.93 | 13.44 | 6.62 | 14.20 | 1.78 | 1.61 | 1.56 | 1.55 | 1.51 |
| 2500 | 27.60 | 27.83 | 27.47 | 12.31 | 27.33 | 4.65 | 4.80 | 4.74 | 4.34 | 4.27 |
| 5000 | 58.28 | 57.87 | 59.61 | 27.51 | 57.99 | 13.10 | 13.53 | 13.18 | 12.20 | 12.66 |

### Cache and `dbt ls` behaviour (all sizes)

| variant | dbt ls calls cold | dbt ls calls warm | cache cold | cache warm |
|---|---:|---:|---|---|
| `dbt_ls_nocache` | 1 | 1 | off | off |
| `dbt_ls_pp_only` | 1 | 1 | off | off |
| `dbt_ls_cache` | 1 | 0 | miss | hit |
| `dbt_manifest` | 0 | 0 | n/a | n/a |
| `dbt_ls_file` | 0 | 0 | n/a | n/a |

### Memory and one-time `dbt parse` cost

| models | peak RSS MB (`dbt_ls_cache` warm) | dbt child peak MB (a `dbt ls` run) | `dbt parse` s (manifest build) |
|---:|---:|---:|---:|
| 100 | 132 | 140 | 4.6 |
| 500 | 144 | 156 | 7.2 |
| 1000 | 160 | 182 | 10.1 |
| 2500 | 208 | 262 | 19.9 |
| 5000 | 285 | 397 | 38.4 |

### Airflow's default 30s `dagbag_import_timeout`

A first pass at n=5000 with Airflow's default `dagbag_import_timeout=30`
(`results/raw-n5000-default-timeout.jsonl`) is itself a result: every
`LoadMode.DBT_LS` variant **timed out** (the parse needs ~58 s, and a timed-out
cold parse never writes the `dbt ls` cache, so the warm parse also misses and
times out). `LoadMode.DBT_MANIFEST` finished in ~14 s and was fine. Raising
the timeout to 600 s is what produced the n=5000 row above. After a successful
cold parse, a `dbt_ls_cache` warm parse is ~13 s and would fit under the 30 s
default — but only if the first parse is allowed to finish.

## Findings

- **The `dbt ls` cache is the large win, and it actually skips `dbt ls`.**
  On a 2500-model project, default Cosmos cache goes from 27.33 s (miss, 1
  `dbt ls`) to 4.65 s (hit, 0 `dbt ls`), a ~6× reduction, and lands in the
  same place as a pre-built manifest (4.80 s). At 5000 models the ratio is
  57.99 s → 13.10 s. With the cache off, warm is no faster than cold.
- **Partial-parse cache alone is a smaller, still-real win, and still calls
  `dbt ls`.** `dbt_ls_pp_only` warm is roughly half of cold (12.31 s vs 27.47 s
  at 2500; 27.51 s vs 59.61 s at 5000) with one `dbt ls` on every parse.
- **`LoadMode.DBT_LS_FILE` is the fastest of the precomputed options**,
  slightly ahead of `DBT_MANIFEST` (4.34 s vs 4.80 s at 2500; 12.20 s vs
  13.53 s at 5000), because the `dbt ls` JSON is a smaller input than a full
  manifest. Like the manifest, it does not invoke `dbt ls` at parse time.
  The cost of building that file is paid once, out of band, the same way
  `dbt parse` is paid once for the manifest.
- **At 5000 models, a cold `DBT_LS` parse does not fit in Airflow's default
  30 s import timeout.** See the section above. Operators who stay on
  `LoadMode.DBT_LS` for large projects need a higher
  `dagbag_import_timeout` (and DAG processor timeout) at least for the first
  parse, or they need to ship a manifest / `dbt ls` file instead.
- RSS of the Airflow process after a cache-hit parse tracks the rendered
  graph (~132 MB at 100 models, ~285 MB at 5000). A live `dbt ls` child is
  larger than that (~140 MB / ~397 MB).

## #2960 — `dbt ls` cache key vs output keys, upgrades, and dbt target

The cache key used to hash select/exclude/vars/selector/partial-parse flags,
env vars, and configured Airflow Variables, but **not** the `--output-keys`
list Cosmos asked `dbt ls` for. A cache written under one key list can then
be served to a parse that needed a different list, and the rendered task
graph is silently wrong. Scenarios in `repro/repro_2960.py`:

| scenario | what changes | 1.16.0 / main | PR [#3179](https://github.com/astronomer/astronomer-cosmos/pull/3179) | PR #3179 + local patch |
|---|---|---|---|---|
| A (control) | a model file is edited | miss, graph unchanged (expected) | same | same |
| B | `pre_dbt_fusion=True`, flip `source_rendering_behavior` none → all | **stale hit** (61-task key list served; graph sha ≠ reference) | miss, graph matches reference | same as #3179 |
| C | simulated upgrade that adds `freshness`/`fqn` to `--output-keys` | **stale hit** | miss, graph matches reference | same as #3179 |
| D | switch dbt target `dev` → `prod` (one model is `enabled=(target.name == 'dev')`) | **stale hit** (61 tasks kept; prod reference is 60) | **stale hit** (same) | miss, 60 tasks, matches reference |
| E | cache written by 1.16.0, read by the build under test | n/a (same version) | miss, graph matches (key args changed, cache rebuilt once) | same as #3179 |

PR #3179 (sohailshk, `5056f94`) closes the issue as filed: it builds the
`--output` / `--output-keys` flags in one place and adds them to the cache
key, so B, C, and the 1.16.0 → newer upgrade all invalidate correctly.
**It does not hash the dbt profile or target.** Scenario D is therefore
still stale on #3179, including on Cosmos 1.16.0 and current main. The
local patch in `patches/2960-profile-target-in-dbt-ls-cache-key.patch`
extends the #3179 key with `--profile` and `--target` and is the only build
in this repo that gets D right, without breaking A–C or E. `just
setup-pr3179-target` applies it on top of #3179.

## End-to-end: switching the DAG target (DagBag parse)

**Short version:** on Cosmos main, switching a `DbtDag` from `target=dev` to
`target=prod` with the `dbt ls` cache on serves the cached `dev` graph, so
models that are only enabled in `prod` never show up as tasks. With the fix
(`--profile` / `--target` in the cache key) the prod parse misses the cache
and renders them.

Reproduce in three commands (needs Python 3.12, `uv`, and `just`):

```bash
just setup                  # Airflow 3.3.2 + dbt-core 1.12.6 / dbt-duckdb 1.11.1
just repro-target-switch    # builds .venv-main (Cosmos main 131cf3a7) and .venv-fix (fix 41ad52b3) if missing
cat results/repro-target-switch.md
```

`just setup-fix` installs the fix from
[`aoelvp94/astronomer-cosmos` at `41ad52b3`](https://github.com/aoelvp94/astronomer-cosmos/commit/41ad52b3be64ad3a4099d444ec86557d731c409a)
(branch `fix/dbt-ls-cache-key-profile-target`). Pass a local checkout to test
something else, e.g. `just setup-fix ../astronomer-cosmos`.

Scenario D above is one case inside `repro/repro_2960.py`. This standalone
recipe is the small, dedicated end-to-end repro for the profile/target
cache-key gap, meant to be linked from an upstream PR.

A two-model project (`projects/target_switch`) is imported through Airflow 3's
`DagBag` — the same path a DAG processor uses, not a direct `DbtGraph` call.
The DAG reads `ProfileConfig.target_name` from the Airflow Variable
`target_switch_dbt_target` (this Variable is **not** listed in
`RenderConfig.airflow_vars_to_purge_dbt_ls_cache`). With the Cosmos `dbt ls`
cache on, the file is parsed at `target=dev` and then at `target=prod`, once
with Cosmos **main** (`131cf3a7`) and once with the **fix** (`41ad52b3`).

Results measured 2026-11-10 (Asia/Yerevan) on Airflow 3.3.2, dbt-core 1.12.6,
and dbt-duckdb 1.11.1. The hashed key args are the second half of the cache
Variable's `version` field (`md5` of `DbtGraph.dbt_ls_cache_key_args`). The
Variable name is `cosmos_cache__target_switch` in every row.

| build | parse | hashed key args | cache | task_ids | prod_only |
|---|---|---|---|---|---|
| main (`131cf3a`) | dev (cold) | `fe93a97f74feddf7bef8f4ca29c2724c` | **miss** | `always_on_run` | no |
| main (`131cf3a`) | prod (cache on) | `fe93a97f74feddf7bef8f4ca29c2724c` (same) | **hit** | `always_on_run` | no |
| fix (`41ad52b`) | dev (cold) | `875f88d1dcf26d19e28abd65e4efde1e` | **miss** | `always_on_run` | no |
| fix (`41ad52b`) | prod (cache on) | `88c21679f014f876ad033a4dea1e0a5b` (changed) | **miss** | `always_on_run`, `prod_only_run` | yes |

On main the key args are just `--no-partial-parse` for both targets, so the
prod parse is a stale cache hit and `prod_only` never appears. On the fix the
key also includes `--profile bench --target <name>`, the hash changes with
the target, prod misses, and `prod_only_run` is rendered.

## #2739 — profile contents logging with profile caching on or off

When Cosmos renders an Airflow connection into a `profiles.yml` through a
profile mapping, the non-cached branch logs the file contents at INFO. The
cached branch (the default) only logged the cache path, which makes "Cosmos
generated the wrong profile" hard to diagnose. `repro/repro_2739.py` calls
`ProfileConfig.ensure_profile()` with a `PostgresUserPasswordProfileMapping`
in a fresh process (no database is needed).

| case | 1.16.0 / main | PR [#3136](https://github.com/astronomer/astronomer-cosmos/pull/3136) |
|---|---|---|
| cache on, miss | path only, contents **not** logged | contents logged, `password: '***'` |
| cache on, hit | path only (optional in the issue) | path only |
| cache off (reference) | contents logged, password is `'{{ env_var(...) }}'`, not the secret | same |
| secret leaked into logs or the rendered file | no | no |

PR #3136 (Hrushikesh-ramilla, `7d697d82`) matches the issue's acceptance
criteria: on a cache miss it logs the rendered profile at INFO with
`secret_fields` redacted, and it does not change the non-cached branch or
leak the password. Logging contents on a cache hit is optional and is not
implemented, which is fine.

## Versions pinned by the repros

| label | source |
|---|---|
| `cosmos-1.16.0` | PyPI `astronomer-cosmos==1.16.0` |
| `cosmos-main` | `astronomer/astronomer-cosmos@131cf3a72192987376622587538fde6e12368ba4` |
| `cosmos-pr3179` | `sohailshk/astronomer-cosmos@5056f941a9a4bd796f60fad817caffcbc2e07fef` |
| `cosmos-pr3136` | `Hrushikesh-ramilla/astronomer-cosmos@7d697d8243b7ca07d3c0c7667f79624468d8def5` |
| `cosmos-pr3179-plus-target` | #3179 plus `patches/2960-profile-target-in-dbt-ls-cache-key.patch` |
