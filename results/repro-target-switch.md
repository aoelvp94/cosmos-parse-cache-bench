# Target-switch DagBag repro

Measured 2026-11-10 (Asia/Yerevan). Real Airflow 3.3.2 `DagBag` parse of
`dags/target_switch_dag.py` (not a direct `DbtGraph` call), with
`AIRFLOW__COSMOS__ENABLE_CACHE` and `ENABLE_CACHE_DBT_LS` on. The DAG reads
`ProfileConfig.target_name` from the Airflow Variable `target_switch_dbt_target`.
Project: `projects/target_switch` (`always_on` + `prod_only` enabled only when
`target.name == 'prod'`).

`just repro-target-switch`

| build | parse | Variable | hashed key args | cache | task_ids | prod_only |
|---|---|---|---|---|---|---|
| main (`131cf3a`) | dev (cold) | `cosmos_cache__target_switch` | `fe93a97f74feddf7bef8f4ca29c2724c` | **miss** | always_on_run | no |
| main (`131cf3a`) | prod (cache on) | `cosmos_cache__target_switch` | `fe93a97f74feddf7bef8f4ca29c2724c` | **hit** | always_on_run | no |
| fix (`41ad52b`) | dev (cold) | `cosmos_cache__target_switch` | `875f88d1dcf26d19e28abd65e4efde1e` | **miss** | always_on_run | no |
| fix (`41ad52b`) | prod (cache on) | `cosmos_cache__target_switch` | `88c21679f014f876ad033a4dea1e0a5b` | **miss** | always_on_run, prod_only_run | yes |

- On main, the Variable name and the hashed key args are the same for both
  parses, so prod is a **stale hit** and `prod_only` is missing.
- On the fix, the hashed key args change with the target, prod is a **miss**,
  and `prod_only` is rendered.
- main stale=True, same hash on main=True;
  fix ok=True, different hash on fix=True.

Raw rows:

```json
[
  {
    "build": "main",
    "sha": "131cf3a72192987376622587538fde6e12368ba4",
    "parse": "dev (cold)",
    "target": "dev",
    "cache_variable": "cosmos_cache__target_switch",
    "cache_args_hash": "fe93a97f74feddf7bef8f4ca29c2724c",
    "cache_key_args": [
      "--no-partial-parse"
    ],
    "cache_version": "483887f1e2e92f13a9e31cc792456737,fe93a97f74feddf7bef8f4ca29c2724c",
    "cache": "miss",
    "dbt_ls_calls": 1,
    "task_ids": [
      "always_on_run"
    ],
    "prod_only": false
  },
  {
    "build": "main",
    "sha": "131cf3a72192987376622587538fde6e12368ba4",
    "parse": "prod (cache on)",
    "target": "prod",
    "cache_variable": "cosmos_cache__target_switch",
    "cache_args_hash": "fe93a97f74feddf7bef8f4ca29c2724c",
    "cache_key_args": [
      "--no-partial-parse"
    ],
    "cache_version": "483887f1e2e92f13a9e31cc792456737,fe93a97f74feddf7bef8f4ca29c2724c",
    "cache": "hit",
    "dbt_ls_calls": 0,
    "task_ids": [
      "always_on_run"
    ],
    "prod_only": false
  },
  {
    "build": "fix",
    "sha": "41ad52b3be64ad3a4099d444ec86557d731c409a",
    "parse": "dev (cold)",
    "target": "dev",
    "cache_variable": "cosmos_cache__target_switch",
    "cache_args_hash": "875f88d1dcf26d19e28abd65e4efde1e",
    "cache_key_args": [
      "--no-partial-parse",
      "--profile",
      "bench",
      "--target",
      "dev"
    ],
    "cache_version": "483887f1e2e92f13a9e31cc792456737,875f88d1dcf26d19e28abd65e4efde1e",
    "cache": "miss",
    "dbt_ls_calls": 1,
    "task_ids": [
      "always_on_run"
    ],
    "prod_only": false
  },
  {
    "build": "fix",
    "sha": "41ad52b3be64ad3a4099d444ec86557d731c409a",
    "parse": "prod (cache on)",
    "target": "prod",
    "cache_variable": "cosmos_cache__target_switch",
    "cache_args_hash": "88c21679f014f876ad033a4dea1e0a5b",
    "cache_key_args": [
      "--no-partial-parse",
      "--profile",
      "bench",
      "--target",
      "prod"
    ],
    "cache_version": "483887f1e2e92f13a9e31cc792456737,88c21679f014f876ad033a4dea1e0a5b",
    "cache": "miss",
    "dbt_ls_calls": 1,
    "task_ids": [
      "always_on_run",
      "prod_only_run"
    ],
    "prod_only": true
  }
]
```
