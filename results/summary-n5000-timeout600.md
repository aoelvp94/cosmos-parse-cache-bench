| models | variant | phase | parse s (median) | min | max | tasks | dbt ls calls | cache | peak RSS MB | dbt child peak MB | rounds |
|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|
| 5000 | dbt_ls_nocache | cold | 58.28 | 58.08 | 58.57 | 10000 | 1 | off | 276 | 397 | 3 |
| 5000 | dbt_ls_nocache | warm | 57.87 | 56.30 | 57.92 | 10000 | 1 | off | 275 | 398 | 3 |
| 5000 | dbt_ls_pp_only | cold | 59.61 | 58.20 | 60.95 | 10000 | 1 | off | 276 | 398 | 3 |
| 5000 | dbt_ls_pp_only | warm | 27.51 | 26.66 | 28.30 | 10000 | 1 | off | 277 | 414 | 3 |
| 5000 | dbt_ls_cache | cold | 57.99 | 56.79 | 58.39 | 10000 | 1 | miss | 287 | 398 | 3 |
| 5000 | dbt_ls_cache | warm | 13.10 | 12.87 | 13.33 | 10000 | 0 | hit | 285 | 0 | 3 |
| 5000 | dbt_manifest | cold | 13.53 | 13.52 | 13.58 | 10000 | 0 | n/a | 292 | 0 | 3 |
| 5000 | dbt_manifest | warm | 13.18 | 12.74 | 13.87 | 10000 | 0 | n/a | 291 | 0 | 3 |
