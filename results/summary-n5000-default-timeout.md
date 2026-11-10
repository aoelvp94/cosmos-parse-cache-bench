| models | variant | phase | parse s (median) | min | max | tasks | dbt ls calls | cache | peak RSS MB | dbt child peak MB | rounds |
|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|
| 5000 | dbt_ls_nocache | cold | import timeout | | | 0 | 1 | off | | | 1 (1 timeouts) |
| 5000 | dbt_ls_nocache | warm | import timeout | | | 0 | 1 | off | | | 1 (1 timeouts) |
| 5000 | dbt_ls_pp_only | cold | import timeout | | | 0 | 1 | off | | | 1 (1 timeouts) |
| 5000 | dbt_ls_pp_only | warm | import timeout | | | 0 | 1 | off | | | 1 (1 timeouts) |
| 5000 | dbt_ls_cache | cold | import timeout | | | 0 | 1 | miss | | | 1 (1 timeouts) |
| 5000 | dbt_ls_cache | warm | import timeout | | | 0 | 1 | miss | | | 1 (1 timeouts) |
| 5000 | dbt_manifest | cold | 14.20 | 14.20 | 14.20 | 10000 | 0 | n/a | 291 | 0 | 1 |
| 5000 | dbt_manifest | warm | 14.59 | 14.59 | 14.59 | 10000 | 0 | n/a | 291 | 0 | 1 |
