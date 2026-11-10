| models | dbt_ls_nocache cold | dbt_ls_nocache warm | dbt_ls_pp_only cold | dbt_ls_pp_only warm | dbt_ls_cache cold | dbt_ls_cache warm | dbt_manifest cold | dbt_manifest warm | dbt_ls_file cold | dbt_ls_file warm |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 4.64 | 4.70 | 4.75 | 3.25 | 5.13 | 0.32 | 0.29 | 0.29 | 0.18 | 0.19 |
| 500 | 8.31 | 8.40 | 8.70 | 4.79 | 8.58 | 0.84 | 0.87 | 0.86 | 0.76 | 0.78 |
| 1000 | 13.84 | 13.93 | 13.44 | 6.62 | 14.20 | 1.78 | 1.61 | 1.56 | 1.55 | 1.51 |
| 2500 | 27.60 | 27.83 | 27.47 | 12.31 | 27.33 | 4.65 | 4.80 | 4.74 | 4.34 | 4.27 |
| 5000 | 58.28 | 57.87 | 59.61 | 27.51 | 57.99 | 13.10 | 13.53 | 13.18 | 12.20 | 12.66 |

| variant | dbt ls calls cold | dbt ls calls warm | cache cold | cache warm |
|---|---:|---:|---|---|
| dbt_ls_nocache | 1 | 1 | off | off |
| dbt_ls_pp_only | 1 | 1 | off | off |
| dbt_ls_cache | 1 | 0 | miss | hit |
| dbt_manifest | 0 | 0 | n/a | n/a |
| dbt_ls_file | 0 | 0 | n/a | n/a |

| models | peak RSS MB (dbt_ls_cache warm) | dbt child peak MB (dbt ls run) | dbt parse s (manifest build) |
|---:|---:|---:|---:|
| 100 | 132 | 140 | 4.6 |
| 500 | 144 | 156 | 7.2 |
| 1000 | 160 | 182 | 10.1 |
| 2500 | 208 | 262 | 19.9 |
| 5000 | 285 | 397 | 38.4 |
