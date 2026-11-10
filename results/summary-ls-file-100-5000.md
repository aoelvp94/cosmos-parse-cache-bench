| models | variant | phase | parse s (median) | min | max | tasks | dbt ls calls | cache | peak RSS MB | dbt child peak MB | rounds |
|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|
| 100 | dbt_ls_file | cold | 0.18 | 0.17 | 0.22 | 200 | 0 | n/a | 122 | 0 | 3 |
| 100 | dbt_ls_file | warm | 0.19 | 0.16 | 0.19 | 200 | 0 | n/a | 122 | 0 | 3 |
| 500 | dbt_ls_file | cold | 0.76 | 0.75 | 0.78 | 1000 | 0 | n/a | 135 | 0 | 3 |
| 500 | dbt_ls_file | warm | 0.78 | 0.76 | 0.79 | 1000 | 0 | n/a | 135 | 0 | 3 |
| 1000 | dbt_ls_file | cold | 1.55 | 1.52 | 1.58 | 2000 | 0 | n/a | 150 | 0 | 3 |
| 1000 | dbt_ls_file | warm | 1.51 | 1.48 | 1.52 | 2000 | 0 | n/a | 150 | 0 | 3 |
| 2500 | dbt_ls_file | cold | 4.34 | 4.23 | 4.40 | 5000 | 0 | n/a | 199 | 0 | 3 |
| 2500 | dbt_ls_file | warm | 4.27 | 4.27 | 4.28 | 5000 | 0 | n/a | 199 | 0 | 3 |
| 5000 | dbt_ls_file | cold | 12.20 | 12.15 | 12.77 | 10000 | 0 | n/a | 276 | 0 | 3 |
| 5000 | dbt_ls_file | warm | 12.66 | 12.41 | 12.68 | 10000 | 0 | n/a | 276 | 0 | 3 |
