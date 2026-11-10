# #2960 repro (cosmos-pr3179-plus-target)

| scenario | step | cache | dbt ls calls | tasks | tasks sha | reference sha | stale render | keys in cached dbt ls JSON |
|---|---|---|---:|---:|---|---|---|---|
| A | 1 cold | miss | 1 | 61 | fdfcb3ced582 |  |  |  |
| A | 2 warm, nothing changed | hit | 0 | 61 | fdfcb3ced582 |  |  |  |
| A | 3 model file edited | miss | 1 | 61 | fdfcb3ced582 |  |  |  |
| B | 1 cold, source_rendering=none (no --output-keys) | miss | 1 | 61 | fdfcb3ced582 |  |  | alias,config,depends_on,direct_parents,name,original_file_path,package_name,resource_type,tags,unique_id |
| B | ref: source_rendering=all, dbt ls cache off | off | 1 | 73 | cb06f5015799 |  |  |  |
| B | 2 flip to source_rendering=all, cache on | miss | 1 | 73 | cb06f5015799 | cb06f5015799 | no | config,depends_on,fqn,name,original_file_path,resource_type,tags,unique_id |
| C | 1 cold, 'old' Cosmos (no freshness,fqn keys) | miss | 1 | 73 | d84feb44584d |  |  | config,depends_on,name,original_file_path,resource_type,tags,unique_id |
| C | ref: 'new' Cosmos, dbt ls cache off | off | 1 | 73 | cb06f5015799 |  |  |  |
| C | 2 'new' Cosmos, cache on, project untouched | miss | 1 | 73 | cb06f5015799 | cb06f5015799 | no | config,depends_on,fqn,name,original_file_path,resource_type,tags,unique_id |
| D | 1 cold, target=dev | miss | 1 | 61 | fdfcb3ced582 |  |  |  |
| D | ref: target=prod, dbt ls cache off | off | 1 | 60 | 3bc81d93903e |  |  |  |
| D | 2 target=prod, cache on | miss | 1 | 60 | 3bc81d93903e | 3bc81d93903e | no |  |
| E | 1 cold, written by Cosmos 1.16.0 | miss | 1 | 73 | cb06f5015799 |  |  | config,depends_on,fqn,name,original_file_path,resource_type,tags,unique_id |
| E | ref: version under test, dbt ls cache off | off | 1 | 73 | cb06f5015799 |  |  |  |
| E | 2 version under test reads the 1.16.0 cache | miss | 1 | 73 | cb06f5015799 | cb06f5015799 | no | config,depends_on,fqn,name,original_file_path,resource_type,tags,unique_id |
