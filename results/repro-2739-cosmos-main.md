# #2739 repro (cosmos-main)

| case | profile log lines | rendered contents logged | password in logs | password in rendered file |
|---|---|---|---|---|
| cache on, miss | 3 | **no** | no | no |
| cache on, hit | 2 | **no** | no | no |
| cache off (reference) | 2 | yes | no | no |

Captured Cosmos log lines that mention the profile:

### cache on, miss

```text
Profile caching is enable.
Using real values for profile bench
Profile not found in cache storing and using profile: /workspace/cosmos-parse-cache-bench/work/repro2739/cache/profile/247edc45a9f9ee891365c0cf4b2266f183255f918e1623e3a06371262582416f/profiles.yml.
```

### cache on, hit

```text
Profile caching is enable.
Profile found in cache using profile: /workspace/cosmos-parse-cache-bench/work/repro2739/cache/profile/247edc45a9f9ee891365c0cf4b2266f183255f918e1623e3a06371262582416f/profiles.yml.
```

### cache off (reference)

```text
Using real values for profile bench
Creating temporary profiles.yml with use_mock_values=False at /tmp/tmp33na9af9/profiles.yml with the following contents:
bench:
    outputs:
        dev:
            dbname: analytics
            host: db.example.internal
            password: '{{ env_var(''COSMOS_CONN_POSTGRES_PASSWORD'') }}'
            port: 5432
            schema: public
            type: postgres
            user: bench_user
    target: dev

```
