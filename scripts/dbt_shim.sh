#!/usr/bin/env bash
# Counts dbt invocations made by Cosmos during DAG parsing, then execs the real dbt.
# BENCH_DBT_LOG: file to append one line per invocation. BENCH_REAL_DBT: real dbt executable.
if [ -n "${BENCH_DBT_LOG:-}" ]; then
  printf '%s %s\n' "$(date +%s.%N)" "$*" >> "$BENCH_DBT_LOG"
fi
exec "${BENCH_REAL_DBT:?BENCH_REAL_DBT not set}" "$@"
