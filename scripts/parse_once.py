"""Parse the benchmark DAG once in a fresh interpreter and print one JSON line of metrics.

Measured:
  parse_s        wall time of DagBag(...) for the bench DAG file (Airflow import excluded)
  tasks          number of Airflow tasks rendered
  dbt_calls      dbt invocations made during parsing (counted by scripts/dbt_shim.sh)
  dbt_ls_calls   of which `dbt ls`
  cache          hit | miss | off (dbt ls cache disabled) | n/a   (from Cosmos' own "Cosmos performance: Cache hit/miss" log lines)
  rss_base_mb    RSS high-water mark before parsing (Airflow + Cosmos imported)
  rss_peak_mb    RSS high-water mark of this process after parsing
  child_peak_mb  RSS high-water mark of the largest child (the dbt subprocess)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import resource
import sys
import tempfile
import time
from pathlib import Path


class _Collector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.lines.append(record.getMessage())
        except Exception:  # pragma: no cover
            pass


def main() -> None:
    dag_file = Path(sys.argv[1])
    dbt_log = Path(tempfile.mkstemp(prefix="dbtcalls-")[1])
    os.environ["BENCH_DBT_LOG"] = str(dbt_log)

    from airflow.dag_processing.dagbag import DagBag  # noqa: E402  (Airflow 3.x location)
    import cosmos  # noqa: F401,E402  warm the import so it is not billed to parsing

    drop = [k for k in os.environ.get("BENCH_DROP_OUTPUT_KEYS", "").split(",") if k]
    if drop:
        # Repro helper for #2960: behave like a Cosmos release that requested fewer --output-keys.
        import functools

        import cosmos.dbt.graph as g

        if hasattr(g.DbtGraph, "dbt_ls_output_args"):
            # Cosmos builds the output flags in one property (and may hash them into the cache key):
            # patch the property so both the command and the key see the "older" list.
            _orig_prop = g.DbtGraph.dbt_ls_output_args.func

            def _older(self):
                return [a for a in _orig_prop(self) if a not in drop]

            prop = functools.cached_property(_older)
            prop.__set_name__(g.DbtGraph, "dbt_ls_output_args")
            g.DbtGraph.dbt_ls_output_args = prop
        else:
            # Cosmos <= 1.16 inlines the list in run_dbt_ls and never hashes it: filter the command.
            _orig = g.run_command

            def _run_command(command, *args, **kwargs):
                if "ls" in command and "--output-keys" in command:
                    command = [c for c in command if c not in drop]
                return _orig(command, *args, **kwargs)

            g.run_command = _run_command

    collector = _Collector()
    cosmos_level = logging.DEBUG if os.environ.get("BENCH_DUMP_CACHE") else logging.INFO
    for name in ("cosmos", "airflow.task.cosmos"):
        lg = logging.getLogger(name)
        lg.addHandler(collector)
        lg.setLevel(cosmos_level)

    child_base = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024
    rss_base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    t0 = time.perf_counter()
    bag = DagBag(dag_folder=str(dag_file), safe_mode=False)
    parse_s = time.perf_counter() - t0
    rss_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    child_peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024

    calls = [ln for ln in dbt_log.read_text().splitlines() if ln.strip()]
    dbt_log.unlink(missing_ok=True)
    msgs = collector.lines
    hit = any("Cache hit for" in m for m in msgs)
    from cosmos import settings as cs

    ls_cache_on = bool(cs.enable_cache and cs.enable_cache_dbt_ls)
    miss = ls_cache_on and any("Cache miss for" in m for m in msgs)
    skipped = not ls_cache_on
    dags = list(bag.dags.values())
    task_ids = sorted(f"{t.task_id}:{type(t).__name__}" for d in dags for t in d.tasks)
    if os.environ.get("BENCH_DUMP_TASKS"):
        Path(os.environ["BENCH_DUMP_TASKS"]).write_text("\n".join(task_ids) + "\n")
    out = {
        "parse_s": round(parse_s, 4),
        "dags": len(dags),
        "tasks": sum(len(d.tasks) for d in dags),
        "tasks_sha": hashlib.sha256("\n".join(task_ids).encode()).hexdigest()[:12],
        "import_errors": {k: v[-400:] for k, v in bag.import_errors.items()},
        "dbt_calls": len(calls),
        "dbt_ls_calls": sum(1 for c in calls if " ls " in f" {c.split(' ', 1)[1]} "),
        "dbt_cmds": [c.split(" ", 1)[1].split(" ")[0] for c in calls],
        "cache": "hit" if hit else ("miss" if miss else ("off" if skipped else "n/a")),
        "rss_base_mb": round(rss_base, 1),
        "rss_peak_mb": round(rss_peak, 1),
        "child_peak_mb": round(child_peak, 1) if child_peak > child_base else 0.0,
        "cosmos_msgs": [m[:300] for m in msgs if "Cosmos performance" in m or "cache" in m.lower()][:20],
    }
    if os.environ.get("BENCH_DUMP_CACHE"):
        # Extra fields for the target-switch DagBag repro: Variable name, hashed key
        # args, the raw dbt_ls_cache_key_args list, and rendered task ids.
        from airflow.models import Variable as AFVar

        dag_id = dags[0].dag_id if dags else os.environ.get("BENCH_DAG_ID", "")
        var_name = f"cosmos_cache__{dag_id}"
        out["cache_variable"] = var_name
        out["task_ids"] = [tid.split(":", 1)[0] for tid in task_ids]
        args_list = None
        for m in msgs:
            if "dbt_ls_cache_key_args" in m:
                # "Value of `dbt_ls_cache_key_args` for <cosmos_cache__...>: ['--profile', ...]"
                bracket = m.find("[")
                if bracket >= 0:
                    try:
                        args_list = json.loads(m[bracket:].replace("'", '"'))
                    except json.JSONDecodeError:
                        args_list = None
        out["cache_key_args"] = args_list
        raw = AFVar.get(var_name, default_var=None)
        version = None
        if raw:
            payload = json.loads(raw) if isinstance(raw, str) else raw
            version = payload.get("version")
        # Prefer the version Cosmos compared against (hit log includes it).
        for m in msgs:
            if "Cache hit for" in m and " - " in m:
                version = m.split(" - ", 1)[1].strip() or version
        out["cache_version"] = version
        out["cache_args_hash"] = version.split(",", 1)[1] if version and "," in version else None
    print("BENCH_RESULT " + json.dumps(out))


if __name__ == "__main__":
    main()
