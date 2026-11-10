"""End-to-end DagBag repro: switching the DAG's dbt target must not reuse a stale dbt ls cache.

A two-model project (always_on + prod_only, the latter enabled only when
target.name == 'prod') is parsed twice through Airflow's DagBag — the same
path the DAG processor uses — with the Cosmos dbt ls cache on. The DAG reads
ProfileConfig.target_name from the Airflow Variable target_switch_dbt_target
(not listed in airflow_vars_to_purge_dbt_ls_cache). After a cold parse at
target=dev, the Variable is set to prod and the file is parsed again.

On Cosmos main (and 1.16.0) the cache key ignores the target, so the second
parse is a hit and prod_only is missing from the task list. On the local fix
the hashed key args change, the second parse misses, and prod_only is rendered.

Usage:
  .venv-main/bin/python repro/repro_target_switch.py \\
      --main-py .venv-main/bin/python --fix-py .venv-fix/bin/python
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAG = ROOT / "dags" / "target_switch_dag.py"
PROJECT = ROOT / "projects" / "target_switch"
HOME = Path(os.environ.get("REPRO_AIRFLOW_HOME", ROOT / ".airflow_home_target_switch"))
CACHE = ROOT / "work" / "target-switch-cache"
DAG_ID = "target_switch"
TARGET_VAR = "target_switch_dbt_target"
CACHE_VAR = f"cosmos_cache__{DAG_ID}"


def base_env(py: Path) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AIRFLOW__COSMOS__", "BENCH_"))}
    env.update(
        AIRFLOW_HOME=str(HOME),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__DAGS_FOLDER=str(ROOT / "dags"),
        AIRFLOW__LOGGING__LOGGING_LEVEL="INFO",
        AIRFLOW__COSMOS__ENABLE_CACHE="1",
        AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS="1",
        AIRFLOW__COSMOS__CACHE_DIR=str(CACHE),
        BENCH_DAG_ID=DAG_ID,
        BENCH_DBT=str(ROOT / "scripts" / "dbt_shim.sh"),
        BENCH_REAL_DBT=str(ROOT / ".venv-dbt" / "bin" / "dbt"),
        BENCH_PROFILES_YML=str(ROOT / "profiles" / "profiles.yml"),
        TARGET_SWITCH_PROJECT=str(PROJECT),
        BENCH_DUMP_CACHE="1",
        # Target comes from the Airflow Variable, not this env var.
        BENCH_TARGET="",
    )
    # An empty BENCH_TARGET would be treated as set by the DAG (`os.environ.get`).
    # Drop it so Variable.get is used.
    env.pop("BENCH_TARGET", None)
    return env


def ensure_db(py: Path) -> None:
    HOME.mkdir(parents=True, exist_ok=True)
    if not (HOME / "airflow.db").exists():
        subprocess.run(
            [str(py.parent / "airflow"), "db", "migrate"],
            env={**os.environ, "AIRFLOW_HOME": str(HOME), "AIRFLOW__CORE__LOAD_EXAMPLES": "False"},
            check=True,
            capture_output=True,
        )


def set_target(py: Path, target: str) -> None:
    env = {**os.environ, "AIRFLOW_HOME": str(HOME), "AIRFLOW__CORE__LOAD_EXAMPLES": "False"}
    subprocess.run(
        [str(py.parent / "airflow"), "variables", "set", TARGET_VAR, target],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def wipe_dbt_ls_cache() -> None:
    shutil.rmtree(CACHE, ignore_errors=True)
    db = HOME / "airflow.db"
    if db.exists():
        with sqlite3.connect(db) as c:
            c.execute("delete from variable where key like ?", (f"{CACHE_VAR}%",))


def parse(py: Path) -> dict:
    env = base_env(py)
    p = subprocess.run(
        [str(py), str(ROOT / "scripts" / "parse_once.py"), str(DAG)],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    line = next((ln for ln in p.stdout.splitlines() if ln.startswith("BENCH_RESULT ")), None)
    if not line:
        raise RuntimeError(
            f"parse failed rc={p.returncode}\nSTDOUT:\n{p.stdout[-2000:]}\nSTDERR:\n{p.stderr[-3000:]}"
        )
    r = json.loads(line[len("BENCH_RESULT "):])
    if r.get("import_errors"):
        raise RuntimeError(f"import errors: {r['import_errors']}")
    return r


def run_build(label: str, sha: str, py: Path) -> list[dict]:
    ensure_db(py)
    wipe_dbt_ls_cache()
    rows = []
    for step, target in (("dev (cold)", "dev"), ("prod (cache on)", "prod")):
        set_target(py, target)
        r = parse(py)
        task_ids = r.get("task_ids") or []
        rows.append(
            {
                "build": label,
                "sha": sha,
                "parse": step,
                "target": target,
                "cache_variable": r.get("cache_variable") or CACHE_VAR,
                "cache_args_hash": r.get("cache_args_hash"),
                "cache_key_args": r.get("cache_key_args"),
                "cache_version": r.get("cache_version"),
                "cache": r["cache"],
                "dbt_ls_calls": r.get("dbt_ls_calls"),
                "task_ids": task_ids,
                "prod_only": any("prod_only" in t for t in task_ids),
            }
        )
    return rows


def markdown_table(rows: list[dict]) -> str:
    lines = [
        "| build | parse | Variable | hashed key args | cache | task_ids | prod_only |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        tasks = ", ".join(r["task_ids"]) if r["task_ids"] else "(none)"
        present = "yes" if r["prod_only"] else "no"
        lines.append(
            f"| {r['build']} (`{r['sha'][:7]}`) | {r['parse']} | `{r['cache_variable']}` | "
            f"`{r['cache_args_hash'] or '?'}` | **{r['cache']}** | {tasks} | {present} |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--main-py", default=str(ROOT / ".venv-main" / "bin" / "python"))
    ap.add_argument("--fix-py", default=str(ROOT / ".venv-fix" / "bin" / "python"))
    ap.add_argument("--main-sha", default="131cf3a72192987376622587538fde6e12368ba4")
    ap.add_argument("--fix-sha", default="41ad52b3be64ad3a4099d444ec86557d731c409a")
    ap.add_argument("--out", default=str(ROOT / "results" / "repro-target-switch.md"))
    a = ap.parse_args()

    main_py, fix_py = Path(a.main_py), Path(a.fix_py)
    for p in (main_py, fix_py):
        if not p.exists():
            raise SystemExit(f"missing interpreter {p}; run `just setup-ref` / `just setup-fix` first")

    rows = []
    rows.extend(run_build("main", a.main_sha, main_py))
    rows.extend(run_build("fix", a.fix_sha, fix_py))

    main_dev, main_prod, fix_dev, fix_prod = rows
    main_stale = main_prod["cache"] == "hit" and not main_prod["prod_only"]
    fix_ok = fix_prod["cache"] == "miss" and fix_prod["prod_only"] and not fix_dev["prod_only"]
    same_hash_on_main = main_dev["cache_args_hash"] == main_prod["cache_args_hash"]
    different_hash_on_fix = fix_dev["cache_args_hash"] != fix_prod["cache_args_hash"]

    table = markdown_table(rows)
    body = f"""# Target-switch DagBag repro

Measured 2026-11-10 (Asia/Yerevan). Real Airflow 3.3.2 `DagBag` parse of
`dags/target_switch_dag.py` (not a direct `DbtGraph` call), with
`AIRFLOW__COSMOS__ENABLE_CACHE` and `ENABLE_CACHE_DBT_LS` on. The DAG reads
`ProfileConfig.target_name` from the Airflow Variable `target_switch_dbt_target`.
Project: `projects/target_switch` (`always_on` + `prod_only` enabled only when
`target.name == 'prod'`).

`just repro-target-switch`

{table}

- On main, the Variable name and the hashed key args are the same for both
  parses, so prod is a **stale hit** and `prod_only` is missing.
- On the fix, the hashed key args change with the target, prod is a **miss**,
  and `prod_only` is rendered.
- main stale={main_stale}, same hash on main={same_hash_on_main};
  fix ok={fix_ok}, different hash on fix={different_hash_on_fix}.

Raw rows:

```json
{json.dumps(rows, indent=2)}
```
"""
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(body)
    print(table)
    print()
    print(f"main: stale={main_stale} same_hash={same_hash_on_main}")
    print(f"fix:  ok={fix_ok} different_hash={different_hash_on_fix}")
    print(f"wrote {out}")
    return 0 if (main_stale and fix_ok and same_hash_on_main and different_hash_on_fix) else 1


if __name__ == "__main__":
    raise SystemExit(main())
