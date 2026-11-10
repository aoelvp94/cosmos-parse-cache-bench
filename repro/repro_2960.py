"""Repro for astronomer/astronomer-cosmos#2960: the dbt ls cache key ignores what Cosmos asked dbt ls for.

Each step is a fresh DAG parse (scripts/parse_once.py) against a small synthetic project with
source freshness configured. For each step we record whether Cosmos served the dbt ls cache,
how many times dbt ls ran, and a fingerprint of the rendered tasks (task_id:OperatorClass).
A "reference" step re-parses the same configuration with the dbt ls cache disabled, which is
what the DAG *should* look like. A cache hit whose fingerprint differs from the reference is a
stale render.

Scenarios
  A  control: editing a model file invalidates the cache (expected behaviour)
  B  #2960 case 1: pre_dbt_fusion=True, flip source_rendering_behavior none -> all
  C  #2960 case 2: simulated upgrade. "Old" Cosmos requested fewer --output-keys (no freshness/fqn),
     "new" Cosmos requests the full list, project untouched. The "old" list is injected where the Cosmos
     under test builds it (see BENCH_DROP_OUTPUT_KEYS in scripts/parse_once.py)
  E  real upgrade: cache written by Cosmos 1.16.0 (.venv), then read by the Cosmos under test (REPRO_PY)
  D  beyond the issue: switching the dbt target (dev -> prod) with a target-dependent model

Usage: REPRO_PY=.venv/bin/python python repro/repro_2960.py [--label 1.16.0]
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from gen_project import generate  # noqa: E402

PY = Path(os.environ.get("REPRO_PY", ROOT / ".venv" / "bin" / "python"))
HOME = Path(os.environ.get("REPRO_AIRFLOW_HOME", ROOT / ".airflow_home_repro"))
WORK = ROOT / "work" / "repro2960"
CACHE = WORK / "cosmos-cache"


def env_for(project: Path, dag_id: str, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AIRFLOW__COSMOS__", "BENCH_"))}
    env.update(
        AIRFLOW_HOME=str(HOME),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__COSMOS__CACHE_DIR=str(CACHE),
        BENCH_PROJECT_DIR=str(project),
        BENCH_LOAD_MODE="dbt_ls",
        BENCH_DAG_ID=dag_id,
        BENCH_DBT=str(ROOT / "scripts" / "dbt_shim.sh"),
        BENCH_REAL_DBT=str(ROOT / ".venv-dbt" / "bin" / "dbt"),
        BENCH_PROFILES_YML=str(ROOT / "profiles" / "profiles.yml"),
    )
    env.update(extra)
    return env


def parse(env: dict[str, str], py: Path | None = None) -> dict:
    p = subprocess.run([str(py or PY), str(ROOT / "scripts" / "parse_once.py"), str(ROOT / "dags" / "cosmos_bench_dag.py")],
                       env=env, capture_output=True, text=True, timeout=900)
    line = next((ln for ln in p.stdout.splitlines() if ln.startswith("BENCH_RESULT ")), None)
    if not line:
        raise RuntimeError(p.stderr[-3000:])
    r = json.loads(line[13:])
    if r["import_errors"]:
        raise RuntimeError(r["import_errors"])
    return r


def cached_keys(dag_id: str) -> list[str]:
    """Keys present in the first node of the cached dbt ls JSON (decrypted/decompressed from the Airflow Variable)."""
    code = (
        "import sys;from airflow.models.variable import Variable as V;"
        "v=V.get_variable_from_secrets(sys.argv[1]);print(v or '')"
    )
    p = subprocess.run([str(PY), "-c", code, f"cosmos_cache__{dag_id}"], env={**os.environ, "AIRFLOW_HOME": str(HOME)},
                       capture_output=True, text=True)
    raw = p.stdout[p.stdout.find("{"):] if "{" in p.stdout else ""
    if not raw:
        return []
    d = json.loads(raw)
    out = zlib.decompress(base64.b64decode(d["dbt_ls_compressed"])).decode()
    first = next(json.loads(ln) for ln in out.splitlines() if ln.startswith("{"))
    return sorted(first)


def wipe(dag_id: str) -> None:
    shutil.rmtree(CACHE, ignore_errors=True)
    with sqlite3.connect(HOME / "airflow.db") as c:
        c.execute("delete from variable where key like ?", (f"cosmos_cache__{dag_id}%",))


def row(scn: str, step: str, r: dict, ref: dict | None = None, keys: list[str] | None = None) -> dict:
    stale = None
    if ref is not None:
        stale = r["cache"] == "hit" and r["tasks_sha"] != ref["tasks_sha"]
    return {"scenario": scn, "step": step, "cache": r["cache"], "dbt_ls_calls": r["dbt_ls_calls"], "tasks": r["tasks"],
            "tasks_sha": r["tasks_sha"], "ref_sha": ref["tasks_sha"] if ref else "", "stale": stale,
            "cached_keys": ",".join(keys or [])}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="installed")
    a = ap.parse_args()
    if not (HOME / "airflow.db").exists():
        HOME.mkdir(parents=True, exist_ok=True)
        subprocess.run([str(PY.parent / "airflow"), "db", "migrate"], env={**os.environ, "AIRFLOW_HOME": str(HOME)},
                       check=True, capture_output=True)
    proj = WORK / "project"
    generate(30, proj, freshness=True)
    # a model that only exists on the dev target (used by scenario D)
    (proj / "models" / "marts" / "sales" / "dev_only_model.sql").write_text(
        "{{ config(enabled=(target.name == 'dev')) }}\nselect 1 as id\n")
    rows = []

    # A: control
    dag = "r2960_a"; wipe(dag)
    e = env_for(proj, dag)
    rows.append(row("A", "1 cold", parse(e)))
    rows.append(row("A", "2 warm, nothing changed", parse(e)))
    f = proj / "models" / "marts" / "sales" / "dev_only_model.sql"
    orig = f.read_text(); f.write_text(orig + "-- edited\n")
    rows.append(row("A", "3 model file edited", parse(e)))
    f.write_text(orig)

    # B: pre_dbt_fusion + source_rendering_behavior flip
    dag = "r2960_b"; wipe(dag)
    base = {"AIRFLOW__COSMOS__PRE_DBT_FUSION": "True"}
    rows.append(row("B", "1 cold, source_rendering=none (no --output-keys)", parse(env_for(proj, dag, **base, BENCH_SOURCE_RENDERING="none")), keys=cached_keys(dag)))
    ref = parse(env_for(proj, dag, **base, BENCH_SOURCE_RENDERING="all", AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS="False"))
    rows.append(row("B", "ref: source_rendering=all, dbt ls cache off", ref))
    rows.append(row("B", "2 flip to source_rendering=all, cache on", parse(env_for(proj, dag, **base, BENCH_SOURCE_RENDERING="all")), ref, cached_keys(dag)))

    # C: simulated upgrade that adds output keys
    dag = "r2960_c"; wipe(dag)
    old = {"BENCH_DROP_OUTPUT_KEYS": "freshness,fqn", "BENCH_SOURCE_RENDERING": "all"}
    rows.append(row("C", "1 cold, 'old' Cosmos (no freshness,fqn keys)", parse(env_for(proj, dag, **old)), keys=cached_keys(dag)))
    ref = parse(env_for(proj, dag, BENCH_SOURCE_RENDERING="all", AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS="False"))
    rows.append(row("C", "ref: 'new' Cosmos, dbt ls cache off", ref))
    rows.append(row("C", "2 'new' Cosmos, cache on, project untouched", parse(env_for(proj, dag, BENCH_SOURCE_RENDERING="all")), ref, cached_keys(dag)))

    # D: dbt target switch
    dag = "r2960_d"; wipe(dag)
    rows.append(row("D", "1 cold, target=dev", parse(env_for(proj, dag, BENCH_TARGET="dev"))))
    ref = parse(env_for(proj, dag, BENCH_TARGET="prod", AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS="False"))
    rows.append(row("D", "ref: target=prod, dbt ls cache off", ref))
    rows.append(row("D", "2 target=prod, cache on", parse(env_for(proj, dag, BENCH_TARGET="prod")), ref))

    # E: real cross-version upgrade, 1.16.0 writes, the version under test reads
    writer = ROOT / ".venv" / "bin" / "python"
    if writer.absolute() != PY.absolute():
        dag = "r2960_e"; wipe(dag)
        e = env_for(proj, dag, BENCH_SOURCE_RENDERING="all")
        rows.append(row("E", "1 cold, written by Cosmos 1.16.0", parse(e, writer), keys=cached_keys(dag)))
        ref = parse(env_for(proj, dag, BENCH_SOURCE_RENDERING="all", AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS="False"))
        rows.append(row("E", "ref: version under test, dbt ls cache off", ref))
        rows.append(row("E", "2 version under test reads the 1.16.0 cache", parse(e), ref, cached_keys(dag)))

    out = ROOT / "results" / f"repro-2960-{a.label}.md"
    lines = [f"# #2960 repro ({a.label})", "",
             "| scenario | step | cache | dbt ls calls | tasks | tasks sha | reference sha | stale render | keys in cached dbt ls JSON |",
             "|---|---|---|---:|---:|---|---|---|---|"]
    for r in rows:
        st = "" if r["stale"] is None else ("**YES**" if r["stale"] else "no")
        lines.append(f"| {r['scenario']} | {r['step']} | {r['cache']} | {r['dbt_ls_calls']} | {r['tasks']} | {r['tasks_sha']} | {r['ref_sha']} | {st} | {r['cached_keys']} |")
    out.write_text("\n".join(lines) + "\n")
    print(out.read_text())


if __name__ == "__main__":
    main()
