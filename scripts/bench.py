"""Cosmos DAG-parse benchmark: N models x loading strategy x cold/warm, R rounds, medians.

Every measurement is a fresh Python process (scripts/parse_once.py), the way a DAG processor
re-parses a file. Results go to results/raw-<tag>.jsonl (one line per run) and
results/summary-<tag>.md (median table).

Variants
  dbt_ls_nocache   LoadMode.DBT_LS, AIRFLOW__COSMOS__ENABLE_CACHE=False (no dbt ls cache, no partial-parse cache)
  dbt_ls_pp_only   LoadMode.DBT_LS, dbt ls cache off, Cosmos partial-parse cache on
  dbt_ls_cache     LoadMode.DBT_LS, Cosmos defaults (dbt ls cache in an Airflow Variable + partial-parse cache)
  dbt_manifest     LoadMode.DBT_MANIFEST from a pre-built target/manifest.json (dbt parse cost reported separately)

cold = Cosmos cache dir and cosmos_cache__* Variables wiped before the run.
warm = the run right after a cold run, caches kept, project untouched.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from gen_project import generate  # noqa: E402

PY = ROOT / ".venv" / "bin" / "python"
DBT = ROOT / ".venv-dbt" / "bin" / "dbt"
WORK = ROOT / "work"
AIRFLOW_HOME = ROOT / ".airflow_home"
CACHE_DIR = WORK / "cosmos-cache"

VARIANTS = {
    "dbt_ls_nocache": {"mode": "dbt_ls", "env": {"AIRFLOW__COSMOS__ENABLE_CACHE": "False"}},
    "dbt_ls_pp_only": {"mode": "dbt_ls", "env": {"AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS": "False"}},
    "dbt_ls_cache": {"mode": "dbt_ls", "env": {}},
    "dbt_manifest": {"mode": "dbt_manifest", "env": {}},
}


def base_env(project: Path, dag_id: str, mode: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("AIRFLOW__COSMOS__")}
    env.update(
        AIRFLOW_HOME=str(AIRFLOW_HOME),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__DAGS_FOLDER=str(ROOT / "dags"),
        AIRFLOW__LOGGING__LOGGING_LEVEL="INFO",
        AIRFLOW__COSMOS__CACHE_DIR=str(CACHE_DIR),
        BENCH_PROJECT_DIR=str(project),
        BENCH_LOAD_MODE=mode,
        BENCH_MANIFEST=str(WORK / "manifests" / project.name / "manifest.json"),
        BENCH_DAG_ID=dag_id,
        BENCH_DBT=str(ROOT / "scripts" / "dbt_shim.sh"),
        BENCH_REAL_DBT=str(DBT),
        BENCH_PROFILES_YML=str(ROOT / "profiles" / "profiles.yml"),
    )
    return env


def wipe_caches(dag_id: str) -> None:
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    db = AIRFLOW_HOME / "airflow.db"
    with sqlite3.connect(db) as c:
        c.execute("delete from variable where key like ?", (f"cosmos_cache__{dag_id}%",))


def parse_once(env: dict[str, str]) -> dict:
    t0 = time.perf_counter()
    p = subprocess.run(
        [str(PY), str(ROOT / "scripts" / "parse_once.py"), str(ROOT / "dags" / "cosmos_bench_dag.py")],
        env=env, capture_output=True, text=True, timeout=1800,
    )
    wall = time.perf_counter() - t0
    line = next((ln for ln in p.stdout.splitlines() if ln.startswith("BENCH_RESULT ")), None)
    if not line:
        raise RuntimeError(f"parse failed rc={p.returncode}\n{p.stderr[-3000:]}")
    res = json.loads(line[len("BENCH_RESULT "):])
    res["process_wall_s"] = round(wall, 3)
    if res["import_errors"]:
        err = " ".join(res["import_errors"].values())
        if "AirflowTaskTimeout" in err or "import timeout" in err:
            # A real outcome, not a harness failure: the DAG file did not import within
            # [core] dagbag_import_timeout, so the DAG processor would report an import error.
            res["status"] = "import_timeout"
            res["parse_s"] = None
            return res
        raise RuntimeError(f"import errors: {res['import_errors']}")
    res["status"] = "ok"
    return res


def prepare_project(n: int) -> tuple[Path, dict, float]:
    proj = WORK / "projects" / f"n{n}"
    shape = generate(n, proj)
    t0 = time.perf_counter()
    subprocess.run(
        [str(DBT), "parse", "--profiles-dir", str(ROOT / "profiles"), "--project-dir", str(proj), "-q"],
        check=True, capture_output=True, cwd=proj,
    )
    elapsed = time.perf_counter() - t0
    # Keep the manifest outside the project and drop target/: otherwise Cosmos would pick up
    # target/partial_parse.msgpack from the project folder and no dbt ls run would ever be cold.
    mdir = WORK / "manifests" / f"n{n}"
    mdir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(proj / "target" / "manifest.json"), mdir / "manifest.json")
    shutil.rmtree(proj / "target", ignore_errors=True)
    shutil.rmtree(proj / "logs", ignore_errors=True)
    return proj, shape, elapsed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="100,500,1000,2500,5000")
    ap.add_argument("--variants", default=",".join(VARIANTS))
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--tag", default=datetime.now().strftime("%Y%m%d-%H%M"))
    ap.add_argument("--import-timeout", type=float, default=None,
                    help="[core] dagbag_import_timeout in seconds (default: Airflow's own default, 30)")
    a = ap.parse_args()

    out_dir = ROOT / "results"
    out_dir.mkdir(exist_ok=True)
    raw = out_dir / f"raw-{a.tag}.jsonl"
    rows = []
    for n in [int(x) for x in a.sizes.split(",")]:
        proj, shape, dbt_parse_s = prepare_project(n)
        print(f"[n={n}] shape={shape} dbt_parse_s={dbt_parse_s:.2f}", flush=True)
        for vname in a.variants.split(","):
            v = VARIANTS[vname]
            dag_id = f"bench_{vname}_{n}"
            env = base_env(proj, dag_id, v["mode"])
            env.update(v["env"])
            if a.import_timeout is not None:
                env["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = str(a.import_timeout)
            for r in range(a.rounds):
                wipe_caches(dag_id)
                phases = ["cold", "warm"]
                for phase in phases:
                    res = parse_once(env)
                    rec = {"n": n, "variant": vname, "round": r, "phase": phase, "dbt_parse_s": round(dbt_parse_s, 3), **shape, **res}
                    rec.pop("cosmos_msgs", None)
                    rows.append(rec)
                    with raw.open("a") as fp:
                        fp.write(json.dumps(rec) + "\n")
                    ps = "TIMEOUT" if res["parse_s"] is None else f"{res['parse_s']:.2f}s"
                    print(f"  {vname:15s} r{r} {phase}: parse={ps} tasks={res['tasks']} "
                          f"dbt_ls={res['dbt_ls_calls']} cache={res['cache']} rss={res['rss_peak_mb']}MB "
                          f"child={res['child_peak_mb']}MB", flush=True)
    write_summary(rows, out_dir / f"summary-{a.tag}.md")


def summarize_file(raw: Path, out: Path) -> None:
    write_summary([json.loads(ln) for ln in raw.read_text().splitlines() if ln.strip()], out)


def write_summary(rows: list[dict], path: Path) -> None:
    keys = sorted({(r["n"], r["variant"], r["phase"]) for r in rows}, key=lambda k: (k[0], list(VARIANTS).index(k[1]), k[2]))
    lines = [
        "| models | variant | phase | parse s (median) | min | max | tasks | dbt ls calls | cache | peak RSS MB | dbt child peak MB | rounds |",
        "|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|",
    ]
    for n, v, ph in keys:
        rs = [r for r in rows if (r["n"], r["variant"], r["phase"]) == (n, v, ph)]
        ps = [r["parse_s"] for r in rs if r["parse_s"] is not None]
        timeouts = len(rs) - len(ps)
        if not ps:
            lines.append(f"| {n} | {v} | {ph} | import timeout | | | 0 | {statistics.median([r['dbt_ls_calls'] for r in rs]):g} | "
                         f"{'/'.join(sorted({r['cache'] for r in rs}))} | | | {len(rs)} ({timeouts} timeouts) |")
            continue
        lines.append(
            f"| {n} | {v} | {ph} | {statistics.median(ps):.2f} | {min(ps):.2f} | {max(ps):.2f} | {max(r['tasks'] for r in rs)} | "
            f"{statistics.median([r['dbt_ls_calls'] for r in rs]):g} | {'/'.join(sorted({r['cache'] for r in rs}))} | "
            f"{statistics.median([r['rss_peak_mb'] for r in rs]):.0f} | {statistics.median([r['child_peak_mb'] for r in rs]):.0f} | "
            f"{len(rs)}{f' ({timeouts} timeouts)' if timeouts else ''} |"
        )
    path.write_text("\n".join(lines) + "\n")
    print(path.read_text())


if __name__ == "__main__":
    main()
