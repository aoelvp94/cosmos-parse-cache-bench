"""Repro for astronomer/astronomer-cosmos#2739: rendered profile contents are not logged when profile caching is on.

Runs ProfileConfig.ensure_profile() with a PostgresUserPasswordProfileMapping (no database needed: the profile is only
rendered, never used) in three fresh processes and captures the INFO log lines Cosmos emits:

  1. enable_cache_profile=True, empty cache   -> cache miss
  2. enable_cache_profile=True, same cache    -> cache hit
  3. enable_cache_profile=False               -> non-cached branch (reference behaviour)

For every case it reports whether the rendered profiles.yml contents appear in the logs and whether the connection
password leaks into the logs or into the rendered file.

Usage: REPRO_PY=.venv/bin/python python repro/repro_2739.py [--label 1.16.0]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = Path(os.environ.get("REPRO_PY", ROOT / ".venv" / "bin" / "python"))
WORK = ROOT / "work" / "repro2739"
SECRET = "s3cr3t-P4ssw0rd-do-not-log"

CHILD = r'''
import json, logging, os, sys
from pathlib import Path
lines = []
class H(logging.Handler):
    def emit(self, r): lines.append(r.getMessage())
lg = logging.getLogger("cosmos"); lg.addHandler(H()); lg.setLevel(logging.INFO)
from cosmos.config import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
pc = ProfileConfig(profile_name="bench", target_name="dev",
                   profile_mapping=PostgresUserPasswordProfileMapping(conn_id="pg_bench", profile_args={"schema": "public"}))
with pc.ensure_profile() as (path, env):
    rendered = Path(path).read_text()
print("RESULT " + json.dumps({"lines": lines, "rendered": rendered, "env_keys": sorted(env)}))
'''


def run(cache_on: bool) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith("AIRFLOW__COSMOS__")}
    env.update(
        AIRFLOW_HOME=str(WORK / "airflow_home"),
        AIRFLOW__COSMOS__CACHE_DIR=str(WORK / "cache"),
        AIRFLOW__COSMOS__ENABLE_CACHE_PROFILE=str(cache_on),
        AIRFLOW_CONN_PG_BENCH=json.dumps({"conn_type": "postgres", "host": "db.example.internal", "login": "bench_user",
                                          "password": SECRET, "port": 5432, "schema": "analytics"}),
    )
    p = subprocess.run([str(PY), "-c", CHILD], env=env, capture_output=True, text=True, timeout=300)
    line = next((ln for ln in p.stdout.splitlines() if ln.startswith("RESULT ")), None)
    if not line:
        raise RuntimeError(p.stderr[-3000:])
    return json.loads(line[7:])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="installed")
    a = ap.parse_args()
    shutil.rmtree(WORK, ignore_errors=True)
    (WORK / "airflow_home").mkdir(parents=True)
    cases = [("cache on, miss", True), ("cache on, hit", True), ("cache off (reference)", False)]
    md = [f"# #2739 repro ({a.label})", "",
          "| case | profile log lines | rendered contents logged | password in logs | password in rendered file |",
          "|---|---|---|---|---|"]
    details = []
    for name, on in cases:
        r = run(on)
        prof_lines = [ln for ln in r["lines"] if "rofile" in ln]
        contents_logged = any("outputs:" in ln for ln in r["lines"])
        leak_logs = any(SECRET in ln for ln in r["lines"])
        leak_file = SECRET in r["rendered"]
        md.append(f"| {name} | {len(prof_lines)} | {'yes' if contents_logged else '**no**'} | "
                  f"{'**YES**' if leak_logs else 'no'} | {'**YES**' if leak_file else 'no'} |")
        details.append(f"### {name}\n\n```text\n" + "\n".join(prof_lines) + "\n```\n")
    out = ROOT / "results" / f"repro-2739-{a.label}.md"
    out.write_text("\n".join(md) + "\n\nCaptured Cosmos log lines that mention the profile:\n\n" + "\n".join(details))
    print(out.read_text())


if __name__ == "__main__":
    main()
