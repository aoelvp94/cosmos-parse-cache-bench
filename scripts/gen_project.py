"""Synthesize a dbt project with N models and a realistic layered DAG shape.

Layout (per domain folder):
  models/staging/<domain>/stg_*.sql        -> each selects from one source table
  models/intermediate/<domain>/int_*.sql   -> refs 2-4 staging/earlier intermediate models
  models/marts/<domain>/fct_*/dim_*.sql    -> refs 2-6 intermediate/staging models (fan-in)
  one _schema.yml per folder with unique + not_null tests on every model,
  one _sources.yml per staging domain.

Deterministic for a given (N, seed), so every reviewer gets byte-identical projects.
"""
from __future__ import annotations

import argparse
import random
import shutil
from pathlib import Path

DOMAINS = ["sales", "finance", "marketing", "product", "support", "ops", "hr", "logistics", "billing", "risk"]
SHARE = {"staging": 0.40, "intermediate": 0.35, "marts": 0.25}

PROJECT_YML = """name: bench_{n}
version: "1.0.0"
config-version: 2
profile: bench
model-paths: ["models"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]
models:
  bench_{n}:
    +materialized: view
"""


def model_sql(cols: list[str], refs: list[str] | None = None, source: tuple[str, str] | None = None) -> str:
    if source:
        return (
            "with src as (\n    select * from {{ source('%s', '%s') }}\n)\n"
            "select\n    id,\n    %s\nfrom src\n" % (source[0], source[1], ",\n    ".join(cols))
        )
    assert refs
    head, *rest = refs
    lines = [f"with base as (\n    select * from {{{{ ref('{head}') }}}}\n)"]
    for i, r in enumerate(rest):
        lines.append(f", j{i} as (\n    select id, count(*) as c{i} from {{{{ ref('{r}') }}}} group by id\n)")
    sel = "select\n    base.id,\n    " + ",\n    ".join(f"j{i}.c{i}" for i in range(len(rest)) or ["1 as one"]) if rest else "select\n    base.id"
    body = "\n".join(lines) + "\n" + sel + "\nfrom base\n"
    body += "".join(f"left join j{i} on base.id = j{i}.id\n" for i in range(len(rest)))
    return body


def schema_yml(models: list[str]) -> str:
    out = ["version: 2", "models:"]
    for m in models:
        out += [
            f"  - name: {m}",
            f"    description: Synthetic model {m}",
            "    columns:",
            "      - name: id",
            "        data_tests:",
            "          - unique",
            "          - not_null",
        ]
    return "\n".join(out) + "\n"


def sources_yml(src: str, tables: list[str], freshness: bool = False) -> str:
    out = ["version: 2", "sources:", f"  - name: {src}", "    schema: raw"]
    if freshness:
        out += [
            "    config:",
            "      loaded_at_field: updated_at",
            "      freshness:",
            "        warn_after: {count: 24, period: hour}",
        ]
    out += ["    tables:"]
    out += [f"      - name: {t}" for t in tables]
    return "\n".join(out) + "\n"


def generate(n: int, out: Path, seed: int = 42, freshness: bool = False) -> dict[str, int]:
    rnd = random.Random(seed)
    if out.exists():
        shutil.rmtree(out)
    (out / "models").mkdir(parents=True)
    (out / "dbt_project.yml").write_text(PROJECT_YML.format(n=n))

    counts = {k: max(1, int(n * v)) for k, v in SHARE.items()}
    counts["marts"] = n - counts["staging"] - counts["intermediate"]
    layers: dict[str, list[tuple[str, str]]] = {k: [] for k in SHARE}  # (domain, name)

    for i in range(counts["staging"]):
        d = DOMAINS[i % len(DOMAINS)]
        layers["staging"].append((d, f"stg_{d}__t{i:05d}"))
    for i in range(counts["intermediate"]):
        d = DOMAINS[i % len(DOMAINS)]
        layers["intermediate"].append((d, f"int_{d}__m{i:05d}"))
    for i in range(counts["marts"]):
        d = DOMAINS[i % len(DOMAINS)]
        kind = "fct" if i % 3 else "dim"
        layers["marts"].append((d, f"{kind}_{d}__m{i:05d}"))

    edges = 0
    files: dict[Path, list[str]] = {}
    # staging: one source each
    src_tables: dict[str, list[str]] = {}
    for d, name in layers["staging"]:
        tbl = name.split("__")[1]
        src_tables.setdefault(d, []).append(tbl)
        p = out / "models" / "staging" / d
        p.mkdir(parents=True, exist_ok=True)
        (p / f"{name}.sql").write_text(model_sql(["col_a", "col_b", "updated_at"], source=(f"raw_{d}", tbl)))
        files.setdefault(p, []).append(name)
    for d, tbls in src_tables.items():
        (out / "models" / "staging" / d / "_sources.yml").write_text(sources_yml(f"raw_{d}", tbls, freshness))

    stg_names = [n_ for _, n_ in layers["staging"]]
    int_done: list[str] = []
    for d, name in layers["intermediate"]:
        pool_same = [x for x in stg_names if f"_{d}__" in x] or stg_names
        k = rnd.randint(2, 4)
        refs = rnd.sample(pool_same, min(k - 1, len(pool_same)))
        if int_done and rnd.random() < 0.5:
            refs.append(rnd.choice(int_done[-50:]))  # chains inside intermediate
        else:
            refs.append(rnd.choice(stg_names))
        refs = list(dict.fromkeys(refs))
        edges += len(refs)
        p = out / "models" / "intermediate" / d
        p.mkdir(parents=True, exist_ok=True)
        (p / f"{name}.sql").write_text(model_sql([], refs=refs))
        files.setdefault(p, []).append(name)
        int_done.append(name)

    upstream = int_done or stg_names
    for d, name in layers["marts"]:
        k = rnd.randint(2, 6)
        refs = rnd.sample(upstream, min(k, len(upstream)))
        if rnd.random() < 0.3:
            refs.append(rnd.choice(stg_names))
        refs = list(dict.fromkeys(refs))
        edges += len(refs)
        p = out / "models" / "marts" / d
        p.mkdir(parents=True, exist_ok=True)
        (p / f"{name}.sql").write_text(model_sql([], refs=refs))
        files.setdefault(p, []).append(name)

    for p, models in files.items():
        (p / "_schema.yml").write_text(schema_yml(models))

    return {
        "models": n,
        "staging": counts["staging"],
        "intermediate": counts["intermediate"],
        "marts": counts["marts"],
        "sources": counts["staging"],
        "tests": 2 * n,
        "ref_edges": edges,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", "-n", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--freshness", action="store_true", help="add source freshness config (used by the #2960 repro)")
    a = ap.parse_args()
    print(generate(a.models, a.out, a.seed, a.freshness))
