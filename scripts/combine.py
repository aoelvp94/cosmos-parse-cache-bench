"""Combine the raw benchmark JSONL files into one median table (results/summary-all.md).

Sources: raw-concurrent-100-2500.jsonl (n=100..2500, four variants), raw-n5000-timeout600.jsonl
(n=5000, four variants, dagbag_import_timeout=600) and raw-ls-file-100-5000.jsonl (DBT_LS_FILE, all sizes).
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
FILES = ["raw-concurrent-100-2500.jsonl", "raw-n5000-timeout600.jsonl", "raw-ls-file-100-5000.jsonl"]
ORDER = ["dbt_ls_nocache", "dbt_ls_pp_only", "dbt_ls_cache", "dbt_manifest", "dbt_ls_file"]

rows = [json.loads(ln) for f in FILES for ln in (RES / f).read_text().splitlines() if ln.strip()]
sizes = sorted({r["n"] for r in rows})


def med(n: int, v: str, ph: str, key: str = "parse_s") -> float | None:
    xs = [r[key] for r in rows if (r["n"], r["variant"], r["phase"]) == (n, v, ph) and r.get(key) is not None]
    return statistics.median(xs) if xs else None


out = ["| models | " + " | ".join(f"{v} {ph}" for v in ORDER for ph in ("cold", "warm")) + " |",
       "|---:|" + "---:|" * (2 * len(ORDER))]
for n in sizes:
    cells = []
    for v in ORDER:
        for ph in ("cold", "warm"):
            m = med(n, v, ph)
            cells.append("" if m is None else f"{m:.2f}")
    out.append(f"| {n} | " + " | ".join(cells) + " |")

calls = ["", "| variant | dbt ls calls cold | dbt ls calls warm | cache cold | cache warm |", "|---|---:|---:|---|---|"]
for v in ORDER:
    rs = [r for r in rows if r["variant"] == v]
    c = {ph: sorted({r["dbt_ls_calls"] for r in rs if r["phase"] == ph}) for ph in ("cold", "warm")}
    k = {ph: "/".join(sorted({r["cache"] for r in rs if r["phase"] == ph})) for ph in ("cold", "warm")}
    calls.append(f"| {v} | {','.join(map(str, c['cold']))} | {','.join(map(str, c['warm']))} | {k['cold']} | {k['warm']} |")

rss = ["", "| models | peak RSS MB (dbt_ls_cache warm) | dbt child peak MB (dbt ls run) | dbt parse s (manifest build) |", "|---:|---:|---:|---:|"]
for n in sizes:
    rss.append(f"| {n} | {med(n, 'dbt_ls_cache', 'warm', 'rss_peak_mb'):.0f} | {med(n, 'dbt_ls_nocache', 'cold', 'child_peak_mb'):.0f} | "
               f"{med(n, 'dbt_manifest', 'cold', 'dbt_parse_s'):.1f} |")

text = "\n".join(out + calls + rss) + "\n"
(RES / "summary-all.md").write_text(text)
print(text)
