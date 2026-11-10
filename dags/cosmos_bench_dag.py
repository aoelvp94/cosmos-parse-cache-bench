"""Benchmark DAG: one Cosmos DbtDag whose loading strategy is driven by environment variables.

BENCH_PROJECT_DIR   path to the synthetic dbt project
BENCH_LOAD_MODE     dbt_ls | dbt_manifest | dbt_ls_file | automatic
BENCH_MANIFEST      manifest.json for dbt_manifest (default <project>/target/manifest.json)
BENCH_DBT_LS_PATH   dbt ls JSON file for dbt_ls_file
BENCH_DAG_ID        dag_id (also the Cosmos dbt ls cache identifier)
BENCH_DBT           dbt executable (the counting shim)
BENCH_PROFILES_YML  profiles.yml (dbt-duckdb, no credentials)
BENCH_SOURCE_RENDERING  optional, Cosmos SourceRenderingBehavior value (default "none")
BENCH_TARGET        optional, dbt target name (default "dev")
"""
import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, SourceRenderingBehavior

project_dir = Path(os.environ["BENCH_PROJECT_DIR"])
mode = LoadMode(os.environ.get("BENCH_LOAD_MODE", "dbt_ls"))
manifest = Path(os.environ.get("BENCH_MANIFEST", project_dir / "target" / "manifest.json"))

project_kwargs = {"dbt_project_path": project_dir}
if mode in (LoadMode.DBT_MANIFEST, LoadMode.AUTOMATIC) and manifest.exists():
    project_kwargs["manifest_path"] = manifest

render_kwargs = {
    "load_method": mode,
    "dbt_executable_path": os.environ["BENCH_DBT"],
    "invocation_mode": InvocationMode.SUBPROCESS,
    "source_rendering_behavior": SourceRenderingBehavior(os.environ.get("BENCH_SOURCE_RENDERING", "none")),
}
if mode is LoadMode.DBT_LS_FILE:
    render_kwargs["dbt_ls_path"] = Path(os.environ["BENCH_DBT_LS_PATH"])

dag = DbtDag(
    dag_id=os.environ.get("BENCH_DAG_ID", "cosmos_bench"),
    project_config=ProjectConfig(**project_kwargs),
    profile_config=ProfileConfig(
        profile_name="bench",
        target_name=os.environ.get("BENCH_TARGET", "dev"),
        profiles_yml_filepath=Path(os.environ["BENCH_PROFILES_YML"]),
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=os.environ["BENCH_DBT"],
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    render_config=RenderConfig(**render_kwargs),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
