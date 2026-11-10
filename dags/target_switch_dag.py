"""Minimal DbtDag whose dbt target is switchable at parse time.

The target comes from the Airflow Variable ``target_switch_dbt_target`` (default
``dev``), or from the ``BENCH_TARGET`` environment variable if that is set.
This Variable is *not* listed in ``RenderConfig.airflow_vars_to_purge_dbt_ls_cache``:
the point of the repro is whether the dbt ls cache key itself includes the
profile/target, not whether an operator opted into a manual purge variable.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models import Variable
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode

ROOT = Path(__file__).resolve().parents[1]
project_dir = Path(os.environ.get("TARGET_SWITCH_PROJECT", ROOT / "projects" / "target_switch"))
profiles = Path(os.environ.get("BENCH_PROFILES_YML", ROOT / "profiles" / "profiles.yml"))
dbt = os.environ["BENCH_DBT"]

target = os.environ.get("BENCH_TARGET") or Variable.get("target_switch_dbt_target", default_var="dev")

dag = DbtDag(
    dag_id=os.environ.get("BENCH_DAG_ID", "target_switch"),
    project_config=ProjectConfig(dbt_project_path=project_dir, partial_parse=False),
    profile_config=ProfileConfig(
        profile_name="bench",
        target_name=target,
        profiles_yml_filepath=profiles,
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=dbt,
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        dbt_executable_path=dbt,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_deps=False,
        enable_mock_profile=False,
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
