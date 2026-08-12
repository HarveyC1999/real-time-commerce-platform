from __future__ import annotations

import shutil
import subprocess
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task

PROJECT_DIRECTORY = "/opt/commerce"
DBT_DIRECTORY = f"{PROJECT_DIRECTORY}/dbt"


@dag(
    dag_id="commerce_analytics",
    description="Load finalized Gold aggregates and build tested dbt marts.",
    schedule="0 * * * *",
    start_date=datetime(2026, 8, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["commerce", "analytics"],
)
def commerce_analytics():
    @task
    def load_gold_analytics() -> None:
        from scripts.load_analytics import main

        main()

    @task
    def build_dbt_models() -> None:
        dbt_executable = shutil.which("dbt")
        if dbt_executable is None:
            raise RuntimeError("dbt executable is not installed in the Airflow image.")

        subprocess.run(
            [
                dbt_executable,
                "build",
                "--project-dir",
                DBT_DIRECTORY,
                "--profiles-dir",
                DBT_DIRECTORY,
            ],
            cwd=PROJECT_DIRECTORY,
            check=True,
        )

    load_gold_analytics() >> build_dbt_models()


commerce_analytics()
