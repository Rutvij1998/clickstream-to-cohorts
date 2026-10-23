"""Replay 2019-10-01 through 2019-11-30, one logical date per run.

end_date is exclusive. 2019-12-01 keeps the logical date 2019-11-30 and drops
2019-12-01. That is 61 days. catchup is on and only one run is active.

Each run checks the local Parquet partition, refreshes that event_date in
DuckDB, runs tests, and exports mart CSV files. Run `make run` once first so
the warehouse already holds every ingested day. A catchup on an empty
warehouse only contains the days completed so far.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

CLICKSTREAM_HOME = os.environ.get("CLICKSTREAM_HOME", "/opt/clickstream")
if CLICKSTREAM_HOME not in sys.path:
    sys.path.insert(0, CLICKSTREAM_HOME)

from pipeline.window import REPLAY_END_EXCLUSIVE, REPLAY_START  # noqa: E402

DBT_VARS = ' --vars \'{"event_date": "{{ ds }}", "silver_source": "parquet"}\''


def _dbt(command: str) -> str:
    return (
        "set -euo pipefail; "
        "cd /opt/clickstream/dbt; "
        "dbt deps --profiles-dir .; "
        f"dbt {command} --target dev --profiles-dir . "
        "--select seeds staging intermediate core marts"
        f"{DBT_VARS}"
    )


@dag(
    dag_id="build_daily",
    schedule="@daily",
    start_date=datetime(REPLAY_START.year, REPLAY_START.month, REPLAY_START.day),
    end_date=REPLAY_END_EXCLUSIVE,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["clickstream"],
)
def build_daily():
    check_partition = BashOperator(
        task_id="check_partition",
        bash_command=("python /opt/clickstream/scripts/check_partition.py --date {{ ds }}"),
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=_dbt("build"),
    )
    data_quality_gate = BashOperator(
        task_id="data_quality_gate",
        bash_command=_dbt("test"),
    )
    export_marts = BashOperator(
        task_id="export_marts",
        bash_command="python /opt/clickstream/scripts/export_marts.py",
    )
    check_partition >> dbt_build >> data_quality_gate >> export_marts


build_daily()
