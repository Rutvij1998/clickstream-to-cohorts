"""Manual ingest of one REES46 month. Does not run on a schedule.

param month is 2019-10 or 2019-11. The download step runs only when that
month's CSV is missing.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

CLICKSTREAM_HOME = os.environ.get("CLICKSTREAM_HOME", "/opt/clickstream")
if CLICKSTREAM_HOME not in sys.path:
    sys.path.insert(0, CLICKSTREAM_HOME)

from pipeline.window import REPLAY_START  # noqa: E402


@dag(
    dag_id="ingest_month",
    schedule=None,
    start_date=datetime(REPLAY_START.year, REPLAY_START.month, REPLAY_START.day),
    catchup=False,
    max_active_runs=1,
    params={
        "month": Param("2019-10", enum=["2019-10", "2019-11"]),
    },
    tags=["clickstream"],
)
def ingest_month():
    download_or_fail = BashOperator(
        task_id="download_or_fail",
        bash_command="""
        set -euo pipefail
        month="{{ params.month }}"
        if [ "$month" = "2019-10" ]; then
          file="2019-Oct.csv"
        else
          file="2019-Nov.csv"
        fi
        if [ ! -f "/opt/clickstream/data/raw/$file" ]; then
          bash /opt/clickstream/ingestion/download.sh
        fi
        test -f "/opt/clickstream/data/raw/$file"
        """,
    )
    csv_to_parquet = BashOperator(
        task_id="csv_to_parquet",
        bash_command=("python /opt/clickstream/scripts/ingest_month.py --month {{ params.month }}"),
    )
    sessionize = BashOperator(
        task_id="sessionize",
        bash_command=(
            "export PYTHONPATH=/opt/clickstream; "
            "cd /opt/clickstream; "
            "python -m spark.jobs.sessionize_check "
            "--input /opt/clickstream/data/silver/events "
            "--report /opt/clickstream/data/logs/session_disagreement.json "
            "--pairs /opt/clickstream/data/logs/session_disagreements "
            '--driver-memory "${SPARK_DRIVER_MEMORY:-8g}"'
        ),
    )
    upload_silver = BashOperator(
        task_id="upload_silver",
        bash_command="python /opt/clickstream/scripts/upload_silver.py",
    )
    download_or_fail >> csv_to_parquet >> sessionize >> upload_silver


ingest_month()
