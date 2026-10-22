"""Compare the Spark run log with an Athena count of events_silver.

Prints both numbers. Exits 1 when they differ. Does not embed an expected count.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def spark_output_rows() -> int:
    logs = sorted((ROOT / "data" / "logs").glob("2019-*.json"))
    if not logs:
        raise SystemExit("No data/logs/2019-*.json run logs. Run make ingest for each month.")
    total = 0
    for path in logs:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "output_rows" not in payload:
            continue
        total += int(payload["output_rows"])
        print(f"{path.name} output_rows={payload['output_rows']}")
    return total


def athena_count() -> int:
    import boto3

    database = os.environ.get("GLUE_DATABASE", "rees46")
    workgroup = os.environ.get("ATHENA_WORKGROUP", "clickstream-to-cohorts")
    client = boto3.client("athena")
    query = f"SELECT count(*) AS row_count FROM {database}.events_silver"
    execution_id = client.start_query_execution(QueryString=query, WorkGroup=workgroup)[
        "QueryExecutionId"
    ]
    while True:
        status = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"][
            "Status"
        ]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            raise SystemExit(status.get("StateChangeReason", state))
        time.sleep(2)
    rows = client.get_query_results(QueryExecutionId=execution_id)["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])


def main() -> None:
    logged = spark_output_rows()
    counted = athena_count()
    print(f"spark_output_rows={logged}")
    print(f"athena_count={counted}")
    if logged != counted:
        raise SystemExit("Spark output_rows and the Athena count differ.")


if __name__ == "__main__":
    main()
