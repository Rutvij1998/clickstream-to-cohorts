"""Export mart tables for Tableau.

local reads the DuckDB dev database and writes CSV files with headers.
athena deletes the prior export prefix, UNLOADs text files, and also saves
one header CSV from the query result.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from pipeline.marts import MART_SCHEMA, MART_TABLES

ROOT = Path(os.environ.get("CLICKSTREAM_HOME", Path(__file__).resolve().parents[1]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export product marts.")
    parser.add_argument(
        "--target",
        choices=["local", "athena"],
        default=os.environ.get("EXPORT_TARGET", "local"),
    )
    return parser.parse_args()


def export_local() -> None:
    import duckdb

    database = ROOT / "dbt" / "target" / "clickstream.duckdb"
    if not database.is_file():
        raise SystemExit(f"Missing {database}. Run make dbt-dev first.")
    destination = ROOT / "data" / "exports"
    destination.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database), read_only=True)
    try:
        for name in MART_TABLES:
            output = destination / f"{name}.csv"
            connection.execute(
                f"copy (select * from {MART_SCHEMA}.{name}) to '{output}' (header, delimiter ',')"
            )
            print(output)
    finally:
        connection.close()


def _wait(client, execution_id: str) -> None:
    while True:
        state = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"][
            "Status"
        ]
        status = state["State"]
        if status == "SUCCEEDED":
            return
        if status in {"FAILED", "CANCELLED"}:
            reason = state.get("StateChangeReason", status)
            raise SystemExit(f"Athena query {execution_id} {status}: {reason}")
        time.sleep(2)


def _start(client, sql: str, workgroup: str) -> str:
    response = client.start_query_execution(QueryString=sql, WorkGroup=workgroup)
    execution_id = response["QueryExecutionId"]
    _wait(client, execution_id)
    return execution_id


def _delete_prefix(s3, bucket: str, prefix: str) -> None:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def export_athena() -> None:
    import boto3

    bucket = os.environ.get("S3_BUCKET", "").strip()
    workgroup = os.environ.get("ATHENA_WORKGROUP", "clickstream-to-cohorts")
    if not bucket:
        raise SystemExit("Set S3_BUCKET before an Athena export.")
    athena = boto3.client("athena")
    s3 = boto3.client("s3")
    for name in MART_TABLES:
        prefix = f"exports/{name}/"
        _delete_prefix(s3, bucket, prefix)
        unload_sql = (
            "UNLOAD ("
            f"SELECT * FROM {MART_SCHEMA}.{name}"
            f") TO 's3://{bucket}/{prefix}' "
            "WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE')"
        )
        _start(athena, unload_sql, workgroup)
        header_id = _start(athena, f"SELECT * FROM {MART_SCHEMA}.{name}", workgroup)
        result_key = f"athena-results/{header_id}.csv"
        s3.copy_object(
            Bucket=bucket,
            Key=f"exports/{name}.csv",
            CopySource={"Bucket": bucket, "Key": result_key},
        )
        print(f"s3://{bucket}/{prefix} and s3://{bucket}/exports/{name}.csv")


def main() -> None:
    args = parse_args()
    if args.target == "local":
        export_local()
    else:
        export_athena()


if __name__ == "__main__":
    main()
