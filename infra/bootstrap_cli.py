"""Create the bucket, Glue databases, Athena workgroup, and IAM policy with boto3.

Use this when Terraform is not installed. Prefer `make infra-apply` otherwise.
The script does not create access keys.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
INFRA = Path(__file__).resolve().parent


def require_bucket() -> str:
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not bucket or "/" in bucket or " " in bucket:
        raise SystemExit("Set S3_BUCKET to a new bucket name before running this script.")
    return bucket


def render_ddl(bucket: str) -> str:
    sql = (INFRA / "athena_ddl.sql").read_text(encoding="utf-8")
    if "BUCKET_NAME" not in sql:
        raise SystemExit("infra/athena_ddl.sql is missing BUCKET_NAME")
    return sql.replace("BUCKET_NAME", bucket)


def render_policy(bucket: str, region: str, account_id: str) -> str:
    text = (INFRA / "iam_policy.json").read_text(encoding="utf-8")
    return (
        text.replace("BUCKET_NAME", bucket)
        .replace("REGION", region)
        .replace("ACCOUNT_ID", account_id)
    )


def ensure_bucket(s3, bucket: str, region: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket exists: {bucket}")
    except ClientError:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"Created bucket: {bucket}")
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        },
    )
    lifecycle = json.loads((INFRA / "s3_lifecycle.json").read_text(encoding="utf-8"))
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)


def ensure_workgroup(athena, bucket: str) -> None:
    name = os.environ.get("ATHENA_WORKGROUP", "clickstream-to-cohorts")
    output = f"s3://{bucket}/athena-results/"
    encryption = {"EncryptionOption": "SSE_S3"}
    create_config = {
        "ResultConfiguration": {
            "OutputLocation": output,
            "EncryptionConfiguration": encryption,
        },
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": False,
        "BytesScannedCutoffPerQuery": 10737418240,
    }
    update_config = {
        "ResultConfigurationUpdates": {
            "OutputLocation": output,
            "EncryptionConfiguration": encryption,
        },
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": False,
        "BytesScannedCutoffPerQuery": 10737418240,
    }
    try:
        athena.create_work_group(Name=name, Configuration=create_config)
        print(f"Created workgroup: {name}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "InvalidRequestException":
            raise
        athena.update_work_group(WorkGroup=name, ConfigurationUpdates=update_config)
        print(f"Updated workgroup: {name}")


def run_ddl(athena, bucket: str) -> None:
    workgroup = os.environ.get("ATHENA_WORKGROUP", "clickstream-to-cohorts")
    statements = [part.strip() for part in render_ddl(bucket).split(";") if part.strip()]
    for statement in statements:
        execution_id = athena.start_query_execution(QueryString=statement, WorkGroup=workgroup)[
            "QueryExecutionId"
        ]
        while True:
            status = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"][
                "Status"
            ]
            state = status["State"]
            if state == "SUCCEEDED":
                break
            if state in {"FAILED", "CANCELLED"}:
                raise SystemExit(status.get("StateChangeReason", state))
            time.sleep(2)
        print(f"Applied: {statement.splitlines()[0]}")


def ensure_policy(iam, bucket: str, region: str, account_id: str) -> str:
    document = render_policy(bucket, region, account_id)
    name = "clickstream-to-cohorts"
    try:
        response = iam.create_policy(
            PolicyName=name,
            PolicyDocument=document,
            Description="One bucket, the project Glue databases, and the project Athena workgroup.",
        )
        arn = response["Policy"]["Arn"]
        print(f"Created policy: {arn}")
        return arn
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        arn = f"arn:aws:iam::{account_id}:policy/{name}"
        print(f"Policy already exists: {arn}")
        return arn


def maybe_budget(account_id: str) -> None:
    email = os.environ.get("BUDGET_ALERT_EMAIL", "").strip()
    if not email:
        print("BUDGET_ALERT_EMAIL is empty, so no budget alert was created.")
        return
    budgets = boto3.client("budgets")
    try:
        budgets.create_budget(
            AccountId=account_id,
            Budget={
                "BudgetName": "clickstream-to-cohorts-monthly",
                "BudgetLimit": {"Amount": "5", "Unit": "USD"},
                "TimeUnit": "MONTHLY",
                "BudgetType": "COST",
                "TimePeriod": {"Start": "2026-10-01T00:00:00Z"},
            },
            NotificationsWithSubscribers=[
                {
                    "Notification": {
                        "NotificationType": "ACTUAL",
                        "ComparisonOperator": "GREATER_THAN",
                        "Threshold": 80,
                        "ThresholdType": "PERCENTAGE",
                    },
                    "Subscribers": [{"SubscriptionType": "EMAIL", "Address": email}],
                }
            ],
        )
        print("Created monthly 5 USD budget alert.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "DuplicateRecordException":
            raise
        print("Budget alert already exists.")


def main() -> None:
    bucket = require_bucket()
    region = os.environ.get("AWS_REGION", "us-east-1")
    session = boto3.Session(region_name=region)
    identity = session.client("sts").get_caller_identity()
    account_id = identity["Account"]
    ensure_bucket(session.client("s3"), bucket, region)
    ensure_workgroup(session.client("athena"), bucket)
    run_ddl(session.client("athena"), bucket)
    arn = ensure_policy(session.client("iam"), bucket, region, account_id)
    maybe_budget(account_id)
    print("Attach the policy to your IAM user or role:")
    print(f"  aws iam attach-user-policy --user-name YOUR_USER --policy-arn {arn}")
    print("Then set these in .env:")
    print(f"  S3_BUCKET={bucket}")
    print(f"  ATHENA_S3_STAGING_DIR=s3://{bucket}/athena-results/")
    print(f"  ATHENA_S3_DATA_DIR=s3://{bucket}/dbt/")


if __name__ == "__main__":
    sys.exit(main() or 0)
