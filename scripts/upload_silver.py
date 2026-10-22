"""Upload local silver partitions to S3, replacing each event_date prefix."""

from __future__ import annotations

import os
from pathlib import Path

import boto3

ROOT = Path(os.environ.get("CLICKSTREAM_HOME", Path(__file__).resolve().parents[1]))


def main() -> None:
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not bucket:
        raise SystemExit("Set S3_BUCKET before uploading silver Parquet.")
    silver = ROOT / "data" / "silver" / "events"
    partitions = (
        sorted(
            path
            for path in silver.iterdir()
            if path.is_dir() and path.name.startswith("event_date=")
        )
        if silver.is_dir()
        else []
    )
    if not partitions:
        raise SystemExit(f"No partitions under {silver}. Run make ingest first.")

    client = boto3.client("s3")
    for partition in partitions:
        prefix = f"silver/events/{partition.name}/"
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        uploaded = 0
        for file_path in partition.iterdir():
            if not file_path.is_file() or file_path.name.startswith("."):
                continue
            client.upload_file(str(file_path), bucket, prefix + file_path.name)
            uploaded += 1
        print(f"s3://{bucket}/{prefix} files={uploaded}")


if __name__ == "__main__":
    main()
