"""Typed, deduped, date-partitioned Parquet from a REES46 CSV.

Reads one month at a time. Reruns overwrite only the event_date partitions
present in that file (`partitionOverwriteMode=dynamic`).

Run log keys are counts and null rates from this invocation. They are not
checked into git.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark.jobs.session import build_spark

RAW_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
    ]
)

VALID_EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")
OUTPUT_COLUMNS = [
    "event_id",
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_date",
]
DEDUP_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RAW_SCHEMA)


def transform(raw: DataFrame) -> tuple[DataFrame, dict]:
    """Return the silver frame and a run log.

    The frame is cached by the caller via the log actions.
    """
    parsed_time = F.to_timestamp(
        F.regexp_replace(F.col("event_time"), r"\s+UTC$", ""),
        "yyyy-MM-dd HH:mm:ss",
    )
    event_type = F.lower(F.trim(F.col("event_type")))
    product_id = F.col("product_id").cast("long")
    category_id = F.col("category_id").cast("long")
    user_id = F.col("user_id").cast("long")
    price = F.col("price").cast("double")
    category_code = F.nullif(F.trim(F.col("category_code")), F.lit(""))
    brand = F.nullif(F.trim(F.col("brand")), F.lit(""))
    user_session = F.nullif(F.trim(F.col("user_session")), F.lit(""))

    typed = raw.select(
        parsed_time.alias("event_time"),
        event_type.alias("event_type"),
        product_id.alias("product_id"),
        category_id.alias("category_id"),
        category_code.alias("category_code"),
        brand.alias("brand"),
        price.alias("price"),
        user_id.alias("user_id"),
        user_session.alias("user_session"),
    )

    reject_reason = (
        F.when(F.col("event_time").isNull(), F.lit("invalid_event_time"))
        .when(
            F.col("event_type").isNull() | ~F.col("event_type").isin(*VALID_EVENT_TYPES),
            F.lit("invalid_event_type"),
        )
        .when(
            F.col("product_id").isNull()
            | F.col("category_id").isNull()
            | F.col("user_id").isNull()
            | F.col("user_session").isNull(),
            F.lit("missing_required"),
        )
        .when(F.col("price").isNull(), F.lit("missing_price"))
        .when(F.col("price") < 0, F.lit("negative_price"))
    )
    labeled = typed.withColumn("reject_reason", reject_reason).cache()

    input_rows = labeled.count()
    rejected_rows_df = (
        labeled.filter(F.col("reject_reason").isNotNull())
        .groupBy("reject_reason")
        .count()
        .collect()
    )
    rejected_rows_by_reason = {row["reject_reason"]: int(row["count"]) for row in rejected_rows_df}
    rejected_rows = sum(rejected_rows_by_reason.values())

    accepted = labeled.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    accepted_rows = accepted.count()
    deduped = accepted.dropDuplicates(DEDUP_COLUMNS)
    output_rows = deduped.count()

    with_id = deduped.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.date_format(F.col("event_time"), "yyyy-MM-dd HH:mm:ss"),
                F.col("event_type"),
                F.col("product_id").cast("string"),
                F.col("category_id").cast("string"),
                F.coalesce(F.col("category_code"), F.lit("")),
                F.coalesce(F.col("brand"), F.lit("")),
                F.format_string("%.2f", F.col("price")),
                F.col("user_id").cast("string"),
                F.col("user_session"),
            ),
            256,
        ),
    ).withColumn("event_date", F.to_date(F.col("event_time")))

    silver = with_id.select(*OUTPUT_COLUMNS).cache()
    labeled.unpersist()

    null_exprs = [
        F.sum(F.when(F.col(name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(name)
        for name in OUTPUT_COLUMNS
    ]
    null_row = silver.agg(*null_exprs).collect()[0]
    bounds = silver.agg(
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("max_event_time"),
    ).collect()[0]
    by_type = {
        row["event_type"]: int(row["count"])
        for row in silver.groupBy("event_type").count().collect()
    }
    by_date = {
        row["event_date"].isoformat(): int(row["count"])
        for row in silver.groupBy("event_date").count().orderBy("event_date").collect()
    }

    def _iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.strftime("%Y-%m-%dT%H:%M:%S")

    stats = {
        "input_rows": int(input_rows),
        "accepted_rows": int(accepted_rows),
        "output_rows": int(output_rows),
        "duplicate_rows_removed": int(accepted_rows - output_rows),
        "rejected_rows": int(rejected_rows),
        "rejected_rows_by_reason": rejected_rows_by_reason,
        "null_rates": {
            name: (float(null_row[name]) / output_rows if output_rows else None)
            for name in OUTPUT_COLUMNS
        },
        "rows_by_event_type": by_type,
        "rows_by_event_date": by_date,
        "min_event_time": _iso(bounds["min_event_time"]),
        "max_event_time": _iso(bounds["max_event_time"]),
        "valid_event_types": list(VALID_EVENT_TYPES),
        "finished_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return silver, stats


def write_silver(silver: DataFrame, output_path: str) -> None:
    silver.write.mode("overwrite").partitionBy("event_date").parquet(output_path)


def write_run_log(stats: dict, run_log_path: str) -> None:
    path = Path(run_log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(stats, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    run_log_path: str,
) -> dict:
    raw = read_raw(spark, input_path)
    silver, stats = transform(raw)
    try:
        write_silver(silver, output_path)
    finally:
        silver.unpersist()
    stats["input_path"] = str(input_path)
    stats["output_path"] = str(output_path)
    stats["spark_version"] = spark.version
    write_run_log(stats, run_log_path)
    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert a REES46 CSV month to silver Parquet.")
    parser.add_argument("--input", required=True, help="Path to one CSV, for example 2019-Oct.csv")
    parser.add_argument("--output", required=True, help="Parquet root, partitioned by event_date")
    parser.add_argument("--run-log", required=True, help="JSON file for row counts and null rates")
    parser.add_argument("--driver-memory", default="8g")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = build_spark("rees46-csv-to-parquet", driver_memory=args.driver_memory)
    try:
        stats = run(spark, args.input, args.output, args.run_log)
    finally:
        spark.stop()
    print(
        json.dumps(
            {
                "output_rows": stats["output_rows"],
                "duplicate_rows_removed": stats["duplicate_rows_removed"],
                "rejected_rows": stats["rejected_rows"],
                "run_log": args.run_log,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
