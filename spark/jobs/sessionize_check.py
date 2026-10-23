"""Compare a 30 minute inactivity session with the logged `user_session`.

Marts keep `user_session` as the session key. This job only writes a report.
A gap of exactly 30 minutes stays in the same derived session. A longer gap
starts a new one.

Disagreement, among consecutive events for the same user:
- the pair shares a user_session and the gap is over 30 minutes
- the gap is 30 minutes or less and the user_session changes

`session_disagreement_rate` is those pairs divided by all consecutive pairs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.jobs.session import build_spark

GAP_SECONDS = 30 * 60


def sessionize(events: DataFrame) -> tuple[dict, DataFrame]:
    ordered = Window.partitionBy("user_id").orderBy("event_time", "event_id")
    previous_time = F.lag("event_time").over(ordered)
    previous_session = F.lag("user_session").over(ordered)
    gap_seconds = F.unix_timestamp("event_time") - F.unix_timestamp(previous_time)

    paired = (
        events.withColumn("previous_event_time", previous_time)
        .withColumn("previous_user_session", previous_session)
        .withColumn("gap_seconds", gap_seconds)
    )
    is_pair = F.col("previous_event_time").isNotNull()
    long_gap = (
        is_pair
        & (F.col("gap_seconds") > GAP_SECONDS)
        & (F.col("user_session") == F.col("previous_user_session"))
    )
    short_split = (
        is_pair
        & (F.col("gap_seconds") <= GAP_SECONDS)
        & (F.col("user_session") != F.col("previous_user_session"))
    )
    labeled = paired.withColumn(
        "disagreement_type",
        F.when(long_gap, F.lit("gap_over_30m_inside_user_session")).when(
            short_split, F.lit("within_30m_across_user_sessions")
        ),
    )

    new_session = F.when(
        F.col("previous_event_time").isNull() | (F.col("gap_seconds") > GAP_SECONDS),
        F.lit(1),
    ).otherwise(F.lit(0))
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    derived = labeled.withColumn(
        "derived_session_id",
        F.concat_ws(
            "-",
            F.col("user_id").cast("string"),
            F.sum(new_session).over(running).cast("string"),
        ),
    )

    counts = derived.agg(
        F.count(F.lit(1)).alias("event_rows"),
        F.countDistinct("user_session").alias("user_sessions"),
        F.countDistinct("derived_session_id").alias("derived_sessions"),
        F.sum(F.when(is_pair, F.lit(1)).otherwise(F.lit(0))).alias("consecutive_pairs"),
        F.sum(
            F.when(
                F.col("disagreement_type") == "gap_over_30m_inside_user_session",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("pairs_gap_over_30m_inside_user_session"),
        F.sum(
            F.when(
                F.col("disagreement_type") == "within_30m_across_user_sessions",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("pairs_within_30m_across_user_sessions"),
    ).collect()[0]

    consecutive = int(counts["consecutive_pairs"])
    long_pairs = int(counts["pairs_gap_over_30m_inside_user_session"])
    short_pairs = int(counts["pairs_within_30m_across_user_sessions"])
    disagreeing = long_pairs + short_pairs
    rate = (disagreeing / consecutive) if consecutive else None

    report = {
        "rule": (
            "A new derived session starts when the gap since the previous event "
            "for the same user is greater than 30 minutes."
        ),
        "session_key_in_marts": "user_session",
        "event_rows": int(counts["event_rows"]),
        "user_sessions": int(counts["user_sessions"]),
        "derived_sessions": int(counts["derived_sessions"]),
        "consecutive_pairs": consecutive,
        "pairs_gap_over_30m_inside_user_session": long_pairs,
        "pairs_within_30m_across_user_sessions": short_pairs,
        "session_disagreement_rate": rate,
        "session_disagreement_rate_definition": (
            "Disagreeing consecutive pairs divided by consecutive pairs for the same user. "
            "A pair disagrees when it shares a user_session and the gap is over 30 minutes, "
            "or the gap is 30 minutes or less and the user_session changes."
        ),
    }
    pairs = derived.filter(F.col("disagreement_type").isNotNull()).select(
        "user_id",
        "event_time",
        "previous_event_time",
        "gap_seconds",
        "user_session",
        "previous_user_session",
        "disagreement_type",
        "derived_session_id",
    )
    return report, pairs


def run(spark: SparkSession, input_path: str, report_path: str, pairs_path: str) -> dict:
    events = spark.read.parquet(input_path)
    report, pairs = sessionize(events)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report["input_path"] = str(input_path)
    report_file.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pairs.write.mode("overwrite").option("header", True).csv(pairs_path)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report 30 minute session disagreement.")
    parser.add_argument("--input", required=True, help="Silver Parquet root")
    parser.add_argument("--report", required=True, help="JSON report path")
    parser.add_argument("--pairs", required=True, help="CSV directory of disagreeing pairs")
    parser.add_argument("--driver-memory", default="8g")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = build_spark("rees46-sessionize-check", driver_memory=args.driver_memory)
    try:
        report = run(spark, args.input, args.report, args.pairs)
    finally:
        spark.stop()
    print(
        json.dumps(
            {
                "session_disagreement_rate": report["session_disagreement_rate"],
                "report": args.report,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
