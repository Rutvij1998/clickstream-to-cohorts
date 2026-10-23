"""Run the Spark CSV job for one REES46 month."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(os.environ.get("CLICKSTREAM_HOME", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT))

from spark.jobs.csv_to_parquet import run  # noqa: E402
from spark.jobs.session import build_spark  # noqa: E402

MONTH_FILES = {
    "2019-10": "2019-Oct.csv",
    "2019-11": "2019-Nov.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest one REES46 month.")
    parser.add_argument("--month", required=True, choices=sorted(MONTH_FILES))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_name = MONTH_FILES[args.month]
    source = ROOT / "data" / "raw" / csv_name
    if not source.is_file():
        raise SystemExit(f"Missing {source}. Run make download first.")
    output = ROOT / "data" / "silver" / "events"
    log_path = ROOT / "data" / "logs" / f"{csv_name.removesuffix('.csv')}.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    memory = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    spark = build_spark(f"rees46-{args.month}", driver_memory=memory)
    try:
        stats = run(spark, str(source), str(output), str(log_path))
    finally:
        spark.stop()
    print(
        f"month={args.month} output_rows={stats['output_rows']} "
        f"duplicate_rows_removed={stats['duplicate_rows_removed']} log={log_path}"
    )


if __name__ == "__main__":
    main()
