"""Fail when a replay day has no local silver partition."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(os.environ.get("CLICKSTREAM_HOME", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT))

from pipeline.window import replay_dates  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check one silver event_date partition.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    day = date.fromisoformat(args.date)
    if day not in set(replay_dates()):
        raise SystemExit(f"{args.date} is outside 2019-10-01 through 2019-11-30")

    partition = ROOT / "data" / "silver" / "events" / f"event_date={args.date}"
    parquet_files = list(partition.glob("*.parquet")) if partition.is_dir() else []
    if not parquet_files:
        raise SystemExit(f"No Parquet files in {partition}")
    print(f"Found {len(parquet_files)} file(s) in {partition}")


if __name__ == "__main__":
    main()
