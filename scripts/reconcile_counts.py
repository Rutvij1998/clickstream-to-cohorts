"""Compare Spark run logs with a local Parquet count.

Prints both numbers. Exits 1 when they differ. Does not embed an expected count.
Month logs (data/logs/2019-*.json) win when they exist. Otherwise sample.json
is the log from a fixture run.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def log_paths() -> list[Path]:
    logs = sorted((ROOT / "data" / "logs").glob("2019-*.json"))
    if logs:
        return logs
    sample = ROOT / "data" / "logs" / "sample.json"
    if sample.is_file():
        return [sample]
    raise SystemExit("No data/logs/2019-*.json or sample.json. Run make run first.")


def spark_output_rows() -> int:
    total = 0
    for path in log_paths():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "output_rows" not in payload:
            continue
        total += int(payload["output_rows"])
        print(f"{path.name} output_rows={payload['output_rows']}")
    return total


def parquet_count() -> int:
    import duckdb

    silver = ROOT / "data" / "silver" / "events"
    if not silver.is_dir():
        raise SystemExit(f"No silver Parquet under {silver}.")
    connection = duckdb.connect()
    try:
        row = connection.execute(
            """
            select count(*)
            from read_parquet(?, hive_partitioning = true)
            """,
            [str(silver / "**" / "*.parquet")],
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise SystemExit("Parquet count returned no row.")
    return int(row[0])


def main() -> None:
    logged = spark_output_rows()
    counted = parquet_count()
    print(f"spark_output_rows={logged}")
    print(f"parquet_count={counted}")
    if logged != counted:
        raise SystemExit("Spark output_rows and the Parquet count differ.")


if __name__ == "__main__":
    main()
