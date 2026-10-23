"""Write mart tables from the local DuckDB warehouse to CSV for Tableau Public."""

from __future__ import annotations

import os
from pathlib import Path

from pipeline.marts import MART_SCHEMA, MART_TABLES

ROOT = Path(os.environ.get("CLICKSTREAM_HOME", Path(__file__).resolve().parents[1]))


def main() -> None:
    import duckdb

    database = ROOT / "dbt" / "target" / "clickstream.duckdb"
    if not database.is_file():
        raise SystemExit(f"Missing {database}. Run make run or make dbt-dev first.")
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


if __name__ == "__main__":
    main()
