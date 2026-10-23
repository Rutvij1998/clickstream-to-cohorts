"""Run the pipeline on this machine, from CSV to Tableau CSV extracts.

Month files in data/raw are used when they are present. Otherwise the
checked-in synthetic fixture is ingested, so the command works before a
Kaggle download.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(args: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd or ROOT, env=env, check=True)


def _ingest_months(months: list[str], memory: str) -> None:
    sample_log = ROOT / "data" / "logs" / "sample.json"
    sample_log.unlink(missing_ok=True)
    env = os.environ.copy()
    env["SPARK_DRIVER_MEMORY"] = memory
    env["PYTHONPATH"] = str(ROOT)
    for month in months:
        _run([sys.executable, "scripts/ingest_month.py", "--month", month], env=env)


def _ingest_sample() -> None:
    print("No month CSV in data/raw. Ingesting the checked-in sample.")
    silver = ROOT / "data" / "silver" / "events"
    if silver.exists():
        shutil.rmtree(silver)
    logs = ROOT / "data" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    for path in logs.glob("2019-*.json"):
        path.unlink()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    _run(
        [
            sys.executable,
            "-m",
            "spark.jobs.csv_to_parquet",
            "--input",
            "spark/tests/fixtures/events_raw.csv",
            "--output",
            "data/silver/events",
            "--run-log",
            "data/logs/sample.json",
            "--driver-memory",
            "1g",
        ],
        env=env,
    )


def main() -> None:
    raw = ROOT / "data" / "raw"
    present = {
        "2019-10": (raw / "2019-Oct.csv").is_file(),
        "2019-11": (raw / "2019-Nov.csv").is_file(),
    }
    months = [month for month, ok in present.items() if ok]
    memory = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    (ROOT / "data" / "logs").mkdir(parents=True, exist_ok=True)
    if months:
        missing = [month for month, ok in present.items() if not ok]
        if missing:
            print(f"Missing month file for {', '.join(missing)}. Ingesting {', '.join(months)}.")
        _ingest_months(months, memory)
        session_memory = memory
    else:
        _ingest_sample()
        session_memory = "1g"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    _run(
        [
            sys.executable,
            "-m",
            "spark.jobs.sessionize_check",
            "--input",
            "data/silver/events",
            "--report",
            "data/logs/session_disagreement.json",
            "--pairs",
            "data/logs/session_disagreements",
            "--driver-memory",
            session_memory,
        ],
        env=env,
    )
    dbt_dir = ROOT / "dbt"
    _run(["dbt", "deps", "--profiles-dir", "."], cwd=dbt_dir)
    _run(
        [
            "dbt",
            "build",
            "--target",
            "dev",
            "--profiles-dir",
            ".",
            "--full-refresh",
            "--vars",
            "{silver_source: parquet}",
        ],
        cwd=dbt_dir,
    )
    _run([sys.executable, "scripts/export_marts.py"], env=env)
    _run([sys.executable, "scripts/reconcile_counts.py"], env=env)
    print("Mart CSV files are in data/exports.")


if __name__ == "__main__":
    main()
