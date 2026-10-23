import ast
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def test_raw_fixture_shape():
    rows = _lines(ROOT / "spark/tests/fixtures/events_raw.csv")
    assert rows[0].split(",")[:9] == [
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
    data = rows[1:]
    assert len(data) == 30
    assert data[0] == data[1]


def test_seed_and_date_spine_shape():
    events = _lines(ROOT / "dbt/seeds/events_silver.csv")
    assert events[0].split(",")[0] == "event_id"
    assert len(events) - 1 == 29
    spine = _lines(ROOT / "dbt/seeds/dim_date_spine.csv")
    assert len(spine) - 1 == 61
    assert spine[1].startswith("2019-10-01,")
    assert spine[-1].startswith("2019-11-30,")


def test_build_daily_replays_exclusive_end():
    path = ROOT / "airflow/dags/build_daily.py"
    text = path.read_text(encoding="utf-8")
    ast.parse(text)
    assert "build_daily" in text
    assert "catchup=True" in text
    assert "2019-12-01" in text
    assert "check_partition" in text
    assert "data_quality_gate" in text
    assert "export_marts" in text
    assert "silver_source" in text


def test_ingest_month_is_manual():
    path = ROOT / "airflow/dags/ingest_month.py"
    text = path.read_text(encoding="utf-8")
    ast.parse(text)
    assert 'dag_id="ingest_month"' in text
    assert "schedule=None" in text
    assert "2019-10" in text
    assert "2019-11" in text


def test_pipeline_stays_on_the_laptop():
    assert not (ROOT / "infra").exists()
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "\nrun:" in makefile
    tokens = (
        "A" + "WS",
        "a" + "ws",
        "Athe" + "na",
        "athe" + "na",
        "Gl" + "ue",
        "gl" + "ue",
        "Terra" + "form",
        "bo" + "to3",
        "s3://",
    )
    skip = {"dbt/target", "dbt/dbt_packages", "dbt/logs", ".git"}
    suffixes = {".md", ".py", ".sql", ".yml", ".yaml", ".json", ".sh", ".mmd", ".example", ".txt"}
    offenders: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in suffixes:
            continue
        relative = path.relative_to(ROOT).as_posix()
        if relative == "tests/test_repo_contracts.py":
            continue
        if any(part in relative for part in skip):
            continue
        text = path.read_text(encoding="utf-8")
        for token in tokens:
            if token in text:
                offenders.append(f"{relative}: {token}")
    assert offenders == []


def test_download_script_parses():
    script = ROOT / "ingestion/download.sh"
    result = subprocess.run(
        ["bash", "-n", str(script)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
