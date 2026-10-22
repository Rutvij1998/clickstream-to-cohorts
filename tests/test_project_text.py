from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCAN_SUFFIXES = {".md", ".py", ".sql", ".yml", ".yaml", ".tf", ".json", ".sh", ".mmd", ".example"}
SKIP_PARTS = {"dbt/target", "dbt/dbt_packages", "dbt/logs", ".git", "airflow/.astro"}
BANNED = (
    "\u2014",
    "\u2013",
    "Market" + "Vantage",
    "Market" + "-intel",
    "H-" + "1B",
    "Like" + "wize",
    "Snow" + "flake",
    "Red" + "shift",
)


def _tracked_text_files():
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in SCAN_SUFFIXES:
            continue
        relative = path.relative_to(ROOT).as_posix()
        if any(part in relative for part in SKIP_PARTS):
            continue
        yield path, relative


def test_prose_and_code_avoid_banned_tokens():
    offenders: list[str] = []
    for path, relative in _tracked_text_files():
        text = path.read_text(encoding="utf-8")
        for token in BANNED:
            if token in text:
                offenders.append(f"{relative}: {token}")
    assert offenders == []


def test_readme_marks_unpublished_results():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "[N]" in readme
    assert "Tableau Public" in readme
