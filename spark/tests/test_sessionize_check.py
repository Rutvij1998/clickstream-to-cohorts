from spark.jobs.csv_to_parquet import transform
from spark.jobs.sessionize_check import sessionize

FIXTURE = "spark/tests/fixtures/events_raw.csv"


def test_fixture_has_both_disagreement_kinds(spark):
    raw = spark.read.csv(FIXTURE, header=True)
    silver, _stats = transform(raw)
    try:
        report, pairs = sessionize(silver)
        assert report["pairs_gap_over_30m_inside_user_session"] == 1
        assert report["pairs_within_30m_across_user_sessions"] == 1
        assert report["consecutive_pairs"] > 0
        rate = report["session_disagreement_rate"]
        assert rate == (2 / report["consecutive_pairs"])
        assert pairs.count() == 2
    finally:
        silver.unpersist()
