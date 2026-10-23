import json
from pathlib import Path

from pyspark.sql import functions as F

from spark.jobs.csv_to_parquet import run, transform
from spark.jobs.identifiers import event_id_payload, event_id_sha256

FIXTURE = Path(__file__).parent / "fixtures" / "events_raw.csv"


def test_fixture_drops_one_exact_duplicate_and_keeps_types(spark, tmp_path):
    output = tmp_path / "silver"
    log_path = tmp_path / "run_log.json"
    stats = run(spark, str(FIXTURE), str(output), str(log_path))

    assert stats["duplicate_rows_removed"] == 1
    assert stats["rejected_rows"] == 0
    assert stats["output_rows"] == stats["input_rows"] - 1
    assert stats["rows_by_event_type"]["purchase"] >= 1

    silver = spark.read.parquet(str(output))
    assert silver.filter(F.col("price") < 0).count() == 0
    assert silver.filter(F.col("event_id").isNull()).count() == 0
    assert silver.select("event_date").distinct().count() == len(stats["rows_by_event_date"])
    written = json.loads(log_path.read_text(encoding="utf-8"))
    assert written["output_rows"] == stats["output_rows"]


def test_event_id_matches_python_hash(spark):
    raw = spark.read.csv(str(FIXTURE), header=True)
    silver, _stats = transform(raw)
    sample = (
        silver.filter(F.col("user_session") == "sess-1001-a")
        .filter(F.col("event_type") == "view")
        .collect()[0]
    )
    payload = event_id_payload(
        event_time=sample["event_time"].strftime("%Y-%m-%d %H:%M:%S"),
        event_type=sample["event_type"],
        product_id=sample["product_id"],
        category_id=sample["category_id"],
        category_code=sample["category_code"],
        brand=sample["brand"],
        price=sample["price"],
        user_id=sample["user_id"],
        user_session=sample["user_session"],
    )
    assert sample["event_id"] == event_id_sha256(payload)
    silver.unpersist()


def test_rejects_bad_type_and_negative_price(spark):
    rows = [
        (
            "2019-10-01 00:00:00 UTC",
            "refund",
            "1",
            "2",
            "apparel.shoes",
            "nike",
            "10.00",
            "9",
            "sess",
        ),
        (
            "2019-10-01 00:00:00 UTC",
            "view",
            "1",
            "2",
            "apparel.shoes",
            "nike",
            "-1.00",
            "9",
            "sess",
        ),
    ]
    columns = list(spark.read.csv(str(FIXTURE), header=True).columns)
    raw = spark.createDataFrame(rows, schema=columns)
    _silver, stats = transform(raw)
    assert stats["output_rows"] == 0
    assert stats["rejected_rows_by_reason"]["invalid_event_type"] == 1
    assert stats["rejected_rows_by_reason"]["negative_price"] == 1
    _silver.unpersist()
