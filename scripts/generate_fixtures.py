"""Write the synthetic raw CSV, the silver seed, and the date spine.

Rows are invented so unit tests and the DuckDB dbt target can run without
a Kaggle download. They are not a sample of the REES46 files.
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from spark.jobs.identifiers import event_id_payload, event_id_sha256

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "spark" / "tests" / "fixtures" / "events_raw.csv"
SEED_PATH = ROOT / "dbt" / "seeds" / "events_silver.csv"
SPINE_PATH = ROOT / "dbt" / "seeds" / "dim_date_spine.csv"

# event_time, event_type, product_id, category_id, category_code,
# brand, price, user_id, user_session
EVENTS: list[tuple[str, str, str, str, str, str, str, str, str]] = [
    (
        "2019-10-01 10:00:00 UTC",
        "view",
        "100",
        "2101",
        "electronics.smartphone",
        "samsung",
        "499.00",
        "1001",
        "sess-1001-a",
    ),
    (
        "2019-10-01 10:05:00 UTC",
        "cart",
        "100",
        "2101",
        "electronics.smartphone",
        "samsung",
        "499.00",
        "1001",
        "sess-1001-a",
    ),
    (
        "2019-10-01 10:10:00 UTC",
        "purchase",
        "100",
        "2101",
        "electronics.smartphone",
        "samsung",
        "499.00",
        "1001",
        "sess-1001-a",
    ),
    (
        "2019-10-09 11:00:00 UTC",
        "view",
        "200",
        "2102",
        "appliances.kitchen.washer",
        "bosch",
        "350.00",
        "1001",
        "sess-1001-b",
    ),
    (
        "2019-10-09 11:20:00 UTC",
        "purchase",
        "200",
        "2102",
        "appliances.kitchen.washer",
        "bosch",
        "350.00",
        "1001",
        "sess-1001-b",
    ),
    (
        "2019-10-15 09:00:00 UTC",
        "view",
        "300",
        "2103",
        "apparel.shoes",
        "nike",
        "80.00",
        "1001",
        "sess-1001-c",
    ),
    (
        "2019-10-01 12:00:00 UTC",
        "view",
        "101",
        "2101",
        "electronics.smartphone",
        "apple",
        "999.00",
        "1002",
        "sess-1002-d",
    ),
    (
        "2019-10-01 12:02:00 UTC",
        "view",
        "102",
        "2104",
        "electronics.audio",
        "sony",
        "150.00",
        "1002",
        "sess-1002-d",
    ),
    (
        "2019-10-08 15:00:00 UTC",
        "view",
        "301",
        "2103",
        "apparel.shoes",
        "adidas",
        "60.00",
        "1002",
        "sess-1002-e",
    ),
    (
        "2019-10-08 08:00:00 UTC",
        "view",
        "201",
        "2105",
        "appliances.kitchen.mixer",
        "kitchenaid",
        "200.00",
        "1003",
        "sess-1003-f",
    ),
    (
        "2019-10-08 08:30:00 UTC",
        "cart",
        "201",
        "2105",
        "appliances.kitchen.mixer",
        "kitchenaid",
        "200.00",
        "1003",
        "sess-1003-f",
    ),
    (
        "2019-10-10 08:00:00 UTC",
        "purchase",
        "201",
        "2105",
        "appliances.kitchen.mixer",
        "kitchenaid",
        "200.00",
        "1003",
        "sess-1003-g",
    ),
    (
        "2019-10-08 18:00:00 UTC",
        "view",
        "900",
        "9999",
        "",
        "",
        "40.00",
        "1004",
        "sess-1004-h",
    ),
    (
        "2019-10-08 18:05:00 UTC",
        "cart",
        "900",
        "9999",
        "",
        "",
        "40.00",
        "1004",
        "sess-1004-h",
    ),
    (
        "2019-10-03 14:00:00 UTC",
        "view",
        "103",
        "2101",
        "electronics.smartphone",
        "xiaomi",
        "150.00",
        "1005",
        "sess-1005-i",
    ),
    (
        "2019-10-03 14:10:00 UTC",
        "cart",
        "103",
        "2101",
        "electronics.smartphone",
        "xiaomi",
        "150.00",
        "1005",
        "sess-1005-i",
    ),
    (
        "2019-10-03 14:20:00 UTC",
        "remove_from_cart",
        "103",
        "2101",
        "electronics.smartphone",
        "xiaomi",
        "150.00",
        "1005",
        "sess-1005-i",
    ),
    (
        "2019-10-03 16:00:00 UTC",
        "view",
        "100",
        "2101",
        "electronics.smartphone",
        "samsung",
        "499.00",
        "1006",
        "sess-1006-j",
    ),
    (
        "2019-10-03 16:05:00 UTC",
        "purchase",
        "100",
        "2101",
        "electronics.smartphone",
        "samsung",
        "499.00",
        "1006",
        "sess-1006-j",
    ),
    (
        "2019-11-29 20:00:00 UTC",
        "view",
        "101",
        "2101",
        "electronics.smartphone",
        "apple",
        "999.00",
        "1007",
        "sess-1007-k",
    ),
    (
        "2019-11-29 20:10:00 UTC",
        "cart",
        "101",
        "2101",
        "electronics.smartphone",
        "apple",
        "999.00",
        "1007",
        "sess-1007-k",
    ),
    (
        "2019-11-29 20:15:00 UTC",
        "purchase",
        "101",
        "2101",
        "electronics.smartphone",
        "apple",
        "999.00",
        "1007",
        "sess-1007-k",
    ),
    (
        "2019-10-02 09:00:00 UTC",
        "view",
        "301",
        "2103",
        "apparel.shoes",
        "nike",
        "80.00",
        "1008",
        "sess-1008-long",
    ),
    (
        "2019-10-02 10:40:00 UTC",
        "view",
        "301",
        "2103",
        "apparel.shoes",
        "nike",
        "80.00",
        "1008",
        "sess-1008-long",
    ),
    (
        "2019-10-02 13:00:00 UTC",
        "view",
        "102",
        "2104",
        "electronics.audio",
        "sony",
        "25.00",
        "1009",
        "sess-1009-m",
    ),
    (
        "2019-10-02 13:10:00 UTC",
        "cart",
        "102",
        "2104",
        "electronics.audio",
        "sony",
        "25.00",
        "1009",
        "sess-1009-n",
    ),
    (
        "2019-10-04 09:00:00 UTC",
        "view",
        "400",
        "2106",
        "apparel.accessories",
        "generic",
        "5.00",
        "1010",
        "sess-1010-o",
    ),
    (
        "2019-10-04 09:05:00 UTC",
        "cart",
        "400",
        "2106",
        "apparel.accessories",
        "generic",
        "5.00",
        "1010",
        "sess-1010-o",
    ),
    (
        "2019-10-05 12:00:00 UTC",
        "view",
        "500",
        "2103",
        "apparel.shoes",
        "generic",
        "0.00",
        "1011",
        "sess-1011-p",
    ),
]

HEADER = [
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


def _write_raw(rows: list[tuple[str, ...]]) -> None:
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RAW_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)


def _write_seed(rows: list[tuple[str, ...]]) -> None:
    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SEED_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "event_id",
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
                "event_date",
            ]
        )
        for row in rows:
            stamp = row[0].removesuffix(" UTC")
            payload = event_id_payload(
                event_time=stamp,
                event_type=row[1],
                product_id=row[2],
                category_id=row[3],
                category_code=row[4] or None,
                brand=row[5] or None,
                price=float(row[6]),
                user_id=row[7],
                user_session=row[8],
            )
            writer.writerow(
                [
                    event_id_sha256(payload),
                    stamp,
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    stamp[:10],
                ]
            )


def _write_spine() -> None:
    start = date(2019, 10, 1)
    end = date(2019, 11, 30)
    with SPINE_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "date_day",
                "year_number",
                "month_number",
                "day_of_month",
                "iso_day_of_week",
                "day_name",
                "week_start_date",
            ]
        )
        current = start
        while current <= end:
            week_start = current - timedelta(days=current.weekday())
            writer.writerow(
                [
                    current.isoformat(),
                    current.year,
                    current.month,
                    current.day,
                    current.isoweekday(),
                    current.strftime("%A"),
                    week_start.isoformat(),
                ]
            )
            current += timedelta(days=1)


def main() -> None:
    raw_rows = [EVENTS[0], *EVENTS]
    _write_raw(raw_rows)
    _write_seed(EVENTS)
    _write_spine()
    print(f"raw_rows={len(raw_rows)} seed_rows={len(EVENTS)} spine={SPINE_PATH}")


if __name__ == "__main__":
    main()
