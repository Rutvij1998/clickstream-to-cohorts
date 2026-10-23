"""Replay window for the Oct and Nov 2019 REES46 files.

Airflow treats end_date as exclusive: a daily run is kept when its data interval
end is on or before this instant. 2019-12-01 keeps the logical date 2019-11-30
and drops 2019-12-01, which is 61 days.
"""

from datetime import date, datetime, timedelta

REPLAY_START = date(2019, 10, 1)
REPLAY_END = date(2019, 11, 30)
REPLAY_END_EXCLUSIVE = datetime(2019, 12, 1)


def replay_dates() -> list[date]:
    days: list[date] = []
    current = REPLAY_START
    while current <= REPLAY_END:
        days.append(current)
        current += timedelta(days=1)
    return days
