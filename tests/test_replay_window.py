from datetime import date

from pipeline.window import REPLAY_END, REPLAY_START, replay_dates


def test_replay_covers_october_and_november_2019():
    days = replay_dates()
    assert days[0] == date(2019, 10, 1)
    assert days[-1] == date(2019, 11, 30)
    assert REPLAY_START == days[0]
    assert REPLAY_END == days[-1]
    assert len(days) == 61
    assert len(days) == len(set(days))
