"""The gap finder: which days the market was shut, and which open days the raw price table is missing."""
import datetime as dt

from datapipeline.quality import price_gaps as g

D = dt.date


def test_holidays_match_the_exchange():
    assert g.holidays(2025) == {D(2025, 1, 1), D(2025, 1, 20), D(2025, 2, 17), D(2025, 4, 18), D(2025, 5, 26),
                                D(2025, 6, 19), D(2025, 7, 4), D(2025, 9, 1), D(2025, 11, 27), D(2025, 12, 25)}
    assert D(2026, 7, 3) in g.holidays(2026)            # July 4th on a Saturday closes the Friday
    assert D(2026, 4, 3) in g.holidays(2026)            # Good Friday
    assert D(2021, 12, 31) not in g.holidays(2021)      # New Year's on a Saturday is not moved
    assert not g.is_open(D(2025, 1, 9))                 # a one-off closure


def _normal_weeks(start, end, n=1700):
    return {d: n for d in g.open_days(start, end)}


def test_a_dropped_day_and_a_thin_day_are_missing():
    counts = _normal_weeks(D(2026, 2, 2), D(2026, 4, 30))
    del counts[D(2026, 3, 11)]
    counts[D(2026, 3, 12)] = 300
    assert g.missing_days(counts, D(2026, 2, 2), D(2026, 4, 30)) == [D(2026, 3, 11), D(2026, 3, 12)]


def test_a_lasting_drop_in_names_is_not_missing():
    counts = _normal_weeks(D(2025, 10, 1), D(2025, 11, 19), 5600)
    counts.update(_normal_weeks(D(2025, 11, 20), D(2025, 12, 31), 1700))
    assert g.missing_days(counts, D(2025, 10, 1), D(2025, 12, 31)) == []


def test_shut_days_are_never_missing_and_join_spans():
    counts = _normal_weeks(D(2026, 3, 2), D(2026, 4, 30))
    assert D(2026, 4, 3) not in g.missing_days(counts, D(2026, 3, 2), D(2026, 4, 30))
    assert g.spans([D(2026, 4, 2), D(2026, 4, 6), D(2026, 4, 8)]) == [(D(2026, 4, 2), D(2026, 4, 6)),
                                                                     (D(2026, 4, 8), D(2026, 4, 8))]
