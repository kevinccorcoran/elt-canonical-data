"""Missing days in the raw price table, and the days the market was shut.

A day is missing when the market was open and raw.api_data_ingestion_massive holds no rows for it, or under half
the rows of the open days around it. Shut days (weekends, NYSE holidays and the one-off closures) hold only the
handful of names that print every calendar day, so they are never counted as missing.
Used by the weekly gap fill (airflow/dags/raw_gap_fill_weekly.py) and by the weekly conformance check.
"""
from __future__ import annotations

import datetime as dt
from statistics import median

TABLE = "raw.api_data_ingestion_massive"
NEIGHBOURS = 10          # open days on each side that set what a normal day holds
THIN = 0.5               # under this share of a normal day counts as missing

# One-off NYSE closures since the raw table starts (2003-09-10).
SPECIAL_CLOSURES = {
    dt.date(2004, 6, 11),    # Reagan
    dt.date(2007, 1, 2),     # Ford
    dt.date(2012, 10, 29),   # Hurricane Sandy
    dt.date(2012, 10, 30),
    dt.date(2018, 12, 5),    # G. H. W. Bush
    dt.date(2025, 1, 9),     # Carter
}


def _easter(year: int) -> dt.date:
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return dt.date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    first = dt.date(year, month, 1)
    return first + dt.timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    last = (dt.date(year, month + 1, 1) if month < 12 else dt.date(year + 1, 1, 1)) - dt.timedelta(days=1)
    return last - dt.timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: dt.date) -> dt.date | None:
    """A Saturday holiday moves to Friday and a Sunday one to Monday. New Year's on a Saturday is not moved."""
    if day.weekday() == 5:
        return None if (day.month, day.day) == (1, 1) else day - dt.timedelta(days=1)
    if day.weekday() == 6:
        return day + dt.timedelta(days=1)
    return day


def holidays(year: int) -> set[dt.date]:
    days = {
        _observed(dt.date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),             # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),             # Presidents' Day
        _easter(year) - dt.timedelta(days=2),    # Good Friday
        _last_weekday(year, 5, 0),               # Memorial Day
        _observed(dt.date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),             # Labor Day
        _nth_weekday(year, 11, 3, 4),            # Thanksgiving
        _observed(dt.date(year, 12, 25)),
    }
    if year >= 2022:
        days.add(_observed(dt.date(year, 6, 19)))   # Juneteenth
    return {d for d in days if d is not None}


def is_open(day: dt.date) -> bool:
    return day.weekday() < 5 and day not in holidays(day.year) and day not in SPECIAL_CLOSURES


def open_days(start: dt.date, end: dt.date) -> list[dt.date]:
    n = (end - start).days + 1
    return [d for d in (start + dt.timedelta(days=i) for i in range(n)) if is_open(d)]


def missing_days(counts: dict[dt.date, int], start: dt.date, end: dt.date) -> list[dt.date]:
    """Open days between start and end with no rows, or under THIN of the open days both before and after them.
    Both sides, so a lasting change in how many names the table holds (2025-11-20, about 5,600 to 1,700) is
    the new normal from its first day, not a missing day. The newest days have no after side and use the before."""
    days = open_days(start, end)

    def normal(side: list[dt.date]) -> float | None:
        held = [counts[d] for d in side if counts.get(d, 0) > 0]
        return median(held) if held else None

    out = []
    for i, day in enumerate(days):
        n = counts.get(day, 0)
        before = normal(days[max(0, i - NEIGHBOURS):i])
        after = normal(days[i + 1:i + 1 + NEIGHBOURS])
        if n == 0 or all(n < THIN * side for side in (before, after) if side is not None) and (before or after):
            out.append(day)
    return out


def spans(days: list[dt.date]) -> list[tuple[dt.date, dt.date]]:
    """Missing days joined into date ranges: two missing days with only shut days between them share a range."""
    out: list[list[dt.date]] = []
    for day in sorted(days):
        if out and not open_days(out[-1][1] + dt.timedelta(days=1), day - dt.timedelta(days=1)):
            out[-1][1] = day
        else:
            out.append([day, day])
    return [(a, b) for a, b in out]


def daily_counts(cur, start: dt.date, end: dt.date) -> dict[dt.date, int]:
    cur.execute(f"SELECT date, count(*) FROM {TABLE} WHERE date BETWEEN %s AND %s GROUP BY date", (start, end))
    return dict(cur.fetchall())


def first_day(cur) -> dt.date:
    cur.execute(f"SELECT min(date) FROM {TABLE}")
    return cur.fetchone()[0]
