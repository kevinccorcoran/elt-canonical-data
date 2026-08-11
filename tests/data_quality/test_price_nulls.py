"""Data-quality: the cleaned price tables carry no non-positive / NULL OHLC.

Skips cleanly when a table isn't present in the target database (the board-only
dev copy doesn't hold these; they live in prod).
"""
from __future__ import annotations

import pytest

CLEAN_TABLES = ["cdm.historical_daily_main_clean", "cdm.crypto_main_clean"]


@pytest.mark.parametrize("table", CLEAN_TABLES)
def test_no_invalid_ohlc(db_conn, table):
    with db_conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        if cur.fetchone()[0] is None:
            pytest.skip(f"{table} not in this database")
        cur.execute(
            f"SELECT count(*) FROM {table} "
            "WHERE (open <= 0 OR open IS NULL) "
            "AND (high <= 0 OR high IS NULL) "
            "AND (low <= 0 OR low IS NULL)")
        bad = cur.fetchone()[0]
    assert bad == 0, f"{table}: {bad} rows with invalid OHLC"
