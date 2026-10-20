"""EDGAR raw layer (edgar schema): as reported, insert-only, point-in-time safe.

Apple's numbers are the ones printed in each year's 10-K (Consolidated
Statements of Operations). Revenue moves across three tags over the years,
which is why raw keeps every tag. Skips cleanly when the edgar schema or
Apple is not loaded in the target database.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

APPLE = "0000320193"
REVENUE_TAGS = ("SalesRevenueNet", "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax")

# fiscal year -> (fiscal year end, revenue USD, diluted EPS), from that year's 10-K.
APPLE_10K = {
    2015: ("2015-09-26", 233_715_000_000, "9.22"),
    2016: ("2016-09-24", 215_639_000_000, "8.31"),
    2017: ("2017-09-30", 229_234_000_000, "9.21"),
    2018: ("2018-09-29", 265_595_000_000, "11.91"),
    2019: ("2019-09-28", 260_174_000_000, "11.89"),
    2020: ("2020-09-26", 274_515_000_000, "3.28"),
    2021: ("2021-09-25", 365_817_000_000, "5.61"),
    2022: ("2022-09-24", 394_328_000_000, "6.11"),
    2023: ("2023-09-30", 383_285_000_000, "6.13"),
    2024: ("2024-09-28", 391_035_000_000, "6.08"),
}


@pytest.fixture
def edgar(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('edgar.facts')")
        if cur.fetchone()[0] is None:
            pytest.skip("edgar schema not in this database")
        cur.execute("SELECT 1 FROM edgar.facts WHERE cik = %s LIMIT 1", (APPLE,))
        if cur.fetchone() is None:
            pytest.skip("Apple not loaded in edgar.facts")
    return db_conn


def _annual_from_own_10k(conn, tags, fy, period_end):
    """Values for a full fiscal year, taken from that fiscal year's own 10-K.
    fiscal_year is the FILING's year, so fiscal_year = fy picks the original
    report, not a later 10-K's comparative."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT tag, value
            FROM edgar.facts
            WHERE cik = %s AND tag = ANY(%s) AND form = '10-K'
              AND fiscal_year = %s AND fiscal_period = 'FY'
              AND period_end = %s
              AND period_end - period_start BETWEEN 357 AND 371
            """,
            (APPLE, list(tags), fy, period_end),
        )
        return cur.fetchall()


@pytest.mark.parametrize("fy", sorted(APPLE_10K))
def test_apple_revenue_matches_10k(edgar, fy):
    period_end, revenue, _ = APPLE_10K[fy]
    rows = _annual_from_own_10k(edgar, REVENUE_TAGS, fy, period_end)
    assert rows, f"no FY{fy} revenue fact"
    assert {v for _, v in rows} == {Decimal(revenue)}, rows


@pytest.mark.parametrize("fy", sorted(APPLE_10K))
def test_apple_diluted_eps_matches_10k(edgar, fy):
    period_end, _, eps = APPLE_10K[fy]
    rows = _annual_from_own_10k(edgar, ["EarningsPerShareDiluted"], fy, period_end)
    assert {v for _, v in rows} == {Decimal(eps)}, rows


def test_restated_versions_are_separate_rows(edgar):
    """FY2019 EPS: 11.89 as first reported, 2.97 after the 2020 4:1 split.
    FY2009 revenue: 36.537B as first reported, 42.905B after the 10-K/A."""
    with edgar.cursor() as cur:
        cur.execute(
            """
            SELECT tag, value, min(filed_date)
            FROM edgar.facts
            WHERE cik = %s
              AND ((tag = 'EarningsPerShareDiluted' AND period_end = '2019-09-28')
                OR (tag = 'SalesRevenueNet' AND period_end = '2009-09-26'))
              AND period_end - period_start > 350
            GROUP BY 1, 2
            ORDER BY 1, 3
            """,
            (APPLE,),
        )
        got = [(t, v, str(d)) for t, v, d in cur.fetchall()]
    assert got == [
        ("EarningsPerShareDiluted", Decimal("11.89"), "2019-10-31"),
        ("EarningsPerShareDiluted", Decimal("2.97"), "2020-10-30"),
        ("SalesRevenueNet", Decimal("36537000000"), "2009-10-27"),
        ("SalesRevenueNet", Decimal("42905000000"), "2010-01-25"),
    ]


@pytest.mark.parametrize("as_of, expected", [
    ("2009-12-31", Decimal("36537000000")),   # before the 10-K/A
    ("2010-02-01", Decimal("42905000000")),   # after it
])
def test_as_of_query_sees_only_what_was_filed(edgar, as_of, expected):
    """The point-in-time rule in one query: newest version filed on or before
    the as-of date."""
    with edgar.cursor() as cur:
        cur.execute(
            """
            SELECT value FROM edgar.facts
            WHERE cik = %s AND tag = 'SalesRevenueNet'
              AND period_end = '2009-09-26' AND period_end - period_start > 350
              AND filed_date <= %s
            ORDER BY filed_date DESC
            LIMIT 1
            """,
            (APPLE, as_of),
        )
        assert cur.fetchone()[0] == expected


@pytest.mark.parametrize("table", ["edgar.facts", "edgar.facts_quarantine"])
def test_no_duplicate_facts(edgar, table):
    """Natural key, recomputed from the columns (not trusting fact_key)."""
    with edgar.cursor() as cur:
        cur.execute(f"""
            SELECT count(*) FROM (
                SELECT 1 FROM {table}
                GROUP BY cik, taxonomy, tag, unit, accession_number,
                         period_end, period_start, value
                HAVING count(*) > 1
            ) d
        """)
        assert cur.fetchone()[0] == 0


def test_no_duplicate_filings(edgar):
    with edgar.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM (
                SELECT 1 FROM edgar.filings GROUP BY cik, accession_number HAVING count(*) > 1
            ) d
        """)
        assert cur.fetchone()[0] == 0


def test_reinsert_adds_nothing(edgar):
    """Idempotency: re-inserting every Apple fact (instants included, whose
    period_start is NULL) hits the unique key. Rolled back by db_conn."""
    cols = ("cik, taxonomy, tag, unit, value, period_start, period_end, fiscal_year, "
            "fiscal_period, form, accession_number, filed_date, frame")
    with edgar.cursor() as cur:
        cur.execute(f"""
            INSERT INTO edgar.facts ({cols})
            SELECT {cols} FROM edgar.facts WHERE cik = %s
            ON CONFLICT DO NOTHING
        """, (APPLE,))
        assert cur.rowcount == 0


def test_no_filed_date_before_period_end(edgar):
    with edgar.cursor() as cur:
        cur.execute("SELECT count(*) FROM edgar.facts WHERE filed_date < period_end")
        assert cur.fetchone()[0] == 0
        # And the table enforces it, so no future load can break it.
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'ck_edgar_facts_period_end_not_after_filed'
        """)
        assert cur.fetchone() is not None


def test_cik_format_joins_ticker_metadata(edgar):
    with edgar.cursor() as cur:
        cur.execute("SELECT count(*) FROM edgar.companies WHERE cik !~ '^[0-9]{10}$'")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM raw.ticker_metadata WHERE cik !~ '^[0-9]{10}$'")
        assert cur.fetchone()[0] == 0, "raw.ticker_metadata changed CIK format"
        # Survivorship: delisted tickers link to companies with facts.
        cur.execute("""
            SELECT count(DISTINCT c.cik)
            FROM edgar.companies c
            JOIN raw.ticker_metadata t ON t.cik = c.cik
            WHERE t.delisted_utc IS NOT NULL
        """)
        assert cur.fetchone()[0] > 0


def test_every_fact_has_its_filing(edgar):
    """Each fact's accession is in edgar.filings for the same CIK, so the
    exact acceptance time is always available downstream."""
    with edgar.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM (
                SELECT DISTINCT cik, accession_number FROM edgar.facts WHERE cik = %s
            ) f
            LEFT JOIN edgar.filings g USING (cik, accession_number)
            WHERE g.accession_number IS NULL
        """, (APPLE,))
        assert cur.fetchone()[0] == 0
