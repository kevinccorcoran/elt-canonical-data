"""Full statements (edgar.fsn_*): splits, custom tags and layout, as filed.

Apple's FY2024 10-K (accession 0000320193-24-000123) income statement, as
printed, including the Products / Services split and the product-line
revenue from the segment note. Skips cleanly when that filing is not loaded.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

APPLE_FY24_10K = "0000320193-24-000123"

# (plabel, segments) -> value for fiscal 2024, from the 10-K.
APPLE_FY24_IS = {
    ("Net sales", ""): 391_035_000_000,
    ("Net sales", "ProductOrService=Product;"): 294_866_000_000,
    ("Net sales", "ProductOrService=Service;"): 96_169_000_000,
    ("Net sales", "ProductOrService=IPhone;"): 201_183_000_000,
    ("Net sales", "ProductOrService=Mac;"): 29_984_000_000,
    ("Net sales", "ProductOrService=IPad;"): 26_694_000_000,
    ("Net sales", "ProductOrService=WearablesHomeandAccessories;"): 37_005_000_000,
    ("Cost of sales", ""): 210_352_000_000,
    ("Cost of sales", "ProductOrService=Product;"): 185_233_000_000,
    ("Cost of sales", "ProductOrService=Service;"): 25_119_000_000,
    ("Operating income", ""): 123_216_000_000,
    ("Net income", ""): 93_736_000_000,
}


@pytest.fixture
def fsn(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('edgar.fsn_numbers')")
        if cur.fetchone()[0] is None:
            pytest.skip("edgar.fsn_* not in this database")
        cur.execute("SELECT 1 FROM edgar.fsn_submissions WHERE accession_number = %s", (APPLE_FY24_10K,))
        if cur.fetchone() is None:
            pytest.skip("Apple FY2024 10-K not loaded")
    return db_conn


def test_apple_income_statement_with_splits(fsn):
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT plabel, segments, value
            FROM edgar.fsn_statement_lines
            WHERE accession_number = %s AND stmt = 'IS'
              AND ddate = '2024-09-30' AND qtrs = 4 AND iprx = 0
            """,
            (APPLE_FY24_10K,),
        )
        got = {(p, s): v for p, s, v in cur.fetchall()}
    for key, expected in APPLE_FY24_IS.items():
        assert got.get(key) == Decimal(expected), (key, got.get(key))


def test_apple_statement_layout_in_order(fsn):
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT p.plabel
            FROM edgar.fsn_presentation p
            JOIN edgar.fsn_submissions s USING (sub_id)
            WHERE s.accession_number = %s AND p.stmt = 'IS'
            ORDER BY p.report, p.line
            """,
            (APPLE_FY24_10K,),
        )
        labels = [r[0] for r in cur.fetchall()]
    order = ["Net sales", "Cost of sales", "Gross margin", "Operating income", "Net income"]
    positions = [labels.index(lbl) for lbl in order]
    assert positions == sorted(positions), labels


def test_accepted_at_is_new_york_time(fsn):
    """Apple filed its FY2024 10-K the morning of 2024-11-01 New York time."""
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT filed, (accepted_at AT TIME ZONE 'America/New_York')::date
            FROM edgar.fsn_submissions WHERE accession_number = %s
            """,
            (APPLE_FY24_10K,),
        )
        filed, accepted_ny = cur.fetchone()
    assert str(filed) == "2024-11-01" and str(accepted_ny) == "2024-11-01"


def test_accepted_rarely_after_filed(fsn):
    """EDGAR dates a filing on or after its acceptance day (after 17:30 New
    York it is the next business day). SEC's own filing-date adjustments
    break this for a handful (2024q4: 4 of 20,917), which is why the safe
    as-of date is the later of filed and the acceptance day. A timezone bug
    in accepted_at would push this share far higher."""
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE (accepted_at AT TIME ZONE 'America/New_York')::date > filed),
                   count(*)
            FROM edgar.fsn_submissions
            """
        )
        late, total = cur.fetchone()
    assert late <= 0.001 * total, (late, total)


def test_no_duplicate_numbers(fsn):
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM (
                SELECT 1 FROM edgar.fsn_numbers
                GROUP BY sub_id, tag_id, ddate, qtrs, uom, dim_id, iprx
                HAVING count(*) > 1
            ) d
            """
        )
        assert cur.fetchone()[0] == 0


def test_every_number_has_a_statement_line(fsn):
    """Scope rule: only tags that are a line on BS/IS/CF/EQ/CI are kept."""
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM (
                SELECT DISTINCT n.sub_id, n.tag_id FROM edgar.fsn_numbers n
                WHERE n.sub_id IN (SELECT sub_id FROM edgar.fsn_submissions
                                   WHERE accession_number = %s)
            ) n
            LEFT JOIN edgar.fsn_presentation p USING (sub_id, tag_id)
            WHERE p.sub_id IS NULL
            """,
            (APPLE_FY24_10K,),
        )
        assert cur.fetchone()[0] == 0


def test_custom_tags_and_splits_present(fsn):
    with fsn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE t.custom),
                   count(*) FILTER (WHERE n.dim_id <> 0)
            FROM edgar.fsn_numbers n JOIN edgar.fsn_tags t USING (tag_id)
            """
        )
        custom, split = cur.fetchone()
    assert custom > 0 and split > 0
