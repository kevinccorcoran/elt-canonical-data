"""Statements loader parsers: pure functions, no DB, no network."""
from __future__ import annotations

import io

from datapipeline.ingestion.edgar_statements_to_raw_etl import (
    in_universe,
    iter_tsv,
    parse_listing,
    period_key,
)


def test_iter_tsv_handles_quoted_tabs_and_quotes():
    # SEC wraps a field holding a tab or a quote in double quotes (CSV style).
    raw = (
        "dimhash\tsegments\tsegt\n"
        '0xabc\t"InvestmentIdentifier=Unfunded equity\tsecurities;"\t0\n'
        '0xdef\t"Jazan Gas Project Company (""JGPC"")"\t0\n'
        "0x00000000\t\t0\n"
    ).encode()
    rows = list(iter_tsv(io.BytesIO(raw)))
    assert rows[0]["segments"] == "InvestmentIdentifier=Unfunded equity\tsecurities;"
    assert rows[0]["segt"] == "0"
    assert rows[1]["segments"] == 'Jazan Gas Project Company ("JGPC")'
    assert rows[2] == {"dimhash": "0x00000000", "segments": "", "segt": "0"}


def test_parse_listing_oldest_first():
    html = (
        '<a href="/files/dera/data/financial-statement-notes-data-sets/2026_09_notes.zip">'
        '<a href="/files/dera/data/financial-statement-notes-data-sets/2010q1_notes_1.zip">'
        '<a href="/files/dera/data/financial-statement-notes-data-sets/2025q3_notes.zip">'
    )
    names = [n for n, _ in parse_listing(html)]
    assert names == ["2010q1_notes_1", "2025q3_notes", "2026_09_notes"]
    assert parse_listing(html)[0][1].startswith("https://www.sec.gov/files/")


def test_period_key():
    assert period_key("2024q4_notes") == (2024, 12)
    assert period_key("2025_10_notes") == (2025, 10)
    assert period_key("2010q3_notes_0") == (2010, 9)


def test_in_universe_filer_or_coregistrant():
    uni = {"0000320193", "0000000042"}
    assert in_universe({"cik": "320193", "aciks": ""}, uni)
    assert in_universe({"cik": "999", "aciks": "7 42 PARTIAL"}, uni)
    assert not in_universe({"cik": "999", "aciks": "7"}, uni)
