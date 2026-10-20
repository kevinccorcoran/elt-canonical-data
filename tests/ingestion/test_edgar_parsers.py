"""EDGAR loader parsers: pure functions, no DB, no network.

Pins the as-reported mapping from SEC JSON to edgar.* rows: CIK padding,
every version of a fact kept, val text untouched, filings scoped to 2009+.
"""
from __future__ import annotations

import json
from datetime import date

from datapipeline.ingestion.edgar_to_raw_etl import (
    FACT_COLUMNS,
    FILING_COLUMNS,
    company_row,
    iter_fact_rows,
    iter_filing_rows,
    overflow_files,
    pad_cik,
    parse_company_tickers,
    parse_xbrl_rss,
)

# Two versions of Apple's FY2019 diluted EPS: the FY2019 10-K (pre-split)
# and the FY2020 10-K comparative (after the 4:1 split). Both must survive.
COMPANYFACTS = json.loads("""
{"cik": 320193, "entityName": "Apple Inc.", "facts": {
  "us-gaap": {"EarningsPerShareDiluted": {"label": "x", "description": "x", "units": {
    "USD/shares": [
      {"start": "2018-09-30", "end": "2019-09-28", "val": 11.89, "accn": "0000320193-19-000119",
       "fy": 2019, "fp": "FY", "form": "10-K", "filed": "2019-10-31"},
      {"start": "2018-09-30", "end": "2019-09-28", "val": 2.97, "accn": "0000320193-20-000096",
       "fy": 2020, "fp": "FY", "form": "10-K", "filed": "2020-10-30", "frame": "CY2019"}
    ]}}},
  "dei": {"EntityCommonStockSharesOutstanding": {"label": "x", "description": "x", "units": {
    "shares": [
      {"end": "2020-10-16", "val": 17001802000, "accn": "0000320193-20-000096",
       "fy": 2020, "fp": "FY", "form": "10-K", "filed": "2020-10-30", "frame": "CY2020Q3I"}
    ]}}}
}}
""", parse_float=str)


def test_pad_cik_forms():
    assert pad_cik(320193) == "0000320193"
    assert pad_cik("320193") == "0000320193"
    assert pad_cik("CIK0000320193") == "0000320193"
    assert pad_cik("0000320193") == "0000320193"


def test_fact_rows_keep_every_version_as_reported():
    rows = [dict(zip(FACT_COLUMNS, r)) for r in iter_fact_rows(COMPANYFACTS, "320193")]
    assert len(rows) == 3
    eps = [r for r in rows if r["tag"] == "EarningsPerShareDiluted"]
    assert {(r["value"], r["accession_number"]) for r in eps} == {
        ("11.89", "0000320193-19-000119"),
        ("2.97", "0000320193-20-000096"),
    }
    # fy is the FILING's fiscal year, not the period's: kept as SEC sends it.
    assert {r["fiscal_year"] for r in eps} == {2019, 2020}
    assert all(r["cik"] == "0000320193" for r in rows)


def test_fact_rows_take_cik_from_caller():
    # 8 bulk files (2026-10) carry facts but no "cik" key.
    doc = {k: v for k, v in COMPANYFACTS.items() if k != "cik"}
    rows = list(iter_fact_rows(doc, "CIK0000320193"))
    assert len(rows) == 3 and {r[0] for r in rows} == {"0000320193"}


def test_instant_fact_has_no_start_and_exact_value():
    (shares,) = [dict(zip(FACT_COLUMNS, r)) for r in iter_fact_rows(COMPANYFACTS, "320193")
                 if r[2] == "EntityCommonStockSharesOutstanding"]
    assert shares["period_start"] is None
    assert shares["value"] == 17001802000
    assert shares["frame"] == "CY2020Q3I"
    assert shares["taxonomy"] == "dei"


def test_float_values_keep_their_json_text():
    doc = json.loads('{"cik": 1, "facts": {"us-gaap": {"X": {"units": {"pure": ['
                     '{"end": "2020-01-01", "val": 1.2E-5, "accn": "a", "form": "10-K", "filed": "2020-02-01"}'
                     ']}}}}}', parse_float=str)
    (row,) = iter_fact_rows(doc, "1")
    assert row[FACT_COLUMNS.index("value")] == "1.2E-5"


SUBMISSIONS_TABLE = {
    "accessionNumber": ["0000320193-20-000096", "0000320193-08-000001"],
    "filingDate": ["2020-10-30", "2008-11-05"],
    "reportDate": ["2020-09-26", ""],
    "acceptanceDateTime": ["2020-10-29T22:06:25.000Z", "2008-11-05T16:00:00.000Z"],
    "act": ["34", ""],
    "form": ["10-K", "10-K"],
    "fileNumber": ["001-36743", ""],
    "filmNumber": ["201273977", ""],
    "items": ["", ""],
    "size": [12345, 1],
    "isXBRL": [1, 0],
    "isInlineXBRL": [1, 0],
    "primaryDocument": ["aapl-20200926.htm", ""],
    "primaryDocDescription": ["10-K", ""],
}


def test_filing_rows_scope_and_blanks():
    rows = [dict(zip(FILING_COLUMNS, r))
            for r in iter_filing_rows("0000320193", SUBMISSIONS_TABLE, date(2009, 1, 1))]
    assert len(rows) == 1  # the 2008 filing is out of scope
    r = rows[0]
    assert r["accession_number"] == "0000320193-20-000096"
    # Kept as SEC's raw string: its "Z" is not a reliable UTC clock.
    assert r["acceptance_datetime_raw"] == "2020-10-29T22:06:25.000Z"
    assert r["items"] is None
    assert r["is_xbrl"] is True and r["is_inline_xbrl"] is True


def test_overflow_files_only_when_in_scope():
    doc = {"filings": {"files": [
        {"name": "CIK0000320193-submissions-001.json", "filingFrom": "1994-01-26", "filingTo": "2015-09-30"},
        {"name": "CIK0000320193-submissions-002.json", "filingFrom": "1990-01-01", "filingTo": "2005-12-31"},
    ]}}
    assert overflow_files(doc, date(2009, 1, 1)) == ["CIK0000320193-submissions-001.json"]


def test_company_row():
    row = company_row({"cik": "0000320193", "name": "Apple Inc.", "entityType": "operating",
                       "sic": "3571", "sicDescription": "Electronic Computers",
                       "category": "Large accelerated filer", "fiscalYearEnd": "0926",
                       "stateOfIncorporation": "CA", "tickers": ["AAPL"], "exchanges": ["Nasdaq"],
                       "formerNames": []})
    assert row[0] == "0000320193" and row[8] == ["AAPL"] and row[10] == "[]"


def test_company_tickers_dedup_and_pad():
    rows = parse_company_tickers({
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
    })
    assert sorted(rows) == [("0000320193", "AAPL", "Apple Inc."),
                            ("0001652044", "GOOGL", "Alphabet Inc.")]


def test_parse_xbrl_rss():
    xml = b"""<?xml version="1.0" encoding="windows-1252"?>
<rss version="2.0"><channel><item>
  <edgar:xbrlFiling xmlns:edgar="https://www.sec.gov/Archives/edgar">
    <edgar:formType>10-Q</edgar:formType>
    <edgar:filingDate>10/19/2026</edgar:filingDate>
    <edgar:cikNumber>0000009631</edgar:cikNumber>
    <edgar:accessionNumber>0001839882-26-051510</edgar:accessionNumber>
  </edgar:xbrlFiling>
</item></channel></rss>"""
    assert list(parse_xbrl_rss(xml)) == [
        ("0000009631", date(2026, 10, 19), "10-Q", "0001839882-26-051510")
    ]
