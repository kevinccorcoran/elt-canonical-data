#!/usr/bin/env python3
"""
SEC EDGAR fundamentals -> the edgar schema (raw layer, no cleaning).

Two modes:

    # One-time backfill from SEC's nightly bulk files (companyfacts.zip,
    # submissions.zip, company_tickers.json). No per-company API calls.
    python -m datapipeline.ingestion.edgar_to_raw_etl --mode bulk

    # Daily incremental. Every CIK with an XBRL filing in the window (from
    # SEC's monthly XBRL RSS feed) gets companyfacts + submissions re-pulled.
    python -m datapipeline.ingestion.edgar_to_raw_etl --mode daily

edgar.facts and edgar.filings are insert-only (ON CONFLICT DO NOTHING): a rerun
adds nothing, and a number restated in a later filing lands as a new row next
to the original. DDL: tools/create_edgar_schema.sql. Point-in-time rule:
docs/edgar_fundamentals.md.

SEC fair access: every request carries SEC_USER_AGENT ("Name email@host",
from env or Airflow Variable) and the client stays under SEC_MAX_RPS
(default 8, hard cap 9) requests per second.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import random
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


COMPANYFACTS_ZIP_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
SUBMISSIONS_ZIP_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANYFACTS_API_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SUBMISSIONS_API_URL = "https://data.sec.gov/submissions/{name}"
XBRL_RSS_URL = "https://www.sec.gov/Archives/edgar/monthly/xbrlrss-{year:04d}-{month:02d}.xml"

# Tagged numbers start in 2009 (large filers) and 2011 (everyone). Filings
# before this date are out of scope; facts are not date-filtered at all
# (a 2009 10-K legitimately carries 2007 comparatives).
SCOPE_START = date(2009, 1, 1)

# Daily window: re-pull CIKs that filed in the last N days on top of the
# newest successful window_end, so SEC's own processing lag and a missed
# run are both absorbed. Overlap is free (insert-only).
LOOKBACK_DAYS = 3

HTTP_TIMEOUT_SECONDS = 60
MAX_RETRIES = 8
MAX_BACKOFF_SECONDS = 120
DEFAULT_MAX_RPS = 8.0
HARD_MAX_RPS = 9.0

# Rows buffered before a COPY + INSERT round trip.
FLUSH_ROWS = 250_000

NY_TZ = ZoneInfo("America/New_York")

FACT_COLUMNS = (
    "cik", "taxonomy", "tag", "unit", "value", "period_start", "period_end",
    "fiscal_year", "fiscal_period", "form", "accession_number", "filed_date",
    "frame",
)
# Mirror of edgar.facts.fact_key in tools/create_edgar_schema.sql.
FACT_KEY_SQL = """md5(
    cik || '|' || taxonomy || '|' || tag || '|' || unit || '|' ||
    accession_number || '|' ||
    (period_end - DATE '2000-01-01')::text || '|' ||
    coalesce((period_start - DATE '2000-01-01')::text, '') || '|' ||
    value::text
)::uuid"""
_END = FACT_COLUMNS.index("period_end")
_ACCN = FACT_COLUMNS.index("accession_number")
_FILED = FACT_COLUMNS.index("filed_date")

FILING_COLUMNS = (
    "cik", "accession_number", "form", "filing_date", "report_date",
    "acceptance_datetime_raw", "act", "file_number", "film_number", "items",
    "size_bytes", "is_xbrl", "is_inline_xbrl", "primary_document",
    "primary_doc_description",
)


# ──────────────────────────────────────────────
# Config / DB
# ──────────────────────────────────────────────

def _get_var(key: str, default: str | None = None) -> str | None:
    # Imported lazily: datapipeline.config.env raises at import time when no
    # DATABASE_URL is set, which would make the pure parsers below untestable.
    from datapipeline.config.env import get_var
    return get_var(key, default)


def _safe_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    port = f":{p.port}" if p.port else ""
    return f"{p.scheme}://{p.username or ''}:***@{p.hostname or ''}{port}/{p.path.lstrip('/')}"


def _resolve_dsn() -> str:
    from datapipeline.config.env import ENV

    dsn = _get_var("DATABASE_URL")
    if not dsn:
        key = {"prod": "DATABASE_URL_PROD", "staging": "DATABASE_URL_STAGING"}.get(
            ENV, "DATABASE_URL_DEV"
        )
        dsn = _get_var(key)
    if not dsn:
        raise RuntimeError("No DATABASE_URL resolved for ENV=%s" % ENV)
    if dsn.startswith("postgresql+psycopg2://"):
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
    elif dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    return dsn


def _get_user_agent() -> str:
    ua = (_get_var("SEC_USER_AGENT") or "").strip()
    # SEC blocks anonymous / generic agents. Require "Name email@host".
    if "@" not in ua or len(ua.split()) < 2:
        raise RuntimeError(
            "SEC_USER_AGENT must be set to 'Your Name your@email' "
            "(env var or Airflow Variable); SEC fair access requires it."
        )
    return ua


# ──────────────────────────────────────────────
# SEC HTTP client (fair access: UA + rate limit)
# ──────────────────────────────────────────────

class SecClient:
    def __init__(self, user_agent: str, max_rps: float = DEFAULT_MAX_RPS):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        })
        self.min_interval = 1.0 / min(max_rps, HARD_MAX_RPS)
        self._last = 0.0
        self.requests_made = 0

    def _throttle(self) -> None:
        wait = self._last + self.min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def get(self, url: str, stream: bool = False) -> requests.Response | None:
        """GET with throttle + backoff. Returns None on 404."""
        attempt = 0
        while True:
            attempt += 1
            self._throttle()
            self.requests_made += 1
            try:
                resp = self.session.get(url, timeout=HTTP_TIMEOUT_SECONDS, stream=stream)
            except requests.RequestException as exc:
                if attempt >= MAX_RETRIES:
                    raise
                delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
                logging.warning("Network error (%s); retry %d in %.1fs", exc, attempt, delay)
                time.sleep(delay)
                continue

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                # SEC answers 403 to a missing/blocked User-Agent and to a
                # client over the rate threshold. Retrying makes it worse.
                raise RuntimeError(
                    f"SEC returned 403 for {url}. Check SEC_USER_AGENT and the "
                    "request rate (SEC blocks clients for ~10 minutes)."
                )
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt >= MAX_RETRIES:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = min(MAX_BACKOFF_SECONDS, int(retry_after))
                else:
                    delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
                logging.warning("HTTP %s from SEC; retry %d in %.1fs", resp.status_code, attempt, delay)
                time.sleep(delay)
                continue
            resp.raise_for_status()

    def last_modified(self, url: str) -> datetime | None:
        """HEAD request: the file's Last-Modified time, None if absent."""
        self._throttle()
        self.requests_made += 1
        resp = self.session.head(url, timeout=HTTP_TIMEOUT_SECONDS, allow_redirects=True)
        if resp.status_code == 403:
            raise RuntimeError(f"SEC returned 403 for {url}. Check SEC_USER_AGENT and the request rate.")
        resp.raise_for_status()
        lm = resp.headers.get("Last-Modified")
        return parsedate_to_datetime(lm) if lm else None

    def get_json(self, url: str) -> Any | None:
        resp = self.get(url)
        if resp is None:
            return None
        return json.loads(resp.content, parse_float=str)

    def download(self, url: str, dest: str) -> datetime | None:
        """Stream url to dest. Returns the Last-Modified time (the snapshot)."""
        resp = self.get(url, stream=True)
        if resp is None:
            raise RuntimeError(f"{url} returned 404")
        tmp = dest + ".part"
        size = 0
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                size += len(chunk)
        os.replace(tmp, dest)
        logging.info("Downloaded %s (%.1f MB)", url, size / 1e6)
        lm = resp.headers.get("Last-Modified")
        return parsedate_to_datetime(lm) if lm else None


# ──────────────────────────────────────────────
# Parsers (pure; no DB, no network)
# ──────────────────────────────────────────────

def pad_cik(cik: Any) -> str:
    """Any CIK form (320193, '320193', 'CIK0000320193') -> '0000320193'."""
    s = str(cik).strip().upper().removeprefix("CIK")
    if not s.isdigit():
        raise ValueError(f"bad CIK {cik!r}")
    return s.zfill(10)


def iter_fact_rows(doc: dict[str, Any], cik: str) -> Iterator[tuple]:
    """companyfacts JSON -> one tuple per fact, in FACT_COLUMNS order.

    As reported: no filtering, no mapping. val keeps its exact JSON text
    (load the doc with parse_float=str) so NUMERIC stores it unrounded.
    cik comes from the caller (file name / requested URL): a few bulk files
    carry facts but no "cik" key.
    """
    cik = pad_cik(cik)
    for taxonomy, concepts in (doc.get("facts") or {}).items():
        for tag, concept in concepts.items():
            for unit, facts in (concept.get("units") or {}).items():
                for f in facts:
                    yield (
                        cik, taxonomy, tag, unit, f["val"],
                        f.get("start"), f["end"], f.get("fy"), f.get("fp"),
                        f["form"], f["accn"], f["filed"], f.get("frame"),
                    )


def _blank_to_none(v: Any) -> Any:
    return None if v == "" else v


def iter_filing_rows(cik: str, table: dict[str, list], since: date) -> Iterator[tuple]:
    """A submissions filings table (columnar: 'recent' or an overflow file)
    -> one tuple per filing with filing_date >= since, in FILING_COLUMNS order."""
    n = len(table.get("accessionNumber") or [])

    def col(name: str) -> list:
        return table.get(name) or [None] * n

    accn, fdate, rdate, accepted = col("accessionNumber"), col("filingDate"), col("reportDate"), col("acceptanceDateTime")
    act, form, fileno, filmno, items = col("act"), col("form"), col("fileNumber"), col("filmNumber"), col("items")
    size, xbrl, ixbrl, pdoc, pdesc = col("size"), col("isXBRL"), col("isInlineXBRL"), col("primaryDocument"), col("primaryDocDescription")
    since_s = since.isoformat()
    for i in range(n):
        if not fdate[i] or fdate[i] < since_s:
            continue
        yield (
            cik, accn[i], _blank_to_none(form[i]), fdate[i], _blank_to_none(rdate[i]),
            _blank_to_none(accepted[i]), _blank_to_none(act[i]), _blank_to_none(fileno[i]),
            _blank_to_none(filmno[i]), _blank_to_none(items[i]), size[i],
            None if xbrl[i] is None else bool(xbrl[i]),
            None if ixbrl[i] is None else bool(ixbrl[i]),
            _blank_to_none(pdoc[i]), _blank_to_none(pdesc[i]),
        )


def overflow_files(doc: dict[str, Any], since: date) -> list[str]:
    """Names of a submissions doc's older-filings pages that reach `since`."""
    out = []
    for f in (doc.get("filings") or {}).get("files") or []:
        if (f.get("filingTo") or "9999") >= since.isoformat():
            out.append(f["name"])
    return out


def company_row(doc: dict[str, Any]) -> tuple:
    """submissions JSON -> edgar.companies attribute tuple."""
    return (
        pad_cik(doc["cik"]),
        _blank_to_none(doc.get("name")),
        _blank_to_none(doc.get("entityType")),
        _blank_to_none(doc.get("sic")),
        _blank_to_none(doc.get("sicDescription")),
        _blank_to_none(doc.get("category")),
        _blank_to_none(doc.get("fiscalYearEnd")),
        _blank_to_none(doc.get("stateOfIncorporation")),
        doc.get("tickers") or [],
        doc.get("exchanges") or [],
        json.dumps(doc.get("formerNames") or []),
    )


def parse_xbrl_rss(xml_bytes: bytes) -> Iterator[tuple[str, date, str, str]]:
    """Monthly XBRL RSS -> (cik, filing_date, form, accession) per item."""
    root = ET.fromstring(xml_bytes)
    for item in root.iter("item"):
        vals: dict[str, str] = {}
        for el in item.iter():
            local = el.tag.rsplit("}", 1)[-1]
            if local in ("cikNumber", "filingDate", "formType", "accessionNumber"):
                vals[local] = (el.text or "").strip()
        if not vals.get("cikNumber") or not vals.get("filingDate"):
            continue
        yield (
            pad_cik(vals["cikNumber"]),
            datetime.strptime(vals["filingDate"], "%m/%d/%Y").date(),
            vals.get("formType", ""),
            vals.get("accessionNumber", ""),
        )


def parse_company_tickers(doc: dict[str, Any]) -> list[tuple[str, str, str]]:
    """company_tickers.json -> unique (cik, ticker, title)."""
    seen: dict[tuple[str, str], str] = {}
    for rec in doc.values():
        seen[(pad_cik(rec["cik_str"]), rec["ticker"])] = rec.get("title")
    return [(c, t, title) for (c, t), title in seen.items()]


# ──────────────────────────────────────────────
# Writers (COPY into a temp table, then insert-only)
# ──────────────────────────────────────────────

def _copy_rows(cur, table: str, columns: Iterable[str], rows: list[tuple]) -> None:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerows(rows)
    buf.seek(0)
    cur.copy_expert(
        f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')",
        buf,
    )


def _prepare_stage(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS stage_facts "
            "(LIKE edgar.facts INCLUDING DEFAULTS)"
        )
        cur.execute(
            "CREATE TEMP TABLE IF NOT EXISTS stage_filings "
            "(LIKE edgar.filings INCLUDING DEFAULTS)"
        )
    conn.commit()


def _insert_fact_rows(conn, table: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols = ", ".join(FACT_COLUMNS)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stage_facts")
        _copy_rows(cur, "stage_facts", FACT_COLUMNS, rows)
        # fact_key is generated by the target table. Sorting by the same
        # (cik, fact_key) the unique index uses makes the bulk load (CIK
        # order) append to the index, which keeps its pages ~80% full instead
        # of ~60%: about 2 GB at full size. Ordering only, so a drift from the
        # DDL expression costs speed, never correctness.
        cur.execute(f"""
            INSERT INTO {table} ({cols})
            SELECT {cols} FROM stage_facts
            ORDER BY cik, {FACT_KEY_SQL}
            ON CONFLICT DO NOTHING
        """)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def insert_facts(conn, rows: list[tuple], stats: dict[str, Any]) -> None:
    """Facts whose period ends after their filed date (SEC forecasts and
    context-date typos) go to edgar.facts_quarantine; the rest to
    edgar.facts, whose CHECK constraint would reject them anyway."""
    ok = [r for r in rows if r[_END] <= r[_FILED]]
    held = [r for r in rows if r[_END] > r[_FILED]]
    stats["facts_inserted"] += _insert_fact_rows(conn, "edgar.facts", ok)
    stats["quarantined"] += _insert_fact_rows(conn, "edgar.facts_quarantine", held)


def insert_filings(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols = ", ".join(FILING_COLUMNS)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE stage_filings")
        _copy_rows(cur, "stage_filings", FILING_COLUMNS, rows)
        cur.execute(f"""
            INSERT INTO edgar.filings ({cols})
            SELECT {cols} FROM stage_filings
            ON CONFLICT DO NOTHING
        """)
        inserted = cur.rowcount
    conn.commit()
    return inserted


UPSERT_COMPANY_NAME_SQL = """
INSERT INTO edgar.companies (cik, entity_name)
VALUES %s
ON CONFLICT (cik) DO UPDATE SET
    entity_name       = COALESCE(EXCLUDED.entity_name, edgar.companies.entity_name),
    last_refreshed_at = NOW();
"""

UPSERT_COMPANY_SQL = """
INSERT INTO edgar.companies (
    cik, entity_name, entity_type, sic, sic_description, category,
    fiscal_year_end, state_of_incorporation, tickers, exchanges, former_names
)
VALUES %s
ON CONFLICT (cik) DO UPDATE SET
    entity_name            = COALESCE(EXCLUDED.entity_name, edgar.companies.entity_name),
    entity_type            = EXCLUDED.entity_type,
    sic                    = EXCLUDED.sic,
    sic_description        = EXCLUDED.sic_description,
    category               = EXCLUDED.category,
    fiscal_year_end        = EXCLUDED.fiscal_year_end,
    state_of_incorporation = EXCLUDED.state_of_incorporation,
    tickers                = EXCLUDED.tickers,
    exchanges              = EXCLUDED.exchanges,
    former_names           = EXCLUDED.former_names,
    last_refreshed_at      = NOW();
"""

UPSERT_TICKERS_SQL = """
INSERT INTO edgar.company_tickers (cik, ticker, title)
VALUES %s
ON CONFLICT (cik, ticker) DO UPDATE SET
    title        = EXCLUDED.title,
    last_seen_at = NOW();
"""


def upsert_company_names(conn, rows: list[tuple[str, str | None]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_COMPANY_NAME_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


def upsert_companies(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, UPSERT_COMPANY_SQL, rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
            page_size=1000,
        )
    conn.commit()
    return len(rows)


def upsert_company_tickers(conn, rows: list[tuple[str, str, str]]) -> int:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_TICKERS_SQL, rows, page_size=1000)
    conn.commit()
    return len(rows)


# ──────────────────────────────────────────────
# Run bookkeeping
# ──────────────────────────────────────────────

def _start_run(conn, mode: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO edgar.load_runs (mode, status) VALUES (%s, 'running') RETURNING run_id",
            (mode,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _finish_run(conn, run_id: int, status: str, stats: dict[str, Any], error: str | None = None) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE edgar.load_runs SET
                status = %s, finished_at = NOW(), error = %s,
                source_snapshot = %s, window_start = %s, window_end = %s,
                ciks_processed = %s, companies_upserted = %s,
                filings_inserted = %s, facts_inserted = %s, quarantined = %s
            WHERE run_id = %s
            """,
            (
                status, error, stats.get("source_snapshot"), stats.get("window_start"),
                stats.get("window_end"), stats.get("ciks_processed"),
                stats.get("companies_upserted"), stats.get("filings_inserted"),
                stats.get("facts_inserted"), stats.get("quarantined"), run_id,
            ),
        )
    conn.commit()


def _ny_today() -> date:
    return datetime.now(NY_TZ).date()


# ──────────────────────────────────────────────
# Bulk backfill
# ──────────────────────────────────────────────

def _zip_member(zf: zipfile.ZipFile, names: set[str], name: str) -> Any | None:
    if name not in names:
        return None
    raw = zf.read(name)
    if not raw.strip():
        return None
    return json.loads(raw, parse_float=str)


def load_bulk_facts(conn, zip_path: str, only: set[str] | None, stats: dict[str, Any]) -> dict[str, str | None]:
    """Stream companyfacts.zip -> edgar.facts. Returns {cik: entity_name}."""
    companies: dict[str, str | None] = {}
    buf: list[tuple] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = sorted(n for n in zf.namelist() if n.endswith(".json"))
        logging.info("companyfacts.zip: %d company files", len(names))
        for i, name in enumerate(names, start=1):
            cik = pad_cik(name.removesuffix(".json"))
            if only is not None and cik not in only:
                continue
            doc = json.loads(zf.read(name) or b"{}", parse_float=str)
            if not doc.get("facts"):
                continue
            companies[cik] = doc.get("entityName")
            buf.extend(iter_fact_rows(doc, cik))
            if len(buf) >= FLUSH_ROWS:
                insert_facts(conn, buf, stats)
                buf = []
            if i % 1000 == 0:
                logging.info(
                    "facts: %d/%d files, %d companies, %d facts inserted",
                    i, len(names), len(companies), stats["facts_inserted"],
                )
    insert_facts(conn, buf, stats)
    upsert_company_names(conn, sorted(companies.items()))
    logging.info(
        "facts done: %d companies, %d facts inserted, %d quarantined",
        len(companies), stats["facts_inserted"], stats["quarantined"],
    )
    return companies


def load_bulk_submissions(conn, zip_path: str, ciks: Iterable[str], stats: dict[str, Any]) -> None:
    """submissions.zip -> edgar.companies attributes + edgar.filings, for the
    CIKs that have facts only (the zip also holds ~1M insiders and funds)."""
    company_rows: list[tuple] = []
    buf: list[tuple] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        for i, cik in enumerate(sorted(ciks), start=1):
            doc = _zip_member(zf, names, f"CIK{cik}.json")
            if doc is None:
                continue
            company_rows.append(company_row(doc))
            buf.extend(iter_filing_rows(cik, (doc.get("filings") or {}).get("recent") or {}, SCOPE_START))
            for extra in overflow_files(doc, SCOPE_START):
                page = _zip_member(zf, names, extra)
                if page is not None:
                    buf.extend(iter_filing_rows(cik, page, SCOPE_START))
            if len(buf) >= FLUSH_ROWS:
                stats["filings_inserted"] += insert_filings(conn, buf)
                buf = []
            if i % 2000 == 0:
                logging.info("submissions: %d CIKs, %d filings inserted", i, stats["filings_inserted"])
    stats["filings_inserted"] += insert_filings(conn, buf)
    stats["companies_upserted"] = upsert_companies(conn, company_rows)
    logging.info(
        "submissions done: %d companies, %d filings inserted",
        stats["companies_upserted"], stats["filings_inserted"],
    )


def load_company_tickers(conn, sec: SecClient | None, path: str | None) -> int:
    if path:
        with open(path, "rb") as fh:
            doc = json.load(fh)
    else:
        assert sec is not None
        doc = sec.get_json(COMPANY_TICKERS_URL)
    n = upsert_company_tickers(conn, parse_company_tickers(doc))
    logging.info("company_tickers: %d mappings upserted", n)
    return n


def run_bulk(
    work_dir: str,
    companyfacts_zip: str | None,
    submissions_zip: str | None,
    tickers_json: str | None,
    only: set[str] | None,
    keep_files: bool,
) -> None:
    dsn = _resolve_dsn()
    logging.info("bulk: DSN=%s", _safe_dsn(dsn))
    needs_http = not (companyfacts_zip and submissions_zip and tickers_json)
    sec = SecClient(_get_user_agent(), float(_get_var("SEC_MAX_RPS", DEFAULT_MAX_RPS))) if needs_http else None

    conn = psycopg2.connect(dsn)
    run_id = _start_run(conn, "bulk")
    stats: dict[str, Any] = {
        "facts_inserted": 0, "quarantined": 0, "filings_inserted": 0, "companies_upserted": 0,
    }
    downloaded: list[str] = []
    try:
        _prepare_stage(conn)
        os.makedirs(work_dir, exist_ok=True)
        snapshot = None
        if not companyfacts_zip:
            companyfacts_zip = os.path.join(work_dir, "companyfacts.zip")
            snapshot = sec.download(COMPANYFACTS_ZIP_URL, companyfacts_zip)
            downloaded.append(companyfacts_zip)
        if not submissions_zip:
            submissions_zip = os.path.join(work_dir, "submissions.zip")
            sec.download(SUBMISSIONS_ZIP_URL, submissions_zip)
            downloaded.append(submissions_zip)
        if snapshot is None:
            # Local file: its mtime is the best available snapshot time.
            snapshot = datetime.fromtimestamp(os.path.getmtime(companyfacts_zip)).astimezone()

        stats["source_snapshot"] = snapshot
        stats["window_start"] = SCOPE_START
        # The daily run resumes from here. Snapshot date in New York, the
        # calendar SEC's filed dates use.
        stats["window_end"] = snapshot.astimezone(NY_TZ).date()

        companies = load_bulk_facts(conn, companyfacts_zip, only, stats)
        stats["ciks_processed"] = len(companies)
        load_bulk_submissions(conn, submissions_zip, companies.keys(), stats)
        load_company_tickers(conn, sec, tickers_json)
        _finish_run(conn, run_id, "success", stats)
        logging.info("bulk done: %s", {k: v for k, v in stats.items()})
    except BaseException as exc:
        _finish_run(conn, run_id, "failed", stats, error=repr(exc)[:2000])
        raise
    finally:
        conn.close()
        if not keep_files:
            for p in downloaded:
                try:
                    os.remove(p)
                except OSError:
                    pass


# ──────────────────────────────────────────────
# Daily incremental
# ──────────────────────────────────────────────

def _months(start: date, end: date) -> list[tuple[int, int]]:
    out, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def ciks_filed_between(sec: SecClient, start: date, end: date) -> set[str]:
    ciks: set[str] = set()
    for y, m in _months(start, end):
        resp = sec.get(XBRL_RSS_URL.format(year=y, month=m))
        if resp is None:  # month not published yet (1st of the month)
            continue
        for cik, fdate, _form, _accn in parse_xbrl_rss(resp.content):
            if start <= fdate <= end:
                ciks.add(cik)
    return ciks


def _known_accessions(conn, cik: str) -> set[str] | None:
    """Accessions already loaded for cik; None if the CIK is new."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM edgar.companies WHERE cik = %s", (cik,))
        if cur.fetchone() is None:
            return None
        cur.execute(
            "SELECT accession_number FROM edgar.facts WHERE cik = %s "
            "UNION SELECT accession_number FROM edgar.facts_quarantine WHERE cik = %s",
            (cik, cik),
        )
        return {r[0] for r in cur.fetchall()}


def run_daily(run_date: date | None, lookback_days: int, only: set[str] | None) -> None:
    dsn = _resolve_dsn()
    logging.info("daily: DSN=%s", _safe_dsn(dsn))
    sec = SecClient(_get_user_agent(), float(_get_var("SEC_MAX_RPS", DEFAULT_MAX_RPS)))

    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT max(window_end) FROM edgar.load_runs WHERE status = 'success'")
        last_end = cur.fetchone()[0]
    conn.commit()
    if last_end is None:
        conn.close()
        raise RuntimeError("No successful edgar load yet. Run --mode bulk first.")

    window_end = run_date or _ny_today()
    window_start = min(last_end, window_end) - timedelta(days=lookback_days)
    run_id = _start_run(conn, "daily")
    stats: dict[str, Any] = {
        "facts_inserted": 0, "quarantined": 0, "filings_inserted": 0, "companies_upserted": 0,
        "window_start": window_start, "window_end": window_end,
    }
    try:
        _prepare_stage(conn)
        ciks = ciks_filed_between(sec, window_start, window_end)
        if only is not None:
            ciks &= only
        logging.info("daily: window %s..%s, %d CIKs with XBRL filings", window_start, window_end, len(ciks))

        facts_buf: list[tuple] = []
        filings_buf: list[tuple] = []
        name_rows: list[tuple] = []     # CIKs with facts but no submissions doc
        company_rows: list[tuple] = []
        since_s = window_start.isoformat()
        for i, cik in enumerate(sorted(ciks), start=1):
            facts_doc = sec.get_json(COMPANYFACTS_API_URL.format(cik=cik))
            if not facts_doc or not facts_doc.get("facts"):
                continue  # no XBRL numbers (yet); revisited while in the window
            sub_doc = sec.get_json(SUBMISSIONS_API_URL.format(name=f"CIK{cik}.json"))
            known = _known_accessions(conn, cik)

            # A new CIK gets its full history. A known CIK gets every fact
            # filed in the window plus any filing whose facts never landed
            # (SEC processed it after that day's run): the gap self-heals.
            for row in iter_fact_rows(facts_doc, cik):
                if known is None or row[_ACCN] not in known or row[_FILED] >= since_s:
                    facts_buf.append(row)

            if sub_doc:
                company_rows.append(company_row(sub_doc))
                # All of "recent" (>= 1 year), not just the window: non-XBRL
                # forms (Form 4, 8-K, ...) filed between XBRL filings land
                # too, so edgar.filings stays as complete as the bulk era.
                # Overlap is free (ON CONFLICT DO NOTHING).
                filings_buf.extend(iter_filing_rows(cik, (sub_doc.get("filings") or {}).get("recent") or {}, SCOPE_START))
                if known is None:
                    for extra in overflow_files(sub_doc, SCOPE_START):
                        page = sec.get_json(SUBMISSIONS_API_URL.format(name=extra))
                        if page:
                            filings_buf.extend(iter_filing_rows(cik, page, SCOPE_START))
                if len(filings_buf) >= FLUSH_ROWS:
                    stats["filings_inserted"] += insert_filings(conn, filings_buf)
                    filings_buf = []
            else:
                name_rows.append((cik, facts_doc.get("entityName")))
            stats["ciks_processed"] = len(company_rows) + len(name_rows)

            if len(facts_buf) >= FLUSH_ROWS:
                insert_facts(conn, facts_buf, stats)
                facts_buf = []
            if i % 200 == 0:
                logging.info("daily: %d/%d CIKs, %d facts inserted", i, len(ciks), stats["facts_inserted"])

        stats["companies_upserted"] = (
            upsert_company_names(conn, name_rows) + upsert_companies(conn, company_rows)
        )
        insert_facts(conn, facts_buf, stats)
        stats["filings_inserted"] += insert_filings(conn, filings_buf)
        load_company_tickers(conn, sec, None)
        _finish_run(conn, run_id, "success", stats)
        logging.info("daily done: %s (SEC requests: %d)", stats, sec.requests_made)
    except BaseException as exc:
        _finish_run(conn, run_id, "failed", stats, error=repr(exc)[:2000])
        raise
    finally:
        conn.close()


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["bulk", "daily"], required=True)
    parser.add_argument(
        "--work-dir",
        default=os.path.join(tempfile.gettempdir(), "edgar_bulk"),
        help="bulk: where companyfacts.zip / submissions.zip are downloaded (~3 GB).",
    )
    parser.add_argument("--companyfacts-zip", help="bulk: use this local file instead of downloading.")
    parser.add_argument("--submissions-zip", help="bulk: use this local file instead of downloading.")
    parser.add_argument("--company-tickers-json", help="bulk: use this local file instead of downloading.")
    parser.add_argument("--keep-files", action="store_true", help="bulk: keep the downloaded zips.")
    parser.add_argument("--run-date", help="daily: window end (YYYY-MM-DD, New York). Default today.")
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument(
        "--ciks",
        help="Comma-separated CIKs to restrict the load to (dev / repair). Default: all.",
    )
    args = parser.parse_args()

    only = {pad_cik(c) for c in args.ciks.split(",")} if args.ciks else None

    if args.mode == "bulk":
        run_bulk(
            work_dir=args.work_dir,
            companyfacts_zip=args.companyfacts_zip,
            submissions_zip=args.submissions_zip,
            tickers_json=args.company_tickers_json,
            only=only,
            keep_files=args.keep_files,
        )
    else:
        run_daily(
            run_date=date.fromisoformat(args.run_date) if args.run_date else None,
            lookback_days=args.lookback_days,
            only=only,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
