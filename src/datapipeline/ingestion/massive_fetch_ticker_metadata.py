#!/usr/bin/env python3
"""
Phase 1 of the delisted-ticker backfill.

Fetches the active and/or delisted US stock universe from the Massive
(Polygon) reference/tickers endpoint and upserts into raw.ticker_metadata
keyed on ticker. Does not touch aggregates_fetched on existing rows.

Typical invocations:
    # Full delisted sweep (~20K rows) for initial backfill
    python -m datapipeline.ingestion.massive_fetch_ticker_metadata \\
        --status=delisted

    # Incremental delisted sweep for the monthly DAG
    python -m datapipeline.ingestion.massive_fetch_ticker_metadata \\
        --status=delisted --delisted-since=2026-03-01

    # Active universe (optional, for completeness)
    python -m datapipeline.ingestion.massive_fetch_ticker_metadata \\
        --status=active
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from typing import Any, Iterable
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

from datapipeline.config.env import ENV, get_var  # noqa: E402


POLYGON_TICKERS_URL = "https://api.polygon.io/v3/reference/tickers"
TARGET_SCHEMA = "raw"
TARGET_TABLE = "ticker_metadata"

HTTP_TIMEOUT_SECONDS = 30
MAX_BACKOFF_SECONDS = 120
MAX_RETRIES = 8


def _safe_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    port = f":{p.port}" if p.port else ""
    return f"{p.scheme}://{p.username or ''}:***@{p.hostname or ''}{port}/{p.path.lstrip('/')}"


def _resolve_dsn() -> str:
    dsn = get_var("DATABASE_URL")
    if not dsn:
        key = {"prod": "DATABASE_URL_PROD", "staging": "DATABASE_URL_STAGING"}.get(
            ENV, "DATABASE_URL_DEV"
        )
        dsn = get_var(key)
    if not dsn:
        raise RuntimeError("No DATABASE_URL resolved for ENV=%s" % ENV)
    if dsn.startswith("postgresql+psycopg2://"):
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
    elif dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    return dsn


def _get_api_key() -> str:
    key = get_var("MASSIVE_API_KEY")
    if not key:
        raise RuntimeError("MASSIVE_API_KEY not set")
    return key


def _request_with_backoff(
    url: str, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise
            delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning("Network error (%s); retry %d in %.1fs", exc, attempt, delay)
            time.sleep(delay)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt >= MAX_RETRIES:
                resp.raise_for_status()
            # Honour Retry-After when present, else exponential backoff.
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = min(MAX_BACKOFF_SECONDS, int(retry_after))
            else:
                delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning(
                "HTTP %s from Polygon; retry %d in %.1fs", resp.status_code, attempt, delay
            )
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp.json()


def iter_ticker_pages(
    api_key: str,
    active: bool,
) -> Iterable[list[dict[str, Any]]]:
    # Note: Polygon v3 /reference/tickers does NOT honour delisted_utc.gte
    # server-side — any date filtering has to happen in the caller after
    # the response is parsed. We paginate the full universe every run;
    # it's ~23k delisted rows and a few seconds of upserts.
    params: dict[str, Any] = {
        "market": "stocks",
        "active": "true" if active else "false",
        "limit": 1000,
        "apiKey": api_key,
    }

    url = POLYGON_TICKERS_URL
    first = True
    while True:
        page = _request_with_backoff(url, params if first else {"apiKey": api_key})
        first = False

        results = page.get("results") or []
        if results:
            yield results

        next_url = page.get("next_url")
        if not next_url:
            return
        url = next_url


def _project_row(raw_row: dict[str, Any], active: bool) -> dict[str, Any]:
    return {
        "ticker": raw_row.get("ticker"),
        "name": raw_row.get("name"),
        "primary_exchange": raw_row.get("primary_exchange"),
        "currency_name": raw_row.get("currency_name"),
        "locale": raw_row.get("locale"),
        "market": raw_row.get("market"),
        # Polygon ticker type (CS, ADRC, OS, ETF, PFD, RIGHT, WARRANT,
        # SP, BOND, FUND, UNIT, ...). Phase 2 filters on this.
        "type": raw_row.get("type"),
        "active": active,
        # Polygon returns this as an ISO-8601 string (or omitted for active).
        "delisted_utc": raw_row.get("delisted_utc"),
        # SEC Central Index Key. NULL on entities without SEC filings
        # (often a shell/fraud/non-US signal).
        "cik": raw_row.get("cik"),
        # Bloomberg FIGIs for cross-dataset joining.
        "composite_figi": raw_row.get("composite_figi"),
        "share_class_figi": raw_row.get("share_class_figi"),
    }


UPSERT_SQL = """
INSERT INTO raw.ticker_metadata (
    ticker, name, primary_exchange, currency_name, locale, market, type,
    active, delisted_utc, cik, composite_figi, share_class_figi,
    last_refreshed_at
)
VALUES %s
ON CONFLICT (ticker) DO UPDATE SET
    name             = EXCLUDED.name,
    primary_exchange = EXCLUDED.primary_exchange,
    currency_name    = EXCLUDED.currency_name,
    locale           = EXCLUDED.locale,
    market           = EXCLUDED.market,
    type             = EXCLUDED.type,
    active           = EXCLUDED.active,
    delisted_utc     = EXCLUDED.delisted_utc,
    cik              = EXCLUDED.cik,
    composite_figi   = EXCLUDED.composite_figi,
    share_class_figi = EXCLUDED.share_class_figi,
    last_refreshed_at = NOW();
"""


def upsert_rows(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    # Polygon sometimes returns the same ticker multiple times on a single
    # page (same symbol reused by different companies over time). Postgres
    # rejects duplicates in one ON CONFLICT DO UPDATE statement, so we
    # dedupe in-batch. Last occurrence wins — ordering within the page is
    # not documented but later entries tend to be more recent.
    deduped: dict[str, dict[str, Any]] = {}
    for r in rows:
        t = r.get("ticker")
        if t:
            deduped[t] = r

    tuples = [
        (
            r["ticker"],
            r["name"],
            r["primary_exchange"],
            r["currency_name"],
            r["locale"],
            r["market"],
            r["type"],
            r["active"],
            r["delisted_utc"],
            r["cik"],
            r["composite_figi"],
            r["share_class_figi"],
        )
        for r in deduped.values()
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            UPSERT_SQL,
            tuples,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
            page_size=500,
        )
    return len(tuples)


def run(
    status: str,
    delisted_since: str | None,
    dry_run: bool,
) -> None:
    if status not in {"active", "delisted"}:
        raise ValueError("--status must be 'active' or 'delisted'")

    api_key = _get_api_key()
    dsn = _resolve_dsn()
    logging.info("ENV=%s; DSN=%s", ENV, _safe_dsn(dsn))

    want_active = status == "active"
    since_prefix: str | None = None
    if delisted_since and not want_active:
        # Client-side filter: keep rows whose delisted_utc ISO string
        # sorts >= delisted_since (ISO-8601 lexicographic comparison).
        since_prefix = delisted_since

    total_fetched = 0
    total_kept = 0
    total_upserted = 0

    conn = None
    if not dry_run:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False

    try:
        for page_idx, page in enumerate(
            iter_ticker_pages(api_key, active=want_active),
            start=1,
        ):
            total_fetched += len(page)
            rows = [_project_row(r, active=want_active) for r in page]

            if since_prefix:
                rows = [
                    r for r in rows
                    if r.get("delisted_utc") and str(r["delisted_utc"]) >= since_prefix
                ]

            total_kept += len(rows)

            if dry_run:
                logging.info(
                    "[dry-run] page %d: fetched=%d kept=%d running_kept=%d",
                    page_idx, len(page), len(rows), total_kept,
                )
                continue

            if rows:
                assert conn is not None
                upserted = upsert_rows(conn, rows)
                total_upserted += upserted
                conn.commit()

            if page_idx % 5 == 0 or page_idx == 1:
                logging.info(
                    "page %d: fetched=%d kept=%d upserted_total=%d",
                    page_idx, total_fetched, total_kept, total_upserted,
                )

    finally:
        if conn is not None:
            conn.close()

    logging.info(
        "Done. status=%s fetched=%d kept=%d upserted=%d dry_run=%s "
        "delisted_since=%s",
        status, total_fetched, total_kept, total_upserted, dry_run, delisted_since,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", choices=["active", "delisted"], default="delisted")
    parser.add_argument(
        "--delisted-since",
        help="ISO date (YYYY-MM-DD) — only upsert delistings on/after this "
             "date. Applied client-side after fetching (Polygon v3 does not "
             "support server-side filtering on delisted_utc). Ignored when "
             "--status=active.",
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from API but skip DB writes.",
    )
    args = parser.parse_args()

    run(
        status=args.status,
        delisted_since=args.delisted_since,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
