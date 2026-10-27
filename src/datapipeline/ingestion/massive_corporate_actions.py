#!/usr/bin/env python3
"""
Dividends and splits from Massive's reference endpoints into
raw.massive_dividends and raw.massive_splits.

The aggregates feed (massive_to_raw_etl) carries closes adjusted for splits
only, and its dividends column stays empty, so prices built on it leave
dividends out while the yfinance history they are joined to folds them in.
These two tables let dbt (cdm.ingest_massive_total_return) put the massive
closes on the same total-return basis. The vendor's dividend amounts are as declared,
not adjusted for later splits (AAPL 2012-08-09: 2.65); the dbt model scales
them with the splits table.

Costs 2 API calls per ticker for --full (~6.5k for the active and delisted
backtest universe); --since-days pages the market-wide endpoints instead.

Idempotent — upserts on the vendor's id; can be re-run freely.

Typical invocations:
    # Every ticker in the active and delisted massive feeds, full history
    python -m datapipeline.ingestion.massive_corporate_actions --full

    # A few tickers (testing)
    python -m datapipeline.ingestion.massive_corporate_actions --tickers SPY,AAPL

    # Daily top-up: actions dated in the last 10 days, all tickers
    python -m datapipeline.ingestion.massive_corporate_actions --since-days 10
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
from polygon import RESTClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
# the client keeps one connection per host; concurrent workers only trip this warning
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

from datapipeline.config.env import ENV, get_var  # noqa: E402


MAX_WORKERS = 8
MAX_RETRIES = 6
MAX_BACKOFF_SECONDS = 60
PAGE_LIMIT = 1000
PROGRESS_EVERY = 250

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.massive_dividends (
    id                text PRIMARY KEY,
    ticker            text NOT NULL,
    ex_dividend_date  date NOT NULL,
    cash_amount       double precision,
    currency          text,
    dividend_type     text,
    frequency         integer,
    pay_date          date,
    record_date       date,
    processed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_massive_dividends_ticker_ex
    ON raw.massive_dividends (ticker, ex_dividend_date);
CREATE TABLE IF NOT EXISTS raw.massive_splits (
    id                text PRIMARY KEY,
    ticker            text NOT NULL,
    execution_date    date NOT NULL,
    split_from        double precision NOT NULL,
    split_to          double precision NOT NULL,
    processed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_massive_splits_ticker_exec
    ON raw.massive_splits (ticker, execution_date);
"""

UPSERT_DIVIDENDS = """
INSERT INTO raw.massive_dividends
    (id, ticker, ex_dividend_date, cash_amount, currency, dividend_type, frequency,
     pay_date, record_date, processed_at)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
    ticker = EXCLUDED.ticker, ex_dividend_date = EXCLUDED.ex_dividend_date,
    cash_amount = EXCLUDED.cash_amount, currency = EXCLUDED.currency,
    dividend_type = EXCLUDED.dividend_type, frequency = EXCLUDED.frequency,
    pay_date = EXCLUDED.pay_date, record_date = EXCLUDED.record_date,
    processed_at = now()
"""

UPSERT_SPLITS = """
INSERT INTO raw.massive_splits (id, ticker, execution_date, split_from, split_to, processed_at)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
    ticker = EXCLUDED.ticker, execution_date = EXCLUDED.execution_date,
    split_from = EXCLUDED.split_from, split_to = EXCLUDED.split_to,
    processed_at = now()
"""

UNIVERSE_SQL = """
SELECT ticker FROM (
    SELECT DISTINCT ticker FROM cdm.api_data_ingestion_massive
    UNION
    SELECT DISTINCT ticker FROM cdm.ingest_massive_delisted_inc WHERE include_for_backtest
) t ORDER BY ticker
"""


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


def _with_retries(fn, what: str):
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as exc:  # the client raises its own BadResponse on HTTP errors
            if attempt >= MAX_RETRIES:
                raise
            delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning("%s failed: %s; retry %d in %.1fs", what, exc, attempt, delay)
            time.sleep(delay)


def _dividend_row(d: Any) -> Optional[tuple]:
    if not getattr(d, "id", None) or not getattr(d, "ex_dividend_date", None):
        return None
    return (d.id, d.ticker, d.ex_dividend_date, getattr(d, "cash_amount", None),
            getattr(d, "currency", None), getattr(d, "dividend_type", None),
            getattr(d, "frequency", None), getattr(d, "pay_date", None),
            getattr(d, "record_date", None))


def _split_row(s: Any) -> Optional[tuple]:
    if not getattr(s, "id", None) or not getattr(s, "execution_date", None):
        return None
    if not getattr(s, "split_from", None) or not getattr(s, "split_to", None):
        return None
    return (s.id, s.ticker, s.execution_date, float(s.split_from), float(s.split_to))


def _fetch_ticker(client: RESTClient, ticker: str) -> tuple[list[tuple], list[tuple]]:
    divs = _with_retries(
        lambda: list(client.list_dividends(ticker=ticker, limit=PAGE_LIMIT)),
        f"{ticker} dividends")
    splits = _with_retries(
        lambda: list(client.list_splits(ticker=ticker, limit=PAGE_LIMIT)),
        f"{ticker} splits")
    return ([r for r in map(_dividend_row, divs) if r],
            [r for r in map(_split_row, splits) if r])


def _write(conn, div_rows: Iterable[tuple], split_rows: Iterable[tuple]) -> None:
    # one row per vendor id: ON CONFLICT cannot touch the same row twice in one statement
    div_rows = list({r[0]: r for r in div_rows}.values())
    split_rows = list({r[0]: r for r in split_rows}.values())
    with conn.cursor() as cur:
        if div_rows:
            execute_values(cur, UPSERT_DIVIDENDS, div_rows,
                           template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, now())")
        if split_rows:
            execute_values(cur, UPSERT_SPLITS, split_rows,
                           template="(%s, %s, %s, %s, %s, now())")
    conn.commit()


def run_tickers(conn, client: RESTClient, tickers: list[str], dry_run: bool) -> None:
    total = len(tickers)
    logging.info("Queued %d tickers for dividends and splits", total)
    t0 = time.perf_counter()
    n_div = n_split = errors = 0
    buf_d: list[tuple] = []
    buf_s: list[tuple] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_ticker, client, t): t for t in tickers}
        for idx, fut in enumerate(as_completed(futures), start=1):
            t = futures[fut]
            try:
                d, s = fut.result()
            except Exception as exc:
                errors += 1
                logging.error("%s: fetch failed: %s", t, exc)
                continue
            buf_d += d
            buf_s += s
            n_div += len(d)
            n_split += len(s)
            if idx % PROGRESS_EVERY == 0 or idx == total:
                if not dry_run:
                    _write(conn, buf_d, buf_s)
                buf_d, buf_s = [], []
                rate = idx / max(time.perf_counter() - t0, 1e-9)
                logging.info("progress %d/%d dividends=%d splits=%d err=%d (%.2f tickers/s)",
                             idx, total, n_div, n_split, errors, rate)
    logging.info("Done. dividends=%d splits=%d errors=%d dry_run=%s", n_div, n_split, errors, dry_run)


def run_since(conn, client: RESTClient, days: int, dry_run: bool) -> None:
    since = (date.today() - timedelta(days=days)).isoformat()
    divs = _with_retries(
        lambda: list(client.list_dividends(ex_dividend_date_gte=since, limit=PAGE_LIMIT)),
        "market-wide dividends")
    splits = _with_retries(
        lambda: list(client.list_splits(execution_date_gte=since, limit=PAGE_LIMIT)),
        "market-wide splits")
    d = [r for r in map(_dividend_row, divs) if r]
    s = [r for r in map(_split_row, splits) if r]
    logging.info("Since %s: dividends=%d splits=%d", since, len(d), len(s))
    if not dry_run:
        _write(conn, d, s)


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true",
                      help="every ticker in the active and delisted massive feeds")
    mode.add_argument("--tickers", help="comma-separated tickers")
    mode.add_argument("--since-days", type=int, help="market-wide actions dated in the last N days")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dsn = _resolve_dsn()
    logging.info("ENV=%s; DSN=%s", ENV, _safe_dsn(dsn))
    client = RESTClient(_get_api_key(), num_pools=MAX_WORKERS * 2)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        if args.since_days is not None:
            run_since(conn, client, args.since_days, args.dry_run)
            return 0
        if args.full:
            with conn.cursor() as cur:
                cur.execute(UNIVERSE_SQL)
                tickers = [r[0] for r in cur.fetchall()]
        else:
            tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        run_tickers(conn, client, tickers, args.dry_run)
        if not args.dry_run:
            # dbt reads these minutes later, before autovacuum may have caught up
            with conn.cursor() as cur:
                cur.execute("ANALYZE raw.massive_dividends; ANALYZE raw.massive_splits;")
            conn.commit()
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
