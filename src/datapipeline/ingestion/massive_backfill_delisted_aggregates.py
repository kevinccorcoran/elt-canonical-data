#!/usr/bin/env python3
"""
Phase 2 of the delisted-ticker backfill.

Reads delisted tickers from raw.ticker_metadata where aggregates_fetched
is false, fetches historical daily bars from the Massive (Polygon) aggs
endpoint, writes them to raw.api_data_ingestion_massive_delisted
(separate from the live active-ticker table
raw.api_data_ingestion_massive; schemas match via LIKE), and flips
aggregates_fetched=TRUE after a successful DB write.

Idempotent: reruns skip tickers already marked fetched; row-level
idempotency on the bars table is provided by the existing
ON CONFLICT (ticker_date_id) DO NOTHING constraint.

Typical invocations:
    # Initial backfill (~20K tickers, 1–2 days depending on API tier)
    python -m datapipeline.ingestion.massive_backfill_delisted_aggregates

    # Monthly top-up for tickers freshly added by Phase 1
    python -m datapipeline.ingestion.massive_backfill_delisted_aggregates \\
        --limit 500

    # Resume from a specific ticker (alphabetical order)
    python -m datapipeline.ingestion.massive_backfill_delisted_aggregates \\
        --resume-from AACG
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import datetime, date
from typing import Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import polars as pl
import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

from datapipeline.config.env import ENV, get_var  # noqa: E402
from datapipeline.config.helpers import save_to_database  # noqa: E402


AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")
UTC = ZoneInfo("UTC")

AGGS_URL_TEMPLATE = (
    "https://api.polygon.io/v2/aggs/ticker/{ticker}"
    "/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000"
)
HISTORY_START = "1990-01-01"

TABLE_SCHEMA = "raw"
TABLE_NAME = "api_data_ingestion_massive_delisted"

HTTP_TIMEOUT_SECONDS = 60
MAX_BACKOFF_SECONDS = 120
MAX_RETRIES = 8

PROGRESS_EVERY = 100

# Only common-stock-equivalent types get bars backfilled.
# CS   = Common Stock
# ADRC = American Depositary Receipt (common)
# Everything else (OS, PFD, RIGHT, WARRANT, UNIT, SP, BOND, NOTE, ETF,
# ETN, FUND, ...) is skipped. Rows with NULL type are also skipped
# because Polygon didn't provide a classification.
EQUITY_TYPES = ("CS", "ADRC")

EXPECTED_ORDER = [
    "ticker_date_id",
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_close",
    "processed_at",
]


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


def _delisted_utc_to_date(value) -> date:
    if isinstance(value, datetime):
        return value.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    # String fallback; Polygon returns ISO-8601 with optional 'Z'.
    s = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()


def fetch_unfetched_delisted(
    conn,
    limit: Optional[int],
    resume_from: Optional[str],
    exchanges: Optional[list[str]] = None,
    min_delisted_since: Optional[str] = None,
) -> list[tuple[str, date]]:
    where = [
        "delisted_utc IS NOT NULL",
        "aggregates_fetched = FALSE",
        "type = ANY(%s)",
    ]
    args: list = [list(EQUITY_TYPES)]
    if exchanges:
        where.append("primary_exchange = ANY(%s)")
        args.append(exchanges)
    # Polygon's historical-data plan window: a query only succeeds if
    # range-end falls within the plan's rolling window (5 years on
    # Stocks Starter). Pre-window tickers return 403. Filter them out
    # of the queue so we don't burn API quota on guaranteed failures.
    if min_delisted_since:
        where.append("delisted_utc >= %s")
        args.append(min_delisted_since)
    if resume_from:
        where.append("ticker >= %s")
        args.append(resume_from)

    sql = (
        "SELECT ticker, delisted_utc FROM raw.ticker_metadata "
        f"WHERE {' AND '.join(where)} ORDER BY ticker"
    )
    if limit is not None:
        sql += " LIMIT %s"
        args.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()

    return [(t, _delisted_utc_to_date(d)) for t, d in rows]


def _request_aggs(url: str, api_key: str) -> dict:
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(
                url, params={"apiKey": api_key}, timeout=HTTP_TIMEOUT_SECONDS
            )
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise
            delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning("Network error (%s); retry %d in %.1fs", exc, attempt, delay)
            time.sleep(delay)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code in (404,):
            # No data for this ticker/range.
            return {"results": []}

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt >= MAX_RETRIES:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = min(MAX_BACKOFF_SECONDS, int(retry_after))
            else:
                delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning(
                "HTTP %s from Polygon for %s; retry %d in %.1fs",
                resp.status_code, url, attempt, delay,
            )
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp.json()


def fetch_bars_df(ticker: str, end_date: date, api_key: str) -> pl.DataFrame:
    url = AGGS_URL_TEMPLATE.format(
        ticker=ticker, start=HISTORY_START, end=end_date.isoformat()
    )
    payload = _request_aggs(url, api_key)
    results = payload.get("results") or []
    if not results:
        return pl.DataFrame()

    processed_at = datetime.now(AMSTERDAM_TZ).replace(microsecond=0)

    rows = []
    for bar in results:
        ts = bar.get("t")
        if ts is None:
            continue
        d = datetime.fromtimestamp(ts / 1000, tz=UTC).date()
        rows.append(
            {
                "ticker_date_id": f"{ticker}_{d}",
                "ticker": ticker,
                "date": d,
                "open": float(bar.get("o", 0.0)),
                "high": float(bar.get("h", 0.0)),
                "low": float(bar.get("l", 0.0)),
                "close": float(bar.get("c", 0.0)),
                "volume": int(bar.get("v", 0)),
                "adj_close": float(bar.get("c", 0.0)),
                "processed_at": processed_at,
            }
        )

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).select(EXPECTED_ORDER)


def mark_fetched(conn, ticker: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE raw.ticker_metadata "
            "SET aggregates_fetched = TRUE, last_refreshed_at = NOW() "
            "WHERE ticker = %s",
            (ticker,),
        )
    conn.commit()


def run(
    limit: Optional[int],
    resume_from: Optional[str],
    dry_run: bool,
    exchanges: Optional[list[str]] = None,
    min_delisted_since: Optional[str] = None,
) -> None:
    api_key = _get_api_key()
    dsn = _resolve_dsn()
    logging.info(
        "ENV=%s; DSN=%s; equity_types=%s; exchanges=%s; min_delisted_since=%s",
        ENV, _safe_dsn(dsn), EQUITY_TYPES,
        exchanges or "(all)", min_delisted_since or "(none)",
    )

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        queue = fetch_unfetched_delisted(
            conn,
            limit=limit,
            resume_from=resume_from,
            exchanges=exchanges,
            min_delisted_since=min_delisted_since,
        )
        total = len(queue)
        logging.info("Queued %d unfetched delisted tickers", total)
        if total == 0:
            return

        fetched_ok = 0
        empty = 0
        errors = 0
        t0 = time.perf_counter()

        for idx, (ticker, end_d) in enumerate(queue, start=1):
            try:
                df = fetch_bars_df(ticker, end_d, api_key)
            except Exception as exc:
                errors += 1
                logging.error("%s: fetch failed: %s", ticker, exc)
                continue

            if df.height == 0:
                empty += 1
                if not dry_run:
                    mark_fetched(conn, ticker)
                continue

            if dry_run:
                fetched_ok += 1
            else:
                try:
                    save_to_database(
                        df,
                        table_name=TABLE_NAME,
                        schema_name=TABLE_SCHEMA,
                        connection_string=dsn,
                        conflict_key="ticker_date_id",
                    )
                except Exception as exc:
                    errors += 1
                    logging.error("%s: DB write failed: %s", ticker, exc)
                    continue

                mark_fetched(conn, ticker)
                fetched_ok += 1

            if idx % PROGRESS_EVERY == 0 or idx == total:
                elapsed = time.perf_counter() - t0
                rate = idx / elapsed if elapsed > 0 else 0.0
                logging.info(
                    "progress %d/%d ok=%d empty=%d err=%d (%.2f tickers/s)",
                    idx, total, fetched_ok, empty, errors, rate,
                )

        logging.info(
            "Done. ok=%d empty=%d errors=%d dry_run=%s",
            fetched_ok, empty, errors, dry_run,
        )
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max tickers to process in this run (default: all unfetched).",
    )
    parser.add_argument(
        "--resume-from",
        default=None,
        help="Start from this ticker (alphabetical). Tickers earlier in sort "
             "order are skipped this run.",
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated MIC codes to restrict the queue (e.g. "
             "'XNYS,XNAS' for NYSE+NASDAQ). Defaults to all exchanges.",
    )
    parser.add_argument(
        "--min-delisted-since",
        default=None,
        help="ISO date (YYYY-MM-DD). Skip tickers delisted before this "
             "date. Use to match your Polygon plan's historical window "
             "(e.g. '2021-04-24' for Starter's 5-year rolling window). "
             "Pre-window tickers return 403 from the API.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from API but skip DB writes and flag flip.",
    )
    args = parser.parse_args()

    exchanges = (
        [x.strip().upper() for x in args.exchanges.split(",") if x.strip()]
        if args.exchanges
        else None
    )

    run(
        limit=args.limit,
        resume_from=args.resume_from,
        dry_run=args.dry_run,
        exchanges=exchanges,
        min_delisted_since=args.min_delisted_since,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
